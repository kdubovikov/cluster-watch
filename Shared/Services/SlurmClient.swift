import Foundation

public protocol SlurmClientProtocol: Sendable {
    func fetchCurrentJobs(for cluster: ClusterConfig, username: String) async throws -> [CurrentJob]
    func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot?
    func fetchLogPaths(for cluster: ClusterConfig, jobID: String) async throws -> JobLogPaths?
    func tailLog(for cluster: ClusterConfig, remotePath: String, lineCount: Int) async throws -> String
}

public enum SlurmClientError: LocalizedError, Equatable {
    case invalidConfiguration(String)
    case commandFailed(String)
    case queryFailed(String)

    public var errorDescription: String? {
        switch self {
        case let .invalidConfiguration(message):
            return message
        case let .commandFailed(message):
            return message
        case let .queryFailed(message):
            return message
        }
    }
}

public actor SlurmClient: SlurmClientProtocol {
    private let sshPath: String
    private let connectTimeoutSeconds: Int
    private var inferredTimestampOffsetByCluster: [ClusterID: TimeInterval] = [:]

    public init(sshPath: String = "/usr/bin/ssh", connectTimeoutSeconds: Int = 8) {
        self.sshPath = sshPath
        self.connectTimeoutSeconds = connectTimeoutSeconds
    }

    public func fetchCurrentJobs(for cluster: ClusterConfig, username: String) async throws -> [CurrentJob] {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        let remoteCommand = "squeue -h -u \(shellEscape(username)) -o '\(SlurmParsing.squeueFormat)'"
        let output = try await runSSH(destination: cluster.effectiveSSHDestination, remoteCommand: remoteCommand)
        let parsedJobs = SlurmParsing.parseCurrentJobs(output: output, clusterID: cluster.id)
        let inferredOffset = Self.inferTimestampOffset(for: parsedJobs, now: Date())

        if let inferredOffset {
            inferredTimestampOffsetByCluster[cluster.id] = inferredOffset
        }

        let effectiveOffset = inferredOffset ?? inferredTimestampOffsetByCluster[cluster.id]
        return Self.applyingTimestampOffset(effectiveOffset, to: parsedJobs)
    }

    public func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot? {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        let remoteCommand = "sacct -n -P -j \(shellEscape(jobID)) --format=\(SlurmParsing.sacctFormat)"
        let output = try await runSSH(destination: cluster.effectiveSSHDestination, remoteCommand: remoteCommand)
        let snapshot = SlurmParsing.parseHistoricalJob(output: output, clusterID: cluster.id, requestedJobID: jobID)
        return Self.applyingTimestampOffset(inferredTimestampOffsetByCluster[cluster.id], to: snapshot)
    }

    public func fetchLogPaths(for cluster: ClusterConfig, jobID: String) async throws -> JobLogPaths? {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        do {
            let scontrolOutput = try await runSSH(
                destination: cluster.effectiveSSHDestination,
                remoteCommand: "scontrol show job \(shellEscape(jobID))"
            )
            if let logPaths = SlurmParsing.parseScontrolLogPaths(output: scontrolOutput),
               logPaths.hasAnyPath {
                return logPaths
            }
        } catch {
            // Fall through to accounting data for completed or purged jobs.
        }

        let sacctOutput = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "sacct -n -P --expand-patterns -j \(shellEscape(jobID)) --format=\(SlurmParsing.sacctLogFormat)"
        )
        if let logPaths = SlurmParsing.parseHistoricalLogPaths(output: sacctOutput, requestedJobID: jobID),
           logPaths.hasAnyPath {
            return logPaths
        }

        return nil
    }

    public func tailLog(for cluster: ClusterConfig, remotePath: String, lineCount: Int) async throws -> String {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        let safeLineCount = max(1, lineCount)
        return try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "LC_ALL=C tail -n \(safeLineCount) -- \(shellEscape(remotePath))"
        )
    }

    private func runSSH(destination: String, remoteCommand: String) async throws -> String {
        let process = Process()
        let outputPipe = Pipe()
        let errorPipe = Pipe()

        process.executableURL = URL(fileURLWithPath: sshPath)
        process.arguments = [
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=\(connectTimeoutSeconds)",
            destination,
            remoteCommand
        ]
        process.standardOutput = outputPipe
        process.standardError = errorPipe

        return try await withCheckedThrowingContinuation { continuation in
            process.terminationHandler = { process in
                let outputData = outputPipe.fileHandleForReading.readDataToEndOfFile()
                let errorData = errorPipe.fileHandleForReading.readDataToEndOfFile()
                let output = String(decoding: outputData, as: UTF8.self)
                let errorOutput = String(decoding: errorData, as: UTF8.self)
                let normalizedError = errorOutput.trimmedOrEmpty

                if process.terminationStatus == 0 {
                    if output.trimmedOrEmpty.isEmpty,
                       let queryError = Self.parseQueryError(normalizedError) {
                        continuation.resume(throwing: SlurmClientError.queryFailed(queryError))
                    } else {
                        continuation.resume(returning: output)
                    }
                } else {
                    let message = normalizedError.isEmpty ? output.trimmedOrEmpty : normalizedError
                    continuation.resume(throwing: SlurmClientError.commandFailed(message.isEmpty ? "SSH command failed." : message))
                }
            }

            do {
                try process.run()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    private func shellEscape(_ value: String) -> String {
        "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }

    private static func parseQueryError(_ stderr: String) -> String? {
        guard !stderr.isEmpty else { return nil }

        let lines = stderr
            .split(whereSeparator: \.isNewline)
            .map { String($0).trimmedOrEmpty }
            .filter { !$0.isEmpty }

        return lines.first(where: { line in
            line.contains("Invalid user:")
                || line.contains("squeue: error:")
                || line.contains("sacct: error:")
                || line.contains("slurm_load_jobs error:")
        })
    }

    private static func inferTimestampOffset(for jobs: [CurrentJob], now: Date) -> TimeInterval? {
        let candidates = jobs.compactMap { job -> TimeInterval? in
            guard job.state == .running,
                  let startTime = job.startTime,
                  let elapsedSeconds = job.elapsedSeconds else {
                return nil
            }

            let discrepancy = now.timeIntervalSince(startTime) - elapsedSeconds
            let roundedToMinute = (discrepancy / 60).rounded() * 60

            guard abs(roundedToMinute) >= 300 else { return nil }
            return roundedToMinute
        }

        guard !candidates.isEmpty else { return nil }

        let sorted = candidates.sorted()
        return sorted[sorted.count / 2]
    }

    private static func applyingTimestampOffset(_ offset: TimeInterval?, to jobs: [CurrentJob]) -> [CurrentJob] {
        guard let offset else { return jobs }
        return jobs.map { applyingTimestampOffset(offset, to: $0) }
    }

    private static func applyingTimestampOffset(_ offset: TimeInterval?, to snapshot: JobSnapshot?) -> JobSnapshot? {
        guard let offset, var snapshot else { return snapshot }
        snapshot.submitTime = snapshot.submitTime?.addingTimeInterval(offset)
        snapshot.startTime = snapshot.startTime?.addingTimeInterval(offset)
        snapshot.endTime = snapshot.endTime?.addingTimeInterval(offset)
        return snapshot
    }

    private static func applyingTimestampOffset(_ offset: TimeInterval, to job: CurrentJob) -> CurrentJob {
        var job = job
        job.submitTime = job.submitTime?.addingTimeInterval(offset)
        job.startTime = job.startTime?.addingTimeInterval(offset)
        return job
    }
}
