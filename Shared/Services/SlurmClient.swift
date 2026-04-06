import Foundation

public protocol SlurmClientProtocol: Sendable {
    func fetchCurrentJobs(for cluster: ClusterConfig, username: String) async throws -> [CurrentJob]
    func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot?
    func fetchLogPaths(for cluster: ClusterConfig, jobID: String) async throws -> JobLogPaths?
    func fetchLaunchDetails(for cluster: ClusterConfig, jobID: String) async throws -> JobLaunchDetails?
    func fetchClusterLoad(for cluster: ClusterConfig, username: String, currentJobs: [CurrentJob]) async throws -> ClusterLoadSnapshot
    func tailLog(for cluster: ClusterConfig, remotePath: String, lineCount: Int) async throws -> String
    func cancelJob(for cluster: ClusterConfig, jobID: String) async throws
    func cancelJobs(for cluster: ClusterConfig, jobIDs: [String]) async throws
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
    private var timestampOffsetByCluster: [ClusterID: TimeInterval] = [:]
    private let clusterLoadDiscoveryTTL: TimeInterval = 600
    private var clusterLoadDiscoveryByCluster: [ClusterID: CachedClusterLoadDiscovery] = [:]

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
        let effectiveOffset = await resolveTimestampOffset(for: cluster, jobs: parsedJobs, now: Date())
        return Self.applyingTimestampOffset(effectiveOffset, to: parsedJobs)
    }

    public func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot? {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        let remoteCommand = "sacct -n -P -j \(shellEscape(jobID)) --format=\(SlurmParsing.sacctFormat)"
        let output = try await runSSH(destination: cluster.effectiveSSHDestination, remoteCommand: remoteCommand)
        let snapshot = SlurmParsing.parseHistoricalJob(output: output, clusterID: cluster.id, requestedJobID: jobID)
        return Self.applyingTimestampOffset(timestampOffsetByCluster[cluster.id], to: snapshot)
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

    public func fetchLaunchDetails(for cluster: ClusterConfig, jobID: String) async throws -> JobLaunchDetails? {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        var details = JobLaunchDetails()

        do {
            let scontrolOutput = try await runSSH(
                destination: cluster.effectiveSSHDestination,
                remoteCommand: "scontrol show job \(shellEscape(jobID))"
            )
            if let parsed = SlurmParsing.parseScontrolLaunchDetails(output: scontrolOutput) {
                details.commandText = parsed.commandText
                details.workDirectory = parsed.workDirectory
            }
        } catch {
            // Fall through to batch script lookup; older jobs may no longer be available from the controller.
        }

        do {
            let batchScriptOutput = try await runSSH(
                destination: cluster.effectiveSSHDestination,
                remoteCommand: "scontrol write batch_script \(shellEscape(jobID)) -"
            )
            details.batchScriptText = SlurmParsing.parseBatchScript(output: batchScriptOutput)
        } catch {
            // Not all job types expose a batch script, and completed jobs may be purged.
        }

        return details.hasAnyContent ? details : nil
    }

    public func fetchClusterLoad(
        for cluster: ClusterConfig,
        username: String,
        currentJobs: [CurrentJob]
    ) async throws -> ClusterLoadSnapshot {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }

        let now = Date()
        let discovery = try await clusterLoadDiscovery(for: cluster, username: username, now: now)
        let userRunningGPUByQOS = ClusterLoadSupport.runningGPUUsageByQOS(currentJobs)

        var queueSummary: ClusterQueueSummary?
        var resourceSummary: ClusterLoadResourceSummary?
        var scopedUsageSummary = ClusterScopedUsageSummary()
        var messages: [String] = []

        if discovery.hasPartitionMetadata, discovery.accessiblePartitions.isEmpty {
            queueSummary = ClusterQueueSummary(totalJobCount: 0, pendingJobCount: 0)
            resourceSummary = ClusterLoadResourceSummary(
                freeCPUCount: 0,
                totalCPUCount: 0,
                freeGPUCount: 0,
                totalGPUCount: 0,
                freeNodeCount: 0,
                totalNodeCount: 0
            )
            messages.append("No accessible partitions were discovered for this user.")
        } else {
            do {
                queueSummary = try await fetchQueueSummary(
                    for: cluster,
                    partitions: discovery.accessiblePartitions,
                    accounts: discovery.accessibleAccounts
                )
            } catch {
                messages.append((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
            }

            do {
                resourceSummary = try await fetchResourceSummary(for: cluster, accessiblePartitions: discovery.accessiblePartitions)
            } catch {
                messages.append((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
            }

            let needsScopedUsage = !discovery.accessibleAccounts.isEmpty && (
                !discovery.accountGPUCapByQOS.isEmpty
                    || cluster.usableGPUCap != nil
                    || cluster.usableNodeCap != nil
            )

            if needsScopedUsage {
                do {
                    scopedUsageSummary = try await fetchScopedUsageSummary(
                        for: cluster,
                        accounts: discovery.accessibleAccounts
                    )
                } catch {
                    messages.append((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
                }
            }
        }

        return ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: currentJobs,
            queueSummary: queueSummary,
            resourceSummary: resourceSummary,
            userRunningGPUByQOS: userRunningGPUByQOS,
            accountRunningGPUByQOS: scopedUsageSummary.runningGPUByQOS,
            scopedRunningGPUCount: scopedUsageSummary.runningGPUCount,
            scopedRunningNodeCount: scopedUsageSummary.runningNodeCount,
            configuredGPUCap: cluster.usableGPUCap,
            configuredNodeCap: cluster.usableNodeCap,
            now: now,
            message: messages.first
        )
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

    public func cancelJob(for cluster: ClusterConfig, jobID: String) async throws {
        try await cancelJobs(for: cluster, jobIDs: [jobID])
    }

    public func cancelJobs(for cluster: ClusterConfig, jobIDs: [String]) async throws {
        guard !cluster.effectiveSSHDestination.isEmpty else {
            throw SlurmClientError.invalidConfiguration("Missing SSH alias for \(cluster.displayName).")
        }
        let cleanedJobIDs = jobIDs.map(\.trimmedOrEmpty).filter { !$0.isEmpty }
        guard !cleanedJobIDs.isEmpty else { return }

        _ = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "scancel \(shellEscape(cleanedJobIDs.joined(separator: ",")))"
        )
    }

    private func clusterLoadDiscovery(
        for cluster: ClusterConfig,
        username: String,
        now: Date
    ) async throws -> ClusterLoadDiscoveryContext {
        if let cached = clusterLoadDiscoveryByCluster[cluster.id],
           now.timeIntervalSince(cached.refreshedAt) < clusterLoadDiscoveryTTL {
            return cached.context
        }

        let withAssocOutput = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "sacctmgr -Pn show user \(shellEscape(username)) withassoc format=User,DefaultAccount,Account,Cluster,Partition,QOS"
        )
        let userRows = ClusterLoadSupport.parseUserAssociationRows(withAssocOutput)

        let userAssocOutput = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "sacctmgr -Pn show assoc where user=\(shellEscape(username)) format=Cluster,Account,Partition,QOS,MaxJobs,MaxSubmit,GrpJobs,GrpSubmit,GrpTRES,MaxTRES,MaxTRESPerJob,MaxWall"
        )
        let userAssocRows = ClusterLoadSupport.parseAssociationLimitRows(userAssocOutput, includesUserColumn: false)

        let accounts = Set(
            userRows.map(\.account).filter { !$0.isEmpty }
                + userAssocRows.compactMap(\.account)
        )

        var accountAssocRows: [ClusterLoadAssociationLimitRow] = []
        for account in accounts.sorted() {
            let accountAssocOutput = try await runSSH(
                destination: cluster.effectiveSSHDestination,
                remoteCommand: "sacctmgr -Pn show assoc where account=\(shellEscape(account)) format=Cluster,Account,User,Partition,QOS,MaxJobs,MaxSubmit,GrpJobs,GrpSubmit,GrpTRES,MaxTRES,MaxTRESPerJob,MaxWall"
            )
            accountAssocRows += ClusterLoadSupport.parseAssociationLimitRows(accountAssocOutput, includesUserColumn: true)
        }

        let qosNames = Set(
            userRows.flatMap(\.qosValues)
                + userAssocRows.flatMap(\.qosValues)
                + accountAssocRows.flatMap(\.qosValues)
        )

        let qosRows: [ClusterLoadQoSRow]
        if qosNames.isEmpty {
            qosRows = []
        } else {
            let qosOutput = try await runSSH(
                destination: cluster.effectiveSSHDestination,
                remoteCommand: "sacctmgr -Pn show qos format=Name,Flags,MaxJobsPU,MaxSubmitPU,MaxTRESPU,MaxTRESPA,MaxTRESPerJob,GrpTRES,MaxWall"
            )
            let allowedNames = Set(qosNames)
            qosRows = ClusterLoadSupport.parseQoSRows(qosOutput)
                .filter { allowedNames.contains($0.name) }
        }

        let partitionsOutput = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "scontrol show partition -o"
        )
        let partitions = ClusterLoadSupport.parsePartitionRows(partitionsOutput)

        let configOutput = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "scontrol show config | egrep '^(AccountingStorageEnforce|EnforcePartLimits|JobSubmitPlugins)'"
        )

        let context = ClusterLoadSupport.makeDiscoveryContext(
            userRows: userRows,
            userAssocRows: userAssocRows,
            accountAssocRows: accountAssocRows,
            qosRows: qosRows,
            partitions: partitions,
            configOutput: configOutput
        )
        clusterLoadDiscoveryByCluster[cluster.id] = CachedClusterLoadDiscovery(context: context, refreshedAt: now)
        return context
    }

    private func fetchQueueSummary(
        for cluster: ClusterConfig,
        partitions: [String],
        accounts: [String]
    ) async throws -> ClusterQueueSummary {
        var remoteCommand = "squeue -h"

        if !accounts.isEmpty {
            remoteCommand += " -A \(shellEscape(accounts.joined(separator: ",")))"
        }
        if !partitions.isEmpty {
            remoteCommand += " -p \(shellEscape(partitions.joined(separator: ",")))"
        }

        remoteCommand += " -o '%T'"

        let output = try await runSSH(destination: cluster.effectiveSSHDestination, remoteCommand: remoteCommand)
        let states = output
            .split(whereSeparator: \.isNewline)
            .map(String.init)
            .map(\.trimmedOrEmpty)
            .filter { !$0.isEmpty }

        let pendingJobCount = states.filter { state in
            state.uppercased().hasPrefix("PENDING")
        }.count

        return ClusterQueueSummary(totalJobCount: states.count, pendingJobCount: pendingJobCount)
    }

    private func fetchScopedUsageSummary(
        for cluster: ClusterConfig,
        accounts: [String]
    ) async throws -> ClusterScopedUsageSummary {
        guard !accounts.isEmpty else { return ClusterScopedUsageSummary() }

        let remoteCommand = "squeue -h -A \(shellEscape(accounts.joined(separator: ","))) -o '%q|%b|%T|%D'"
        let output = try await runSSH(destination: cluster.effectiveSSHDestination, remoteCommand: remoteCommand)

        return output
            .split(whereSeparator: \.isNewline)
            .reduce(into: ClusterScopedUsageSummary()) { partialResult, line in
                let parts = String(line)
                    .split(separator: "|", omittingEmptySubsequences: false)
                    .map(String.init)
                guard parts.count == 4 else { return }
                let state = NormalizedJobState(rawSlurmState: parts[2])
                guard state == .running else { return }

                let nodeCount = max(1, Int(parts[3].trimmedOrEmpty) ?? 1)
                let gpuPerNodeCount = ClusterLoadSupport.parseGRESGPUCount(parts[1]) ?? 0
                let totalGPUCount = gpuPerNodeCount * nodeCount

                partialResult.runningNodeCount += nodeCount
                partialResult.runningGPUCount += totalGPUCount

                let qos = parts[0].trimmedOrEmpty
                guard !qos.isEmpty else { return }
                partialResult.runningGPUByQOS[qos, default: 0] += totalGPUCount
            }
    }

    private func fetchResourceSummary(
        for cluster: ClusterConfig,
        accessiblePartitions: [String]
    ) async throws -> ClusterLoadResourceSummary {
        let output = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "scontrol show node -o"
        )
        let nodes = ClusterLoadSupport.parseNodeRows(output)
        return ClusterLoadSupport.makeResourceSummary(nodes: nodes, accessiblePartitions: accessiblePartitions)
    }

    private func runSSH(destination: String, remoteCommand: String) async throws -> String {
        let process = Process()
        let outputPipe = Pipe()
        let errorPipe = Pipe()
        let outputHandle = outputPipe.fileHandleForReading
        let errorHandle = errorPipe.fileHandleForReading
        let buffer = SSHStreamBuffer()

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
            outputHandle.readabilityHandler = { handle in
                let chunk = handle.availableData
                guard !chunk.isEmpty else {
                    handle.readabilityHandler = nil
                    return
                }

                buffer.appendOutput(chunk)
            }

            errorHandle.readabilityHandler = { handle in
                let chunk = handle.availableData
                guard !chunk.isEmpty else {
                    handle.readabilityHandler = nil
                    return
                }

                buffer.appendError(chunk)
            }

            process.terminationHandler = { process in
                outputHandle.readabilityHandler = nil
                errorHandle.readabilityHandler = nil

                let remainingOutput = outputHandle.readDataToEndOfFile()
                let remainingError = errorHandle.readDataToEndOfFile()
                let (capturedOutput, capturedError) = buffer.snapshot(
                    remainingOutput: remainingOutput,
                    remainingError: remainingError
                )

                let output = String(decoding: capturedOutput, as: UTF8.self)
                let errorOutput = String(decoding: capturedError, as: UTF8.self)
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

    private func resolveTimestampOffset(for cluster: ClusterConfig, jobs: [CurrentJob], now: Date) async -> TimeInterval? {
        if let inferredOffset = Self.inferTimestampOffset(for: jobs, now: now) {
            timestampOffsetByCluster[cluster.id] = inferredOffset
            return inferredOffset
        }

        if let cachedOffset = timestampOffsetByCluster[cluster.id] {
            return cachedOffset
        }

        guard let remoteOffset = try? await fetchRemoteTimeZoneOffset(for: cluster, now: now) else {
            return nil
        }

        timestampOffsetByCluster[cluster.id] = remoteOffset
        return remoteOffset
    }

    private func fetchRemoteTimeZoneOffset(for cluster: ClusterConfig, now: Date) async throws -> TimeInterval? {
        let output = try await runSSH(
            destination: cluster.effectiveSSHDestination,
            remoteCommand: "date +%z"
        )

        return Self.timestampOffset(fromRemoteUTCOffset: output, now: now)
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

    static func timestampOffset(
        fromRemoteUTCOffset rawValue: String,
        now: Date,
        localTimeZone: TimeZone = .current
    ) -> TimeInterval? {
        let value = rawValue.trimmedOrEmpty
        guard value.count == 5 else { return nil }

        let signCharacter = value.first
        guard signCharacter == "+" || signCharacter == "-" else { return nil }

        let digits = value.dropFirst()
        guard digits.count == 4,
              let hours = Int(digits.prefix(2)),
              let minutes = Int(digits.suffix(2)),
              minutes < 60 else {
            return nil
        }

        let remoteOffsetSeconds = ((hours * 3_600) + (minutes * 60)) * (signCharacter == "-" ? -1 : 1)
        let localOffsetSeconds = localTimeZone.secondsFromGMT(for: now)
        return TimeInterval(localOffsetSeconds - remoteOffsetSeconds)
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

private final class SSHStreamBuffer: @unchecked Sendable {
    private let queue = DispatchQueue(label: "ClusterWatch.SSHStream")
    private var outputData = Data()
    private var errorData = Data()

    func appendOutput(_ data: Data) {
        queue.sync {
            outputData.append(data)
        }
    }

    func appendError(_ data: Data) {
        queue.sync {
            errorData.append(data)
        }
    }

    func snapshot(remainingOutput: Data, remainingError: Data) -> (Data, Data) {
        queue.sync {
            if !remainingOutput.isEmpty {
                outputData.append(remainingOutput)
            }
            if !remainingError.isEmpty {
                errorData.append(remainingError)
            }
            return (outputData, errorData)
        }
    }
}

struct ClusterLoadDiscoveryContext: Hashable, Sendable {
    var accessiblePartitions: [String]
    var accessibleAccounts: [String]
    var preferredQOSOrder: [String]
    var userGPUCapByQOS: [String: Int]
    var accountGPUCapByQOS: [String: Int]
    var maxRunningJobs: Int?
    var maxSubmittedJobs: Int?
    var limitsEnforced: Bool
    var hasPartitionMetadata: Bool
}

struct ClusterLoadAssociationUserRow: Hashable, Sendable {
    var account: String
    var partition: String?
    var qosValues: [String]
}

struct ClusterLoadAssociationLimitRow: Hashable, Sendable {
    var account: String?
    var user: String?
    var partition: String?
    var qosValues: [String]
    var maxJobs: Int?
    var maxSubmit: Int?
    var groupGPUCount: Int?
    var maxGPUCount: Int?
}

struct ClusterLoadQoSRow: Hashable, Sendable {
    var name: String
    var maxJobsPerUser: Int?
    var maxSubmitPerUser: Int?
    var maxGPUPerUser: Int?
    var maxGPUPerAccount: Int?
    var groupGPUCount: Int?
}

struct ClusterLoadPartitionRow: Hashable, Sendable {
    var name: String
    var allowAccounts: Set<String>?
}

struct ClusterLoadNodeRow: Hashable, Sendable {
    var name: String
    var partitions: Set<String>
    var cfgTRES: [String: Int]
    var allocTRES: [String: Int]
}

struct ClusterLoadResourceSummary: Hashable, Sendable {
    var freeCPUCount: Int
    var totalCPUCount: Int
    var freeGPUCount: Int
    var totalGPUCount: Int
    var freeNodeCount: Int
    var totalNodeCount: Int
}

struct ClusterQueueSummary: Hashable, Sendable {
    var totalJobCount: Int
    var pendingJobCount: Int
}

struct ClusterScopedUsageSummary: Hashable, Sendable {
    var runningGPUByQOS: [String: Int] = [:]
    var runningGPUCount: Int = 0
    var runningNodeCount: Int = 0
}

struct ClusterScopedGPUAvailability: Hashable, Sendable {
    var freeGPUCount: Int
    var totalGPUCount: Int
    var description: String
}

struct ClusterScopedNodeAvailability: Hashable, Sendable {
    var freeNodeCount: Int
    var totalNodeCount: Int
    var description: String
}

private struct CachedClusterLoadDiscovery: Sendable {
    var context: ClusterLoadDiscoveryContext
    var refreshedAt: Date
}

enum ClusterLoadSupport {
    static func parseUserAssociationRows(_ output: String) -> [ClusterLoadAssociationUserRow] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { line -> ClusterLoadAssociationUserRow? in
                let parts = splitColumns(String(line), expectedCount: 6)
                guard parts.count == 6 else { return nil }
                let account = parts[2].trimmedOrEmpty
                guard !account.isEmpty else { return nil }
                return ClusterLoadAssociationUserRow(
                    account: account,
                    partition: normalizedToken(parts[4]),
                    qosValues: splitList(parts[5])
                )
            }
    }

    static func parseAssociationLimitRows(_ output: String, includesUserColumn: Bool) -> [ClusterLoadAssociationLimitRow] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { line -> ClusterLoadAssociationLimitRow? in
                let parts = splitColumns(String(line), expectedCount: 12)
                guard parts.count == 12 else { return nil }

                let partitionIndex = includesUserColumn ? 3 : 2
                let qosIndex = includesUserColumn ? 4 : 3
                let maxJobsIndex = includesUserColumn ? 5 : 4
                let maxSubmitIndex = includesUserColumn ? 6 : 5

                return ClusterLoadAssociationLimitRow(
                    account: normalizedToken(parts[1]),
                    user: includesUserColumn ? normalizedToken(parts[2]) : nil,
                    partition: normalizedToken(parts[partitionIndex]),
                    qosValues: splitList(parts[qosIndex]),
                    maxJobs: parseOptionalLimit(parts[maxJobsIndex]),
                    maxSubmit: parseOptionalLimit(parts[maxSubmitIndex]),
                    groupGPUCount: parseGPUCount(inTRESValue: parts[includesUserColumn ? 9 : 8]),
                    maxGPUCount: parseGPUCount(inTRESValue: parts[includesUserColumn ? 10 : 9])
                )
            }
    }

    static func parseQoSRows(_ output: String) -> [ClusterLoadQoSRow] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { line -> ClusterLoadQoSRow? in
                let parts = splitColumns(String(line), expectedCount: 9)
                guard parts.count == 9 else { return nil }
                let name = parts[0].trimmedOrEmpty
                guard !name.isEmpty else { return nil }
                return ClusterLoadQoSRow(
                    name: name,
                    maxJobsPerUser: parseOptionalLimit(parts[2]),
                    maxSubmitPerUser: parseOptionalLimit(parts[3]),
                    maxGPUPerUser: parseGPUCount(inTRESValue: parts[4]),
                    maxGPUPerAccount: parseGPUCount(inTRESValue: parts[5]),
                    groupGPUCount: parseGPUCount(inTRESValue: parts[7])
                )
            }
    }

    static func parsePartitionRows(_ output: String) -> [ClusterLoadPartitionRow] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { line -> ClusterLoadPartitionRow? in
                let fields = parseKeyValueFields(String(line))
                guard let rawName = fields["PartitionName"] else { return nil }
                let name = rawName.replacingOccurrences(of: "*", with: "").trimmedOrEmpty
                guard !name.isEmpty else { return nil }

                let allowAccounts: Set<String>?
                if let token = normalizedToken(fields["AllowAccounts"]) {
                    allowAccounts = Set(splitList(token))
                } else {
                    allowAccounts = nil
                }

                return ClusterLoadPartitionRow(name: name, allowAccounts: allowAccounts)
            }
    }

    static func parseNodeRows(_ output: String) -> [ClusterLoadNodeRow] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { line -> ClusterLoadNodeRow? in
                let fields = parseKeyValueFields(String(line))
                guard let name = normalizedToken(fields["NodeName"]),
                      let partitionsValue = normalizedToken(fields["Partitions"]) else {
                    return nil
                }

                return ClusterLoadNodeRow(
                    name: name,
                    partitions: Set(splitList(partitionsValue)),
                    cfgTRES: parseTRES(fields["CfgTRES"]),
                    allocTRES: parseTRES(fields["AllocTRES"])
                )
            }
    }

    static func makeDiscoveryContext(
        userRows: [ClusterLoadAssociationUserRow],
        userAssocRows: [ClusterLoadAssociationLimitRow],
        accountAssocRows: [ClusterLoadAssociationLimitRow],
        qosRows: [ClusterLoadQoSRow],
        partitions: [ClusterLoadPartitionRow],
        configOutput: String
    ) -> ClusterLoadDiscoveryContext {
        let accountScopeRows = accountAssocRows.filter { row in
            row.user == nil || row.user?.isEmpty == true
        }

        let accounts = Set(
            userRows.map(\.account).filter { !$0.isEmpty }
                + userAssocRows.compactMap(\.account)
                + accountAssocRows.compactMap(\.account)
        )

        let explicitPartitions = Set(
            userRows.compactMap(\.partition)
                + userAssocRows.compactMap(\.partition)
                + accountScopeRows.compactMap(\.partition)
        )
        let preferredQOSOrder = orderedUnique(
            userRows.flatMap(\.qosValues)
                + userAssocRows.flatMap(\.qosValues)
                + accountScopeRows.flatMap(\.qosValues)
        )

        let accessiblePartitions = partitions
            .filter { partition in
                if !explicitPartitions.isEmpty, !explicitPartitions.contains(partition.name) {
                    return false
                }
                guard let allowAccounts = partition.allowAccounts, !allowAccounts.isEmpty else {
                    return true
                }
                return !accounts.isDisjoint(with: allowAccounts)
            }
            .map(\.name)
            .sorted()

        let userLimitCandidates = userAssocRows.filter { row in
            row.user == nil || row.user?.isEmpty == true
        }
        let accountLimitCandidates = accountScopeRows

        let maxRunningJobs = minimumLimit(
            userLimitCandidates.compactMap(\.maxJobs)
                + accountLimitCandidates.compactMap(\.maxJobs)
                + qosRows.compactMap(\.maxJobsPerUser)
        )

        let maxSubmittedJobs = minimumLimit(
            userLimitCandidates.compactMap(\.maxSubmit)
                + accountLimitCandidates.compactMap(\.maxSubmit)
                + qosRows.compactMap(\.maxSubmitPerUser)
        )

        let normalizedConfig = configOutput.lowercased()
        let limitsEnforced = normalizedConfig.contains("accountingstorageenforce =")
            && normalizedConfig.contains("associations")
            && normalizedConfig.contains("limits")
            && normalizedConfig.contains("qos")

        let userGPUCapByQOS = preferredQOSOrder.reduce(into: [String: Int]()) { partialResult, qos in
            let applicableUserRows = userAssocRows.filter { applies(qos: qos, to: $0.qosValues) }
            let candidates = applicableUserRows.compactMap(\.maxGPUCount)
                + qosRows.filter { $0.name == qos }.compactMap(\.maxGPUPerUser)

            if let limit = minimumLimit(candidates) {
                partialResult[qos] = limit
            }
        }

        let accountGPUCapByQOS = preferredQOSOrder.reduce(into: [String: Int]()) { partialResult, qos in
            let applicableAccountRows = accountScopeRows.filter { applies(qos: qos, to: $0.qosValues) }
            let candidates = applicableAccountRows.compactMap(\.groupGPUCount)
                + applicableAccountRows.compactMap(\.maxGPUCount)
                + qosRows.filter { $0.name == qos }.compactMap(\.maxGPUPerAccount)
                + qosRows.filter { $0.name == qos }.compactMap(\.groupGPUCount)

            if let limit = minimumLimit(candidates) {
                partialResult[qos] = limit
            }
        }

        return ClusterLoadDiscoveryContext(
            accessiblePartitions: accessiblePartitions,
            accessibleAccounts: accounts.sorted(),
            preferredQOSOrder: preferredQOSOrder,
            userGPUCapByQOS: userGPUCapByQOS,
            accountGPUCapByQOS: accountGPUCapByQOS,
            maxRunningJobs: maxRunningJobs,
            maxSubmittedJobs: maxSubmittedJobs,
            limitsEnforced: limitsEnforced,
            hasPartitionMetadata: !partitions.isEmpty
        )
    }

    static func makeResourceSummary(
        nodes: [ClusterLoadNodeRow],
        accessiblePartitions: [String]
    ) -> ClusterLoadResourceSummary {
        let partitionFilter = Set(accessiblePartitions)
        let scopedNodes = nodes.filter { node in
            guard !partitionFilter.isEmpty else { return true }
            return !node.partitions.isDisjoint(with: partitionFilter)
        }

        var freeCPUCount = 0
        var totalCPUCount = 0
        var freeGPUCount = 0
        var totalGPUCount = 0
        var freeNodeCount = 0

        for node in scopedNodes {
            let cpuTotal = node.cfgTRES["cpu"] ?? 0
            let cpuAlloc = node.allocTRES["cpu"] ?? 0
            let cpuFree = max(0, cpuTotal - cpuAlloc)

            let gpuTotal = totalGPU(in: node.cfgTRES)
            let gpuAlloc = totalGPU(in: node.allocTRES)
            let gpuFree = max(0, gpuTotal - gpuAlloc)

            totalCPUCount += cpuTotal
            freeCPUCount += cpuFree
            totalGPUCount += gpuTotal
            freeGPUCount += gpuFree

            let hasFreeCapacity = gpuTotal > 0 ? gpuFree > 0 : cpuFree > 0
            if hasFreeCapacity {
                freeNodeCount += 1
            }
        }

        return ClusterLoadResourceSummary(
            freeCPUCount: freeCPUCount,
            totalCPUCount: totalCPUCount,
            freeGPUCount: freeGPUCount,
            totalGPUCount: totalGPUCount,
            freeNodeCount: freeNodeCount,
            totalNodeCount: scopedNodes.count
        )
    }

    static func makeSnapshot(
        discovery: ClusterLoadDiscoveryContext,
        currentJobs: [CurrentJob],
        queueSummary: ClusterQueueSummary?,
        resourceSummary: ClusterLoadResourceSummary?,
        userRunningGPUByQOS: [String: Int],
        accountRunningGPUByQOS: [String: Int],
        scopedRunningGPUCount: Int,
        scopedRunningNodeCount: Int,
        configuredGPUCap: Int?,
        configuredNodeCap: Int?,
        now: Date,
        message: String?
    ) -> ClusterLoadSnapshot {
        let runningJobCount = currentJobs.filter { $0.state == .running }.count
        let submittedJobCount = currentJobs.count
        let pendingJobCount = queueSummary?.pendingJobCount
        let qosGPUAvailabilities = effectiveGPUAvailabilities(
            discovery: discovery,
            currentJobs: currentJobs,
            userRunningGPUByQOS: userRunningGPUByQOS,
            accountRunningGPUByQOS: accountRunningGPUByQOS,
            resourceSummary: resourceSummary
        )
        let scopedGPUAvailability = aggregateGPUAvailability(
            qosGPUAvailabilities: qosGPUAvailabilities,
            resourceSummary: resourceSummary
        )
        let configuredGPUAvailability = configuredScopedGPUAvailability(
            configuredGPUCap: configuredGPUCap,
            configuredNodeCap: configuredNodeCap,
            scopedRunningGPUCount: scopedRunningGPUCount,
            resourceSummary: resourceSummary
        )
        let effectiveScopedGPUAvailability = tighterScopedGPUAvailability(
            scopedGPUAvailability,
            configuredGPUAvailability
        )
        let scopedNodeAvailability = configuredScopedNodeAvailability(
            configuredNodeCap: configuredNodeCap,
            scopedRunningNodeCount: scopedRunningNodeCount,
            resourceSummary: resourceSummary
        )

        let jobHeadroom: Int? = {
            guard discovery.limitsEnforced else { return nil }
            let candidates = [
                discovery.maxRunningJobs.map { max(0, $0 - runningJobCount) },
                discovery.maxSubmittedJobs.map { max(0, $0 - submittedJobCount) }
            ].compactMap { $0 }
            guard !candidates.isEmpty else { return nil }
            return candidates.min()
        }()

        let level = deriveLevel(
            pendingJobCount: pendingJobCount,
            resourceSummary: resourceSummary,
            scopedGPUAvailability: effectiveScopedGPUAvailability,
            scopedNodeAvailability: scopedNodeAvailability,
            jobHeadroom: jobHeadroom
        )

        return ClusterLoadSnapshot(
            level: level,
            jobCount: queueSummary?.totalJobCount,
            pendingJobCount: pendingJobCount,
            scopedFreeGPUCount: effectiveScopedGPUAvailability?.freeGPUCount,
            scopedTotalGPUCount: effectiveScopedGPUAvailability?.totalGPUCount,
            scopedGPUDescription: effectiveScopedGPUAvailability?.description,
            scopedFreeNodeCount: scopedNodeAvailability?.freeNodeCount,
            scopedTotalNodeCount: scopedNodeAvailability?.totalNodeCount,
            scopedNodeDescription: scopedNodeAvailability?.description,
            qosGPUAvailabilities: qosGPUAvailabilities,
            freeCPUCount: resourceSummary?.freeCPUCount,
            totalCPUCount: resourceSummary?.totalCPUCount,
            freeGPUCount: resourceSummary?.freeGPUCount,
            totalGPUCount: resourceSummary?.totalGPUCount,
            freeNodeCount: resourceSummary?.freeNodeCount,
            totalNodeCount: resourceSummary?.totalNodeCount,
            jobHeadroom: jobHeadroom,
            accessiblePartitions: discovery.accessiblePartitions,
            lastUpdatedAt: now,
            message: message
        )
    }

    static func deriveLevel(
        pendingJobCount: Int?,
        resourceSummary: ClusterLoadResourceSummary?,
        scopedGPUAvailability: ClusterScopedGPUAvailability?,
        scopedNodeAvailability: ClusterScopedNodeAvailability?,
        jobHeadroom: Int?
    ) -> ClusterLoadLevel {
        guard pendingJobCount != nil || resourceSummary != nil else {
            return .unknown
        }

        let relevantFree: Int? = {
            if let scopedGPUAvailability {
                return scopedGPUAvailability.freeGPUCount
            }
            if let scopedNodeAvailability {
                return scopedNodeAvailability.freeNodeCount
            }
            guard let resourceSummary else { return nil }
            if resourceSummary.totalGPUCount > 0 {
                return resourceSummary.freeGPUCount
            }
            if resourceSummary.totalCPUCount > 0 {
                return resourceSummary.freeCPUCount
            }
            return resourceSummary.freeNodeCount
        }()

        let relevantTotal: Int? = {
            if let scopedGPUAvailability {
                return scopedGPUAvailability.totalGPUCount
            }
            if let scopedNodeAvailability {
                return scopedNodeAvailability.totalNodeCount
            }
            guard let resourceSummary else { return nil }
            if resourceSummary.totalGPUCount > 0 {
                return resourceSummary.totalGPUCount
            }
            if resourceSummary.totalCPUCount > 0 {
                return resourceSummary.totalCPUCount
            }
            return resourceSummary.totalNodeCount
        }()

        let freeRatio: Double? = {
            guard let relevantFree, let relevantTotal, relevantTotal > 0 else { return nil }
            return Double(relevantFree) / Double(relevantTotal)
        }()

        if jobHeadroom == 0 || relevantFree == 0 {
            return (pendingJobCount ?? 0) > 0 ? .full : .constrained
        }

        if let freeRatio, freeRatio < 0.1 {
            return .constrained
        }

        if let jobHeadroom, jobHeadroom <= 2 {
            return .constrained
        }

        if let pendingJobCount, pendingJobCount > 0 {
            return .busy
        }

        if let freeRatio, freeRatio < 0.3 {
            return .busy
        }

        return .open
    }

    static func runningGPUUsageByQOS(_ jobs: [CurrentJob]) -> [String: Int] {
        jobs.reduce(into: [String: Int]()) { partialResult, job in
            guard job.state == .running,
                  let qos = job.qosName,
                  !qos.isEmpty,
                  let gpuCount = job.gpuCount,
                  gpuCount > 0 else {
                return
            }
            let nodeCount = max(1, job.nodeCount ?? 1)
            partialResult[qos, default: 0] += gpuCount * nodeCount
        }
    }

    static func configuredScopedGPUAvailability(
        configuredGPUCap: Int?,
        configuredNodeCap: Int?,
        scopedRunningGPUCount: Int,
        resourceSummary: ClusterLoadResourceSummary?
    ) -> ClusterScopedGPUAvailability? {
        let effectiveGPUCap: Int? = {
            if let configuredGPUCap, configuredGPUCap > 0 {
                return configuredGPUCap
            }

            guard let configuredNodeCap,
                  configuredNodeCap > 0,
                  let resourceSummary,
                  resourceSummary.totalNodeCount > 0,
                  resourceSummary.totalGPUCount > 0,
                  resourceSummary.totalGPUCount % resourceSummary.totalNodeCount == 0 else {
                return nil
            }

            let gpuPerNode = resourceSummary.totalGPUCount / resourceSummary.totalNodeCount
            return gpuPerNode > 0 ? configuredNodeCap * gpuPerNode : nil
        }()

        guard let effectiveGPUCap, effectiveGPUCap > 0 else { return nil }

        let clusterFreeGPUCount = resourceSummary?.freeGPUCount ?? max(0, effectiveGPUCap - scopedRunningGPUCount)
        let freeGPUCount = min(clusterFreeGPUCount, max(0, effectiveGPUCap - scopedRunningGPUCount))
        let description = configuredGPUCap != nil ? "Configured GPU cap" : "Configured node cap"

        return ClusterScopedGPUAvailability(
            freeGPUCount: freeGPUCount,
            totalGPUCount: effectiveGPUCap,
            description: description
        )
    }

    static func configuredScopedNodeAvailability(
        configuredNodeCap: Int?,
        scopedRunningNodeCount: Int,
        resourceSummary: ClusterLoadResourceSummary?
    ) -> ClusterScopedNodeAvailability? {
        guard let configuredNodeCap, configuredNodeCap > 0 else { return nil }

        let clusterFreeNodeCount = resourceSummary?.freeNodeCount ?? max(0, configuredNodeCap - scopedRunningNodeCount)
        let freeNodeCount = min(clusterFreeNodeCount, max(0, configuredNodeCap - scopedRunningNodeCount))

        return ClusterScopedNodeAvailability(
            freeNodeCount: freeNodeCount,
            totalNodeCount: configuredNodeCap,
            description: "Configured node cap"
        )
    }

    static func effectiveGPUAvailabilities(
        discovery: ClusterLoadDiscoveryContext,
        currentJobs: [CurrentJob],
        userRunningGPUByQOS: [String: Int],
        accountRunningGPUByQOS: [String: Int],
        resourceSummary: ClusterLoadResourceSummary?
    ) -> [ClusterQoSGPUAvailability] {
        guard let resourceSummary, resourceSummary.totalGPUCount > 0 else {
            return []
        }

        let activeQOSOrder = orderedUnique(
            currentJobs
                .filter { $0.state == .running }
                .compactMap(\.qosName)
        )
        let preferredOrder = orderedUnique(activeQOSOrder + discovery.preferredQOSOrder)
        var results: [ClusterQoSGPUAvailability] = []

        for qos in preferredOrder {
            let userCap = discovery.userGPUCapByQOS[qos]
            let accountCap = discovery.accountGPUCapByQOS[qos]
            guard userCap != nil || accountCap != nil else { continue }

            let userFree = userCap.map { max(0, $0 - (userRunningGPUByQOS[qos] ?? 0)) }
            let accountFree = accountCap.map { max(0, $0 - (accountRunningGPUByQOS[qos] ?? 0)) }
            let effectiveFree = minimumLimit((userFree.map { [$0] } ?? []) + (accountFree.map { [$0] } ?? []))
            let effectiveTotal = minimumLimit((userCap.map { [$0] } ?? []) + (accountCap.map { [$0] } ?? []))

            guard let effectiveFree, let effectiveTotal else { continue }

            let cappedFree = min(resourceSummary.freeGPUCount, effectiveFree)
            let description: String
            switch (userCap, accountCap) {
            case (_?, _?):
                description = "user/account cap"
            case (_?, nil):
                description = "user cap"
            case (nil, _?):
                description = "account cap"
            case (nil, nil):
                continue
            }

            results.append(
                ClusterQoSGPUAvailability(
                    qosName: qos,
                    freeGPUCount: cappedFree,
                    totalGPUCount: effectiveTotal,
                    sourceDescription: description
                )
            )
        }

        return results
    }

    static func aggregateGPUAvailability(
        qosGPUAvailabilities: [ClusterQoSGPUAvailability],
        resourceSummary: ClusterLoadResourceSummary?
    ) -> ClusterScopedGPUAvailability? {
        guard !qosGPUAvailabilities.isEmpty else { return nil }

        let summedFree = qosGPUAvailabilities.reduce(0) { $0 + $1.freeGPUCount }
        let summedTotal = qosGPUAvailabilities.reduce(0) { $0 + $1.totalGPUCount }
        let rawFree = resourceSummary?.freeGPUCount ?? summedFree

        return ClusterScopedGPUAvailability(
            freeGPUCount: min(rawFree, summedFree),
            totalGPUCount: summedTotal,
            description: "All QoS"
        )
    }

    static func tighterScopedGPUAvailability(
        _ lhs: ClusterScopedGPUAvailability?,
        _ rhs: ClusterScopedGPUAvailability?
    ) -> ClusterScopedGPUAvailability? {
        switch (lhs, rhs) {
        case let (lhs?, rhs?):
            if lhs.freeGPUCount != rhs.freeGPUCount {
                return lhs.freeGPUCount < rhs.freeGPUCount ? lhs : rhs
            }
            if lhs.totalGPUCount != rhs.totalGPUCount {
                return lhs.totalGPUCount < rhs.totalGPUCount ? lhs : rhs
            }
            return lhs
        case let (lhs?, nil):
            return lhs
        case let (nil, rhs?):
            return rhs
        case (nil, nil):
            return nil
        }
    }

    private static func splitColumns(_ row: String, expectedCount: Int) -> [String] {
        row
            .split(separator: "|", omittingEmptySubsequences: false)
            .map(String.init)
            .prefix(expectedCount)
            .map(\.self)
    }

    private static func splitList(_ rawValue: String) -> [String] {
        rawValue
            .split(separator: ",", omittingEmptySubsequences: true)
            .map { String($0).trimmedOrEmpty }
            .filter { !$0.isEmpty }
    }

    private static func orderedUnique(_ values: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []

        for value in values.map(\.trimmedOrEmpty).filter({ !$0.isEmpty }) {
            if seen.insert(value).inserted {
                result.append(value)
            }
        }

        return result
    }

    private static func applies(qos: String, to qosValues: [String]) -> Bool {
        qosValues.isEmpty || qosValues.contains(qos)
    }

    private static func normalizedToken(_ rawValue: String?) -> String? {
        guard let rawValue else { return nil }
        let value = rawValue.replacingOccurrences(of: "*", with: "").trimmedOrEmpty
        let lowercased = value.lowercased()
        guard !value.isEmpty,
              lowercased != "all",
              lowercased != "(null)",
              lowercased != "none",
              lowercased != "n/a" else {
            return nil
        }
        return value
    }

    private static func parseOptionalLimit(_ rawValue: String) -> Int? {
        let value = rawValue.trimmedOrEmpty
        guard !value.isEmpty else { return nil }
        let lowercased = value.lowercased()
        guard lowercased != "unlimited", lowercased != "none", value != "-1" else {
            return nil
        }
        return Int(value)
    }

    private static func parseGPUCount(inTRESValue rawValue: String) -> Int? {
        let tres = parseTRES(rawValue)
        let count = totalGPU(in: tres)
        return count > 0 ? count : nil
    }

    private static func minimumLimit(_ values: [Int]) -> Int? {
        let filtered = values.filter { $0 >= 0 }
        return filtered.min()
    }

    private static func parseKeyValueFields(_ line: String) -> [String: String] {
        line
            .split(separator: " ", omittingEmptySubsequences: true)
            .reduce(into: [String: String]()) { partialResult, token in
                let parts = token.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard parts.count == 2 else { return }
                partialResult[String(parts[0])] = String(parts[1])
            }
    }

    private static func parseTRES(_ rawValue: String?) -> [String: Int] {
        guard let rawValue else { return [:] }

        return rawValue
            .split(separator: ",", omittingEmptySubsequences: true)
            .reduce(into: [String: Int]()) { partialResult, token in
                let parts = token.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard parts.count == 2 else { return }
                let key = String(parts[0])
                let value = String(parts[1])
                let digits = value.prefix { $0.isNumber || $0 == "-" }
                guard let parsed = Int(digits) else { return }
                partialResult[key] = parsed
            }
    }

    private static func totalGPU(in tres: [String: Int]) -> Int {
        if let total = tres["gres/gpu"] {
            return total
        }

        return tres
            .filter { $0.key.hasPrefix("gres/gpu:") }
            .reduce(0) { $0 + $1.value }
    }

    static func parseGRESGPUCount(_ rawValue: String) -> Int? {
        let value = rawValue.trimmedOrEmpty
        guard !value.isEmpty else { return nil }
        let lowercased = value.lowercased()
        guard lowercased != "(null)", lowercased != "none", lowercased != "n/a" else {
            return nil
        }

        let tokens = value.split(separator: ",", omittingEmptySubsequences: true)
        let counts = tokens.compactMap { token -> Int? in
            let trimmed = String(token).trimmedOrEmpty
            guard trimmed.hasPrefix("gres/gpu") else { return nil }
            let suffix = trimmed.split(separator: ":").last.map(String.init) ?? ""
            return Int(suffix)
        }

        guard !counts.isEmpty else { return nil }
        return counts.reduce(0, +)
    }
}
