import Foundation

public enum SlurmParsing {
    public static let squeueFormat = "%i|%u|%T|%j|%V|%S|%M|%E|%r|%q|%b"
    public static let sacctFormat = "JobIDRaw,User,State,JobName,Submit,Start,End,Elapsed,Reason"
    public static let sacctLogFormat = "JobIDRaw,StdOut,StdErr,WorkDir"

    public static func parseCurrentJobs(output: String, clusterID: ClusterID) -> [CurrentJob] {
        output
            .split(whereSeparator: \.isNewline)
            .compactMap { parseCurrentJobRow(String($0), clusterID: clusterID) }
    }

    public static func parseHistoricalJob(output: String, clusterID: ClusterID, requestedJobID: String) -> JobSnapshot? {
        let rows = output
            .split(whereSeparator: \.isNewline)
            .compactMap { parseHistoricalRow(String($0)) }
            .filter { !$0.jobID.contains(".") }

        if let exact = rows.first(where: { $0.jobID == requestedJobID }) {
            return exact.snapshot
        }

        let requestedBase = baseJobID(for: requestedJobID)
        return rows.first(where: { baseJobID(for: $0.jobID) == requestedBase })?.snapshot
    }

    public static func parseHistoricalLogPaths(output: String, requestedJobID: String) -> JobLogPaths? {
        let rows = output
            .split(whereSeparator: \.isNewline)
            .compactMap { parseHistoricalLogRow(String($0)) }
            .filter { !$0.jobID.contains(".") }

        if let exact = rows.first(where: { $0.jobID == requestedJobID }) {
            return exact.logPaths
        }

        let requestedBase = baseJobID(for: requestedJobID)
        return rows.first(where: { baseJobID(for: $0.jobID) == requestedBase })?.logPaths
    }

    public static func parseScontrolLogPaths(output: String) -> JobLogPaths? {
        let stdoutPath = fieldValue("StdOut", in: output)
        let stderrPath = fieldValue("StdErr", in: output)
        let workDirectory = fieldValue("WorkDir", in: output)

        let logPaths = JobLogPaths(stdoutPath: stdoutPath, stderrPath: stderrPath, workDirectory: workDirectory)
        return logPaths.hasAnyPath || logPaths.workDirectory != nil ? logPaths : nil
    }

    public static func parseScontrolLaunchDetails(output: String) -> JobLaunchDetails? {
        let details = JobLaunchDetails(
            commandText: lineFieldValue("Command", in: output),
            workDirectory: fieldValue("WorkDir", in: output)
        )

        return details.hasAnyContent || details.workDirectory != nil ? details : nil
    }

    public static func parseBatchScript(output: String) -> String? {
        let script = output.trimmingCharacters(in: .whitespacesAndNewlines)
        return script.isEmpty ? nil : script
    }

    private static func parseCurrentJobRow(_ row: String, clusterID: ClusterID) -> CurrentJob? {
        let parts = splitColumns(row, expectedCount: 11)
        guard parts.count == 11 else { return nil }

        let rawState = parts[2]
        let state = NormalizedJobState(rawSlurmState: rawState)
        let parsedStartTime = SlurmDateParser.parse(parts[5])
        let pendingReason = SlurmDependencyParser.parseReason(parts[8])
        let dependencyExpression = SlurmDependencyParser.parseExpression(parts[7])
        let dependencyJobIDs = SlurmDependencyParser.parseJobIDs(from: dependencyExpression)
        return CurrentJob(
            clusterID: clusterID,
            jobID: parts[0],
            jobName: parts[3],
            owner: parts[1],
            state: state,
            rawState: rawState,
            submitTime: SlurmDateParser.parse(parts[4]),
            startTime: state == .pending ? nil : parsedStartTime,
            elapsedSeconds: SlurmDurationParser.parse(parts[6]),
            pendingReason: pendingReason,
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            dependencyIsActive: SlurmDependencyParser.isDependencyActive(
                state: state,
                pendingReason: pendingReason,
                dependencyExpression: dependencyExpression
            ),
            qosName: parseToken(parts[9]),
            gpuCount: parseGRESGPUCount(parts[10])
        )
    }

    private static func parseHistoricalRow(_ row: String) -> HistoricalRow? {
        let parts = splitColumns(row, expectedCount: 9)
        guard parts.count == 9 else { return nil }

        let rawState = parts[2]
        return HistoricalRow(
            jobID: parts[0],
            owner: parts[1],
            rawState: rawState,
            jobName: parts[3],
            submitTime: SlurmDateParser.parse(parts[4]),
            startTime: SlurmDateParser.parse(parts[5]),
            endTime: SlurmDateParser.parse(parts[6]),
            elapsedSeconds: SlurmDurationParser.parse(parts[7]),
            pendingReason: SlurmDependencyParser.parseReason(parts[8])
        )
    }

    private static func parseHistoricalLogRow(_ row: String) -> HistoricalLogRow? {
        let parts = splitColumns(row, expectedCount: 4)
        guard parts.count == 4 else { return nil }

        return HistoricalLogRow(
            jobID: parts[0],
            stdoutPath: parts[1],
            stderrPath: parts[2],
            workDirectory: parts[3]
        )
    }

    private static func splitColumns(_ row: String, expectedCount: Int) -> [String] {
        row
            .split(separator: "|", omittingEmptySubsequences: false)
            .map { String($0) }
            .prefix(expectedCount)
            .map(\.self)
    }

    private static func baseJobID(for jobID: String) -> String {
        jobID.split(separator: ".").first.map(String.init) ?? jobID
    }

    private static func fieldValue(_ field: String, in output: String) -> String? {
        let pattern = "(?:^|\\s)\(NSRegularExpression.escapedPattern(for: field))=(\\S+)"
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return nil
        }

        let range = NSRange(output.startIndex..<output.endIndex, in: output)
        guard let match = regex.firstMatch(in: output, range: range),
              match.numberOfRanges > 1,
              let valueRange = Range(match.range(at: 1), in: output) else {
            return nil
        }

        let value = String(output[valueRange]).trimmedOrEmpty
        let lowercased = value.lowercased()
        guard !value.isEmpty, lowercased != "(null)", lowercased != "none", lowercased != "n/a" else {
            return nil
        }
        return value
    }

    private static func lineFieldValue(_ field: String, in output: String) -> String? {
        let pattern = "(?:^|\\n)\\s*\(NSRegularExpression.escapedPattern(for: field))=([^\\n]*)"
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return nil
        }

        let range = NSRange(output.startIndex..<output.endIndex, in: output)
        guard let match = regex.firstMatch(in: output, range: range),
              match.numberOfRanges > 1,
              let valueRange = Range(match.range(at: 1), in: output) else {
            return nil
        }

        let value = String(output[valueRange]).trimmedOrEmpty
        let lowercased = value.lowercased()
        guard !value.isEmpty, lowercased != "(null)", lowercased != "none", lowercased != "n/a" else {
            return nil
        }
        return value
    }

    private static func parseToken(_ rawValue: String) -> String? {
        let value = rawValue.trimmedOrEmpty
        let lowercased = value.lowercased()
        guard !value.isEmpty, lowercased != "(null)", lowercased != "none", lowercased != "n/a" else {
            return nil
        }
        return value
    }

    private static func parseGRESGPUCount(_ rawValue: String) -> Int? {
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

    private struct HistoricalRow {
        var jobID: String
        var owner: String
        var rawState: String
        var jobName: String
        var submitTime: Date?
        var startTime: Date?
        var endTime: Date?
        var elapsedSeconds: TimeInterval?
        var pendingReason: String?

        var snapshot: JobSnapshot {
            JobSnapshot(
                jobID: jobID,
                owner: owner,
                state: NormalizedJobState(rawSlurmState: rawState),
                rawState: rawState,
                jobName: jobName,
                submitTime: submitTime,
                startTime: startTime,
                endTime: endTime,
                elapsedSeconds: elapsedSeconds,
                pendingReason: pendingReason,
                dependencyIsActive: SlurmDependencyParser.isDependencyActive(
                    state: NormalizedJobState(rawSlurmState: rawState),
                    pendingReason: pendingReason,
                    dependencyExpression: nil
                )
            )
        }
    }

    private struct HistoricalLogRow {
        var jobID: String
        var stdoutPath: String
        var stderrPath: String
        var workDirectory: String

        var logPaths: JobLogPaths {
            JobLogPaths(
                stdoutPath: stdoutPath,
                stderrPath: stderrPath,
                workDirectory: workDirectory
            )
        }
    }
}

public enum SlurmDependencyParser {
    private static let nilValues: Set<String> = ["", "null", "none", "n/a", "(null)"]

    public static func parseExpression(_ rawValue: String) -> String? {
        let value = rawValue.trimmedOrEmpty
        guard !nilValues.contains(value.lowercased()) else { return nil }
        return value
    }

    public static func parseReason(_ rawValue: String) -> String? {
        let value = rawValue.trimmedOrEmpty
        guard !nilValues.contains(value.lowercased()) else { return nil }
        return value
    }

    public static func parseJobIDs(from expression: String?) -> [String] {
        guard let expression, !expression.isEmpty else { return [] }

        let pattern = #"(?:(?<=:)|(?<=,)|(?<=\?))([0-9]+(?:_[0-9]+)?)"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }

        let range = NSRange(expression.startIndex..<expression.endIndex, in: expression)
        let matches = regex.matches(in: expression, range: range)

        var orderedJobIDs: [String] = []
        var seen = Set<String>()

        for match in matches {
            guard match.numberOfRanges > 1,
                  let captureRange = Range(match.range(at: 1), in: expression) else { continue }

            let jobID = String(expression[captureRange])
            if seen.insert(jobID).inserted {
                orderedJobIDs.append(jobID)
            }
        }

        return orderedJobIDs
    }

    public static func isDependencyActive(
        state: NormalizedJobState,
        pendingReason: String?,
        dependencyExpression: String?
    ) -> Bool {
        guard state == .pending else { return false }

        let normalizedReason = JobDependencyStatus.normalizedReasonToken(from: pendingReason)
        return normalizedReason == "DEPENDENCY"
            || normalizedReason == "DEPENDENCYNEVERSATISFIED"
            || parseExpression(dependencyExpression ?? "") != nil
    }
}

public enum SlurmDateParser {
    private static let nilValues: Set<String> = ["", "unknown", "n/a", "none", "invalid", "nan", "none assigned"]

    private static let plainFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = .current
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return formatter
    }()

    private static let plainFormatterNoSeconds: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = .current
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm"
        return formatter
    }()

    private static let spacedFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = .current
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()

    public static func parse(_ rawValue: String) -> Date? {
        let value = rawValue.trimmedOrEmpty
        guard !nilValues.contains(value.lowercased()) else { return nil }

        let isoFormatter = ISO8601DateFormatter()
        isoFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = isoFormatter.date(from: value) {
            return date
        }

        let isoFormatterWithoutFractionalSeconds = ISO8601DateFormatter()
        isoFormatterWithoutFractionalSeconds.formatOptions = [.withInternetDateTime]
        if let date = isoFormatterWithoutFractionalSeconds.date(from: value) {
            return date
        }
        if let date = plainFormatter.date(from: value) {
            return date
        }
        if let date = plainFormatterNoSeconds.date(from: value) {
            return date
        }
        return spacedFormatter.date(from: value)
    }
}

public enum SlurmDurationParser {
    public static func parse(_ rawValue: String) -> TimeInterval? {
        let value = rawValue.trimmedOrEmpty
        guard !value.isEmpty else { return nil }

        let lowercased = value.lowercased()
        guard lowercased != "unknown", lowercased != "n/a", lowercased != "none" else { return nil }

        let dayAndClock = value.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)
        let dayCount: Int
        let clockPart: Substring

        if dayAndClock.count == 2 {
            dayCount = Int(dayAndClock[0]) ?? 0
            clockPart = dayAndClock[1]
        } else {
            dayCount = 0
            clockPart = dayAndClock[0]
        }

        let timeComponents = clockPart.split(separator: ":").map(String.init)
        guard (2...3).contains(timeComponents.count) else { return nil }

        let hours: Int
        let minutes: Int
        let seconds: Int

        if timeComponents.count == 3 {
            hours = Int(timeComponents[0]) ?? 0
            minutes = Int(timeComponents[1]) ?? 0
            seconds = Int(timeComponents[2]) ?? 0
        } else {
            hours = 0
            minutes = Int(timeComponents[0]) ?? 0
            seconds = Int(timeComponents[1]) ?? 0
        }

        return TimeInterval((dayCount * 86_400) + (hours * 3_600) + (minutes * 60) + seconds)
    }
}
