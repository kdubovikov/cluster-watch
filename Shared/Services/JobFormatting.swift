import Foundation

public enum JobFormatting {
    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.timeStyle = .short
        formatter.dateStyle = .none
        return formatter
    }()

    private static let dateTimeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()

    public static func startedText(for job: WatchedJob) -> String {
        if let startTime = job.startTime {
            return "Started \(timeFormatter.string(from: startTime))"
        }
        return job.state == .pending ? "Pending start" : "Start unknown"
    }

    public static func startedText(for job: CurrentJob) -> String {
        if let startTime = job.startTime {
            return "Started \(timeFormatter.string(from: startTime))"
        }
        return job.state == .pending ? "Pending start" : "Start unknown"
    }

    public static func timingSummary(for job: WatchedJob, now: Date) -> String {
        "\(waitText(for: job, now: now)) • \(runText(for: job, now: now))"
    }

    public static func timingSummary(for job: CurrentJob, now: Date) -> String {
        "\(waitText(for: job, now: now)) • \(runText(for: job, now: now))"
    }

    public static func waitText(for job: WatchedJob, now: Date) -> String {
        waitText(
            submitTime: job.submitTime,
            startTime: job.startTime,
            state: job.state,
            now: now
        )
    }

    public static func waitText(for job: CurrentJob, now: Date) -> String {
        waitText(
            submitTime: job.submitTime,
            startTime: job.startTime,
            state: job.state,
            now: now
        )
    }

    public static func runText(for job: WatchedJob, now: Date) -> String {
        runText(
            startTime: job.startTime,
            endTime: job.endTime,
            elapsedSeconds: job.elapsedSeconds,
            state: job.state,
            isTerminal: job.isTerminal,
            now: now
        )
    }

    public static func runText(for job: CurrentJob, now: Date) -> String {
        runText(
            startTime: job.startTime,
            endTime: nil,
            elapsedSeconds: job.elapsedSeconds,
            state: job.state,
            isTerminal: job.state.isTerminal,
            now: now
        )
    }

    public static func refreshText(_ date: Date?) -> String {
        guard let date else { return "Never refreshed" }
        return "Last success \(dateTimeFormatter.string(from: date))"
    }

    public static func absoluteDateText(_ date: Date?) -> String {
        guard let date else { return "Unknown" }
        return dateTimeFormatter.string(from: date)
    }

    public static func formattedLaunchContent(_ content: String, mode: JobLaunchMode) -> String {
        switch mode {
        case .command:
            return formatShellLine(content)
        case .batchScript:
            return formatShellScript(content)
        }
    }

    public static func dependencySummary(
        state: NormalizedJobState,
        dependencyStatus: JobDependencyStatus,
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamJobs: [WatchedJob]
    ) -> String? {
        guard dependencyStatus != .none else { return nil }

        let references = jobReferenceText(
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamJobs: upstreamJobs
        )

        switch dependencyStatus {
        case .neverSatisfied:
            return references.map { "Dependency never satisfied: \($0)" } ?? "Dependency never satisfied"
        case .waiting:
            return references.map { "Waiting on \($0)" } ?? "Waiting on dependency"
        case .satisfied:
            if state == .running || state.isTerminal {
                return references.map { "Started after \($0)" } ?? "Dependencies resolved"
            }
            return references.map { "Dependencies resolved: \($0)" } ?? "Dependencies resolved"
        case .none:
            return nil
        }
    }

    public static func dependencySummary(
        state: NormalizedJobState,
        dependencyStatus: JobDependencyStatus,
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamJobs: [CurrentJob]
    ) -> String? {
        dependencySummary(
            state: state,
            dependencyStatus: dependencyStatus,
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamLabels: upstreamJobs.map { "\($0.jobName) (#\($0.jobID))" }
        )
    }

    public static func dependencyReferenceText(
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamJobs: [WatchedJob]
    ) -> String? {
        jobReferenceText(
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamJobs: upstreamJobs
        )
    }

    public static func dependencyReferenceText(
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamJobs: [CurrentJob]
    ) -> String? {
        jobReferenceText(
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamLabels: upstreamJobs.map { "\($0.jobName) (#\($0.jobID))" }
        )
    }

    public static func downstreamSummary(_ downstreamJobs: [WatchedJob]) -> String? {
        guard !downstreamJobs.isEmpty else { return nil }

        let jobLabels = downstreamJobs.map { "\($0.jobName) (#\($0.jobID))" }
        return "Unblocks \(abbreviatedList(jobLabels))"
    }

    public static func downstreamSummary(_ downstreamJobs: [CurrentJob]) -> String? {
        guard !downstreamJobs.isEmpty else { return nil }

        let jobLabels = downstreamJobs.map { "\($0.jobName) (#\($0.jobID))" }
        return "Unblocks \(abbreviatedList(jobLabels))"
    }

    public static func pendingReasonSummary(_ reason: String?, dependencyStatus: JobDependencyStatus) -> String? {
        guard dependencyStatus == .none || dependencyStatus == .satisfied else { return nil }
        guard let reason = prettify(reason) else { return nil }
        return "Reason: \(reason)"
    }

    public static func durationText(_ interval: TimeInterval?) -> String {
        guard let interval else { return "Unknown" }
        return durationText(interval)
    }

    public static func durationText(_ interval: TimeInterval) -> String {
        let totalSeconds = max(0, Int(interval.rounded()))
        let days = totalSeconds / 86_400
        let hours = (totalSeconds % 86_400) / 3_600
        let minutes = (totalSeconds % 3_600) / 60
        let seconds = totalSeconds % 60

        var parts: [String] = []

        if days > 0 {
            parts.append("\(days)d")
        }
        if hours > 0 || !parts.isEmpty {
            parts.append("\(hours)h")
        }
        if minutes > 0 || !parts.isEmpty {
            parts.append("\(minutes)m")
        }
        if parts.isEmpty || parts.count < 2 {
            parts.append("\(seconds)s")
        }

        return parts.prefix(2).joined(separator: " ")
    }

    private static func waitText(
        submitTime: Date?,
        startTime: Date?,
        state: NormalizedJobState,
        now: Date
    ) -> String {
        guard let submitTime else {
            return state == .pending ? "Waiting unknown" : "Wait unknown"
        }

        if let startTime {
            return "Waited \(durationText(max(0, startTime.timeIntervalSince(submitTime))))"
        }

        if state == .pending {
            return "Waiting \(durationText(max(0, now.timeIntervalSince(submitTime))))"
        }

        return "Wait unknown"
    }

    private static func runText(
        startTime: Date?,
        endTime: Date?,
        elapsedSeconds: TimeInterval?,
        state: NormalizedJobState,
        isTerminal: Bool,
        now: Date
    ) -> String {
        if state == .unknown {
            if let elapsedSeconds, elapsedSeconds > 0 {
                return "Last ran \(durationText(elapsedSeconds))"
            }
            return "Run unknown"
        }

        if let startTime {
            if let endTime {
                return "Ran \(durationText(max(0, endTime.timeIntervalSince(startTime))))"
            }
            if isTerminal, let elapsedSeconds {
                return "Ran \(durationText(elapsedSeconds))"
            }
            return "Running \(durationText(max(0, now.timeIntervalSince(startTime))))"
        }

        if state == .pending {
            return "Running not started"
        }

        if let elapsedSeconds, elapsedSeconds > 0 {
            return isTerminal ? "Ran \(durationText(elapsedSeconds))" : "Running \(durationText(elapsedSeconds))"
        }

        return "Running not started"
    }

    private static func dependencySummary(
        state: NormalizedJobState,
        dependencyStatus: JobDependencyStatus,
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamLabels: [String]
    ) -> String? {
        guard dependencyStatus != .none else { return nil }

        let references = jobReferenceText(
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamLabels: upstreamLabels
        )

        switch dependencyStatus {
        case .neverSatisfied:
            return references.map { "Dependency never satisfied: \($0)" } ?? "Dependency never satisfied"
        case .waiting:
            return references.map { "Waiting on \($0)" } ?? "Waiting on dependency"
        case .satisfied:
            if state == .running || state.isTerminal {
                return references.map { "Started after \($0)" } ?? "Dependencies resolved"
            }
            return references.map { "Dependencies resolved: \($0)" } ?? "Dependencies resolved"
        case .none:
            return nil
        }
    }

    private static func jobReferenceText(
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamJobs: [WatchedJob]
    ) -> String? {
        jobReferenceText(
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            upstreamLabels: upstreamJobs.map { "\($0.jobName) (#\($0.jobID))" }
        )
    }

    private static func jobReferenceText(
        dependencyExpression: String?,
        dependencyJobIDs: [String],
        upstreamLabels: [String]
    ) -> String? {
        var labels: [String] = []
        var seen = Set<String>()

        for label in upstreamLabels {
            guard seen.insert(label).inserted else { continue }
            labels.append(label)
        }

        for jobID in dependencyJobIDs {
            let baseID = JobIdentifier.baseID(for: jobID)
            guard seen.insert(baseID).inserted else { continue }
            labels.append("#\(jobID)")
        }

        if !labels.isEmpty {
            return abbreviatedList(labels)
        }

        guard let dependencyExpression = dependencyExpression?.trimmedOrEmpty, !dependencyExpression.isEmpty else {
            return nil
        }

        return dependencyExpression
    }

    private static func abbreviatedList(_ values: [String], limit: Int = 3) -> String {
        guard values.count > limit else {
            return values.joined(separator: ", ")
        }

        let remainder = values.count - limit
        return values.prefix(limit).joined(separator: ", ") + " +\(remainder) more"
    }

    private static func prettify(_ reason: String?) -> String? {
        guard let reason = reason?.trimmedOrEmpty, !reason.isEmpty else { return nil }

        let withSpacesBetweenWords = reason.replacingOccurrences(
            of: "([a-z0-9])([A-Z])",
            with: "$1 $2",
            options: .regularExpression
        )

        return withSpacesBetweenWords.replacingOccurrences(of: "_", with: " ")
    }

    private static func formatShellScript(_ script: String) -> String {
        let lines = script.components(separatedBy: .newlines)

        return lines
            .map { line in
                guard shouldFormatShellLine(line) else { return line }
                return formatShellLine(line)
            }
            .joined(separator: "\n")
    }

    private static func shouldFormatShellLine(_ line: String) -> Bool {
        let trimmed = line.trimmedOrEmpty
        guard !trimmed.isEmpty else { return false }
        guard !trimmed.hasPrefix("#") else { return false }
        guard trimmed.count > 100 else { return false }

        guard let tokens = shellTokens(in: trimmed) else { return false }
        return tokens.count >= 5
    }

    private static func formatShellLine(_ line: String) -> String {
        let leadingWhitespace = String(line.prefix { $0.isWhitespace })
        let trimmed = line.trimmingCharacters(in: .whitespaces)

        guard shouldFormatShellLine(trimmed), let tokens = shellTokens(in: trimmed) else {
            return line
        }

        let prefixLength = commandPrefixLength(in: tokens)
        guard prefixLength < tokens.count else { return line }

        let prefix = tokens.prefix(prefixLength).joined(separator: " ")
        let groupedArguments = groupedShellArguments(from: tokens, startingAt: prefixLength)
        let continuationIndent = leadingWhitespace + "  "

        let wrappedArguments = groupedArguments.enumerated().map { index, argument in
            let suffix = index == groupedArguments.count - 1 ? "" : " \\"
            return "\(continuationIndent)\(argument)\(suffix)"
        }

        return ([leadingWhitespace + prefix + " \\"] + wrappedArguments).joined(separator: "\n")
    }

    private static func shellTokens(in line: String) -> [String]? {
        var tokens: [String] = []
        var current = ""
        var activeQuote: Character?
        var isEscaping = false

        for character in line {
            if isEscaping {
                current.append(character)
                isEscaping = false
                continue
            }

            if character == "\\" {
                current.append(character)
                isEscaping = true
                continue
            }

            if let quote = activeQuote {
                current.append(character)
                if character == quote {
                    activeQuote = nil
                }
                continue
            }

            if character == "\"" || character == "'" {
                current.append(character)
                activeQuote = character
                continue
            }

            if character.isWhitespace {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
            } else {
                current.append(character)
            }
        }

        guard !isEscaping, activeQuote == nil else { return nil }

        if !current.isEmpty {
            tokens.append(current)
        }

        return tokens
    }

    private static func commandPrefixLength(in tokens: [String]) -> Int {
        var index = 0

        while index < tokens.count, looksLikeEnvironmentAssignment(tokens[index]) {
            index += 1
        }

        guard index < tokens.count else { return tokens.count }

        let commandToken = tokens[index]
        index += 1

        let commandName = URL(fileURLWithPath: commandToken).lastPathComponent.lowercased()

        if ["python", "python3", "python3.11", "python3.12", "bash", "sh", "zsh", "node", "ruby", "perl", "swift", "rscript"].contains(commandName) {
            if index < tokens.count, tokens[index] == "-m", index + 1 < tokens.count {
                index += 2
            } else if index < tokens.count, isLikelyScriptOperand(tokens[index]) {
                index += 1
            }
        }

        return min(index, tokens.count)
    }

    private static func groupedShellArguments(from tokens: [String], startingAt startIndex: Int) -> [String] {
        var groups: [String] = []
        var index = startIndex

        while index < tokens.count {
            let token = tokens[index]

            if index + 1 < tokens.count, shouldJoinArgumentPair(current: token, next: tokens[index + 1]) {
                groups.append("\(token) \(tokens[index + 1])")
                index += 2
            } else {
                groups.append(token)
                index += 1
            }
        }

        return groups
    }

    private static func shouldJoinArgumentPair(current: String, next: String) -> Bool {
        guard current.hasPrefix("-") else { return false }
        guard current != "--" else { return false }
        guard !next.hasPrefix("-") else { return false }
        guard !looksLikeEnvironmentAssignment(next) else { return false }
        return true
    }

    private static func looksLikeEnvironmentAssignment(_ token: String) -> Bool {
        guard let equalsIndex = token.firstIndex(of: "="), equalsIndex != token.startIndex else {
            return false
        }

        let key = token[..<equalsIndex]
        return key.allSatisfy { character in
            character.isLetter || character.isNumber || character == "_" || character == "."
        }
    }

    private static func isLikelyScriptOperand(_ token: String) -> Bool {
        guard !token.hasPrefix("-") else { return false }
        guard !looksLikeEnvironmentAssignment(token) else { return false }
        return true
    }
}
