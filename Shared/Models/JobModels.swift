import Foundation

public enum NormalizedJobState: String, Codable, Hashable, CaseIterable, Sendable {
    case pending
    case running
    case completed
    case failed
    case cancelled
    case timeout
    case outOfMemory
    case nodeFail
    case preempted
    case unknown

    public init(rawSlurmState: String) {
        let normalized = rawSlurmState
            .trimmedOrEmpty
            .uppercased()
            .replacingOccurrences(of: " ", with: "_")

        if normalized.hasPrefix("RUNNING") || normalized.hasPrefix("COMPLETING") || normalized.hasPrefix("SUSPENDED") || normalized.hasPrefix("STOPPED") {
            self = .running
        } else if normalized.hasPrefix("PENDING") || normalized.hasPrefix("CONFIGURING") {
            self = .pending
        } else if normalized.hasPrefix("COMPLETED") {
            self = .completed
        } else if normalized.hasPrefix("FAILED") {
            self = .failed
        } else if normalized.hasPrefix("CANCELLED") {
            self = .cancelled
        } else if normalized.hasPrefix("TIMEOUT") {
            self = .timeout
        } else if normalized.hasPrefix("OUT_OF_MEMORY") {
            self = .outOfMemory
        } else if normalized.hasPrefix("NODE_FAIL") {
            self = .nodeFail
        } else if normalized.hasPrefix("PREEMPTED") {
            self = .preempted
        } else {
            self = .unknown
        }
    }

    public var isTerminal: Bool {
        switch self {
        case .completed, .failed, .cancelled, .timeout, .outOfMemory, .nodeFail, .preempted:
            return true
        case .pending, .running, .unknown:
            return false
        }
    }

    public var badgeTitle: String {
        switch self {
        case .running:
            return "Running"
        case .pending:
            return "Pending"
        case .completed:
            return "Completed"
        case .failed, .timeout, .outOfMemory, .nodeFail, .preempted:
            return "Failed"
        case .cancelled:
            return "Cancelled"
        case .unknown:
            return "Unknown"
        }
    }

    public var sortPriority: Int {
        switch self {
        case .running:
            return 0
        case .pending:
            return 1
        case .unknown:
            return 2
        case .completed, .failed, .cancelled, .timeout, .outOfMemory, .nodeFail, .preempted:
            return 3
        }
    }
}

public enum JobDependencyStatus: String, Codable, Hashable, Sendable {
    case none
    case waiting
    case satisfied
    case neverSatisfied

    public static func derive(
        state: NormalizedJobState,
        pendingReason: String?,
        hasDependencyInfo: Bool,
        dependencyIsActive: Bool
    ) -> JobDependencyStatus {
        let normalizedReason = normalizedReasonToken(from: pendingReason)

        if normalizedReason == "DEPENDENCYNEVERSATISFIED" {
            return .neverSatisfied
        }

        if state == .pending, dependencyIsActive || normalizedReason == "DEPENDENCY" {
            return .waiting
        }

        if hasDependencyInfo {
            return .satisfied
        }

        return .none
    }

    public static func normalizedReasonToken(from reason: String?) -> String? {
        let token = reason?.trimmedOrEmpty ?? ""
        guard !token.isEmpty else { return nil }
        return token
            .replacingOccurrences(of: " ", with: "")
            .replacingOccurrences(of: "_", with: "")
            .uppercased()
    }
}

public enum JobIdentifier {
    public static func baseID(for jobID: String) -> String {
        let withoutStep = jobID.split(separator: ".").first.map(String.init) ?? jobID
        if let rangeStart = withoutStep.range(of: "_[") {
            return String(withoutStep[..<rangeStart.lowerBound])
        }
        if let bracketStart = withoutStep.firstIndex(of: "[") {
            return String(withoutStep[..<bracketStart])
        }
        return withoutStep
    }

    public static func schedulerLookupID(for jobID: String) -> String {
        let trimmed = jobID.trimmedOrEmpty
        guard !trimmed.isEmpty else { return trimmed }

        if trimmed.contains("_[") || trimmed.contains("[") {
            return baseID(for: trimmed)
        }

        return trimmed.split(separator: ".").first.map(String.init) ?? trimmed
    }

    public static func arrayParentID(for jobID: String) -> String {
        let baseID = baseID(for: jobID)
        guard let underscore = baseID.lastIndex(of: "_") else { return baseID }

        let suffixStart = baseID.index(after: underscore)
        let suffix = baseID[suffixStart...]
        guard !suffix.isEmpty, suffix.allSatisfy(\.isNumber) else { return baseID }
        return String(baseID[..<underscore])
    }

    public static func arrayTaskID(for jobID: String) -> Int? {
        let baseID = baseID(for: jobID)
        guard let underscore = baseID.lastIndex(of: "_") else { return nil }

        let suffixStart = baseID.index(after: underscore)
        let suffix = baseID[suffixStart...]
        guard !suffix.isEmpty, suffix.allSatisfy(\.isNumber) else { return nil }
        return Int(suffix)
    }

    public static func arrayTaskIDs(for jobID: String) -> Set<Int>? {
        guard let openBracket = jobID.firstIndex(of: "["),
              let closeBracket = jobID[openBracket...].firstIndex(of: "]") else {
            return nil
        }

        let rawExpression = String(jobID[jobID.index(after: openBracket)..<closeBracket])
        let rangeExpression = rawExpression.split(separator: "%", maxSplits: 1).first.map(String.init) ?? rawExpression
        let taskIDs = rangeExpression
            .split(separator: ",")
            .reduce(into: Set<Int>()) { taskIDs, token in
                let bounds = token.split(separator: "-", maxSplits: 1)
                if bounds.count == 2, let lower = Int(bounds[0]), let upper = Int(bounds[1]), lower <= upper {
                    taskIDs.formUnion(lower...upper)
                } else if bounds.count == 1, let taskID = Int(bounds[0]) {
                    taskIDs.insert(taskID)
                }
            }

        return taskIDs.isEmpty ? nil : taskIDs
    }
}

public struct JobLogPaths: Codable, Hashable, Sendable {
    public var stdoutPath: String?
    public var stderrPath: String?
    public var workDirectory: String?

    public init(stdoutPath: String? = nil, stderrPath: String? = nil, workDirectory: String? = nil) {
        self.stdoutPath = stdoutPath?.trimmedOrEmpty.nilIfEmpty
        self.stderrPath = stderrPath?.trimmedOrEmpty.nilIfEmpty
        self.workDirectory = workDirectory?.trimmedOrEmpty.nilIfEmpty
    }

    public var hasAnyPath: Bool {
        stdoutPath != nil || stderrPath != nil
    }
}

public struct JobLaunchDetails: Codable, Hashable, Sendable {
    public var commandText: String?
    public var batchScriptText: String?
    public var workDirectory: String?

    public init(commandText: String? = nil, batchScriptText: String? = nil, workDirectory: String? = nil) {
        self.commandText = commandText?.trimmedOrEmpty.nilIfEmpty
        self.batchScriptText = batchScriptText?.trimmedOrEmpty.nilIfEmpty
        self.workDirectory = workDirectory?.trimmedOrEmpty.nilIfEmpty
    }

    public var hasAnyContent: Bool {
        commandText != nil || batchScriptText != nil
    }

    public var availableModes: [JobLaunchMode] {
        JobLaunchMode.allCases.filter { content(for: $0) != nil }
    }

    public var preferredMode: JobLaunchMode {
        if batchScriptText != nil {
            return .batchScript
        }
        return .command
    }

    public func content(for mode: JobLaunchMode) -> String? {
        switch mode {
        case .command:
            return commandText
        case .batchScript:
            return batchScriptText
        }
    }
}

public enum JobLaunchMode: String, Codable, CaseIterable, Hashable, Identifiable, Sendable {
    case command
    case batchScript

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .command:
            return "Command"
        case .batchScript:
            return "Batch Script"
        }
    }
}

public enum JobInspectorTab: String, Codable, CaseIterable, Hashable, Identifiable, Sendable {
    case logs
    case command

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .logs:
            return "Logs"
        case .command:
            return "Command"
        }
    }
}

public struct JobInspectorSession: Hashable, Identifiable, Sendable {
    public var clusterID: ClusterID
    public var clusterName: String
    public var jobID: String
    public var jobName: String
    public var logPaths: JobLogPaths?
    public var preferredLogStream: JobLogStream
    public var launchDetails: JobLaunchDetails?
    public var preferredLaunchMode: JobLaunchMode
    public var preferredTab: JobInspectorTab

    public init(
        clusterID: ClusterID,
        clusterName: String,
        jobID: String,
        jobName: String,
        logPaths: JobLogPaths? = nil,
        preferredLogStream: JobLogStream = .stdout,
        launchDetails: JobLaunchDetails? = nil,
        preferredLaunchMode: JobLaunchMode = .command,
        preferredTab: JobInspectorTab
    ) {
        self.clusterID = clusterID
        self.clusterName = clusterName
        self.jobID = jobID
        self.jobName = jobName
        self.logPaths = logPaths
        self.preferredLogStream = preferredLogStream
        self.launchDetails = launchDetails
        self.preferredLaunchMode = preferredLaunchMode
        self.preferredTab = preferredTab
    }

    public var id: String {
        "\(clusterID.rawValue):\(jobID)"
    }

    public func path(for stream: JobLogStream) -> String? {
        switch stream {
        case .stdout:
            return logPaths?.stdoutPath
        case .stderr:
            return logPaths?.stderrPath
        }
    }

    public var availableStreams: [JobLogStream] {
        JobLogStream.allCases.filter { path(for: $0) != nil }
    }
}

public struct JobLaunchSession: Hashable, Identifiable, Sendable {
    public var clusterID: ClusterID
    public var clusterName: String
    public var jobID: String
    public var jobName: String
    public var details: JobLaunchDetails
    public var preferredMode: JobLaunchMode

    public init(
        clusterID: ClusterID,
        clusterName: String,
        jobID: String,
        jobName: String,
        details: JobLaunchDetails,
        preferredMode: JobLaunchMode
    ) {
        self.clusterID = clusterID
        self.clusterName = clusterName
        self.jobID = jobID
        self.jobName = jobName
        self.details = details
        self.preferredMode = preferredMode
    }

    public var id: String {
        "\(clusterID.rawValue):\(jobID)"
    }
}

public enum JobLogStream: String, Codable, CaseIterable, Hashable, Identifiable, Sendable {
    case stdout
    case stderr

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .stdout:
            return "Stdout"
        case .stderr:
            return "Stderr"
        }
    }
}

public enum ClusterLoadLevel: String, Codable, CaseIterable, Hashable, Sendable {
    case open
    case busy
    case constrained
    case full
    case unknown

    public var title: String {
        switch self {
        case .open:
            return "Open"
        case .busy:
            return "Busy"
        case .constrained:
            return "Constrained"
        case .full:
            return "Full"
        case .unknown:
            return "Unknown"
        }
    }
}

public struct ClusterQoSGPUAvailability: Hashable, Sendable {
    public var qosName: String
    public var freeGPUCount: Int
    public var totalGPUCount: Int
    public var sourceDescription: String?

    public init(qosName: String, freeGPUCount: Int, totalGPUCount: Int, sourceDescription: String? = nil) {
        self.qosName = qosName.trimmedOrEmpty
        self.freeGPUCount = freeGPUCount
        self.totalGPUCount = totalGPUCount
        self.sourceDescription = sourceDescription?.trimmedOrEmpty.nilIfEmpty
    }

    public var summaryText: String {
        "\(qosName) \(freeGPUCount)/\(totalGPUCount)"
    }
}

public struct ClusterLoadSnapshot: Hashable, Sendable {
    public var level: ClusterLoadLevel
    public var jobCount: Int?
    public var pendingJobCount: Int?
    public var scopedFreeGPUCount: Int?
    public var scopedTotalGPUCount: Int?
    public var scopedGPUDescription: String?
    public var scopedFreeNodeCount: Int?
    public var scopedTotalNodeCount: Int?
    public var scopedNodeDescription: String?
    public var qosGPUAvailabilities: [ClusterQoSGPUAvailability]
    public var freeCPUCount: Int?
    public var totalCPUCount: Int?
    public var freeGPUCount: Int?
    public var totalGPUCount: Int?
    public var freeNodeCount: Int?
    public var totalNodeCount: Int?
    public var jobHeadroom: Int?
    public var accessiblePartitions: [String]
    public var lastUpdatedAt: Date?
    public var message: String?

    public init(
        level: ClusterLoadLevel,
        jobCount: Int? = nil,
        pendingJobCount: Int? = nil,
        scopedFreeGPUCount: Int? = nil,
        scopedTotalGPUCount: Int? = nil,
        scopedGPUDescription: String? = nil,
        scopedFreeNodeCount: Int? = nil,
        scopedTotalNodeCount: Int? = nil,
        scopedNodeDescription: String? = nil,
        qosGPUAvailabilities: [ClusterQoSGPUAvailability] = [],
        freeCPUCount: Int? = nil,
        totalCPUCount: Int? = nil,
        freeGPUCount: Int? = nil,
        totalGPUCount: Int? = nil,
        freeNodeCount: Int? = nil,
        totalNodeCount: Int? = nil,
        jobHeadroom: Int? = nil,
        accessiblePartitions: [String] = [],
        lastUpdatedAt: Date? = nil,
        message: String? = nil
    ) {
        self.level = level
        self.jobCount = jobCount
        self.pendingJobCount = pendingJobCount
        self.scopedFreeGPUCount = scopedFreeGPUCount
        self.scopedTotalGPUCount = scopedTotalGPUCount
        self.scopedGPUDescription = scopedGPUDescription?.trimmedOrEmpty.nilIfEmpty
        self.scopedFreeNodeCount = scopedFreeNodeCount
        self.scopedTotalNodeCount = scopedTotalNodeCount
        self.scopedNodeDescription = scopedNodeDescription?.trimmedOrEmpty.nilIfEmpty
        self.qosGPUAvailabilities = qosGPUAvailabilities
        self.freeCPUCount = freeCPUCount
        self.totalCPUCount = totalCPUCount
        self.freeGPUCount = freeGPUCount
        self.totalGPUCount = totalGPUCount
        self.freeNodeCount = freeNodeCount
        self.totalNodeCount = totalNodeCount
        self.jobHeadroom = jobHeadroom
        self.accessiblePartitions = accessiblePartitions
        self.lastUpdatedAt = lastUpdatedAt
        self.message = message?.trimmedOrEmpty.nilIfEmpty
    }

    public static func unknown(message: String? = nil, lastUpdatedAt: Date? = nil) -> ClusterLoadSnapshot {
        ClusterLoadSnapshot(level: .unknown, lastUpdatedAt: lastUpdatedAt, message: message)
    }

    public var primaryFreeResourceText: String? {
        if let scopedTotalGPUCount, scopedTotalGPUCount > 0, let scopedFreeGPUCount {
            return "Free \(scopedFreeGPUCount) GPU"
        }
        if let scopedTotalNodeCount, scopedTotalNodeCount > 0, let scopedFreeNodeCount {
            return "Free \(scopedFreeNodeCount) \(scopedFreeNodeCount == 1 ? "node" : "nodes")"
        }
        if let totalGPUCount, totalGPUCount > 0, let freeGPUCount {
            return "Cluster free \(freeGPUCount) GPU"
        }
        if let totalCPUCount, totalCPUCount > 0, let freeCPUCount {
            return "Cluster free \(freeCPUCount) CPU"
        }
        if let totalNodeCount, totalNodeCount > 0, let freeNodeCount {
            return "Cluster free \(freeNodeCount) \(freeNodeCount == 1 ? "node" : "nodes")"
        }
        return nil
    }

    public var detailResourceText: String? {
        var parts: [String] = []
        if let freeGPUCount, let totalGPUCount, totalGPUCount > 0 {
            parts.append("Cluster GPU \(freeGPUCount)/\(totalGPUCount) free")
        }
        if let freeCPUCount, let totalCPUCount, totalCPUCount > 0 {
            parts.append("CPU \(freeCPUCount)/\(totalCPUCount) free")
        }
        if let freeNodeCount, let totalNodeCount, totalNodeCount > 0 {
            parts.append("Nodes \(freeNodeCount)/\(totalNodeCount) free")
        }
        return parts.isEmpty ? nil : parts.joined(separator: " • ")
    }

    public var scopedDetailText: String? {
        var parts: [String] = []

        if let scopedFreeGPUCount,
           let scopedTotalGPUCount,
           scopedTotalGPUCount > 0,
           let scopedGPUDescription,
           !scopedGPUDescription.isEmpty,
           scopedGPUDescription != "All QoS" {
            parts.append("\(scopedGPUDescription) GPU \(scopedFreeGPUCount)/\(scopedTotalGPUCount) free")
        }

        if let scopedFreeNodeCount,
           let scopedTotalNodeCount,
           scopedTotalNodeCount > 0,
           let scopedNodeDescription,
           !scopedNodeDescription.isEmpty {
            parts.append("\(scopedNodeDescription) nodes \(scopedFreeNodeCount)/\(scopedTotalNodeCount) free")
        }

        return parts.isEmpty ? nil : parts.joined(separator: " • ")
    }

    public var qosSummaryText: String? {
        guard !qosGPUAvailabilities.isEmpty else { return nil }
        return "QoS " + qosGPUAvailabilities.map(\.summaryText).joined(separator: " • ")
    }

    public var summaryText: String {
        var parts: [String] = []

        if let jobCount {
            parts.append("Jobs \(jobCount)")
        } else {
            parts.append("Jobs unknown")
        }

        if let primaryFreeResourceText {
            parts.append(primaryFreeResourceText)
        }

        if let jobHeadroom {
            parts.append("Headroom \(jobHeadroom) jobs")
        }

        if parts.count == 1, let message, !message.isEmpty {
            return message
        }

        return parts.joined(separator: " • ")
    }
}

public struct JobLogTailSession: Hashable, Identifiable, Sendable {
    public var clusterID: ClusterID
    public var clusterName: String
    public var jobID: String
    public var jobName: String
    public var paths: JobLogPaths
    public var preferredStream: JobLogStream

    public init(
        clusterID: ClusterID,
        clusterName: String,
        jobID: String,
        jobName: String,
        paths: JobLogPaths,
        preferredStream: JobLogStream
    ) {
        self.clusterID = clusterID
        self.clusterName = clusterName
        self.jobID = jobID
        self.jobName = jobName
        self.paths = paths
        self.preferredStream = preferredStream
    }

    public var id: String {
        "\(clusterID.rawValue):\(jobID):\(preferredStream.rawValue)"
    }

    public func path(for stream: JobLogStream) -> String? {
        switch stream {
        case .stdout:
            return paths.stdoutPath
        case .stderr:
            return paths.stderrPath
        }
    }

    public var availableStreams: [JobLogStream] {
        JobLogStream.allCases.filter { path(for: $0) != nil }
    }
}

public struct JobSnapshot: Codable, Hashable, Sendable {
    public var jobID: String
    public var owner: String
    public var state: NormalizedJobState
    public var rawState: String
    public var jobName: String
    public var submitTime: Date?
    public var startTime: Date?
    public var endTime: Date?
    public var elapsedSeconds: TimeInterval?
    public var pendingReason: String?
    public var dependencyExpression: String?
    public var dependencyJobIDs: [String]
    public var dependencyIsActive: Bool

    public init(
        jobID: String,
        owner: String,
        state: NormalizedJobState,
        rawState: String,
        jobName: String,
        submitTime: Date? = nil,
        startTime: Date? = nil,
        endTime: Date? = nil,
        elapsedSeconds: TimeInterval? = nil,
        pendingReason: String? = nil,
        dependencyExpression: String? = nil,
        dependencyJobIDs: [String] = [],
        dependencyIsActive: Bool = false
    ) {
        self.jobID = jobID
        self.owner = owner
        self.state = state
        self.rawState = rawState
        self.jobName = jobName
        self.submitTime = submitTime
        self.startTime = startTime
        self.endTime = endTime
        self.elapsedSeconds = elapsedSeconds
        self.pendingReason = pendingReason?.trimmedOrEmpty.nilIfEmpty
        self.dependencyExpression = dependencyExpression?.trimmedOrEmpty.nilIfEmpty
        self.dependencyJobIDs = dependencyJobIDs
        self.dependencyIsActive = dependencyIsActive
    }
}

public struct CurrentJob: Identifiable, Codable, Hashable, Sendable {
    public let clusterID: ClusterID
    public var jobID: String
    public var jobName: String
    public var owner: String
    public var state: NormalizedJobState
    public var rawState: String
    public var submitTime: Date?
    public var startTime: Date?
    public var elapsedSeconds: TimeInterval?
    public var pendingReason: String?
    public var dependencyExpression: String?
    public var dependencyJobIDs: [String]
    public var dependencyIsActive: Bool
    public var qosName: String?
    public var gpuCount: Int?
    public var nodeCount: Int?

    public init(
        clusterID: ClusterID,
        jobID: String,
        jobName: String,
        owner: String,
        state: NormalizedJobState,
        rawState: String,
        submitTime: Date? = nil,
        startTime: Date? = nil,
        elapsedSeconds: TimeInterval? = nil,
        pendingReason: String? = nil,
        dependencyExpression: String? = nil,
        dependencyJobIDs: [String] = [],
        dependencyIsActive: Bool = false,
        qosName: String? = nil,
        gpuCount: Int? = nil,
        nodeCount: Int? = nil
    ) {
        self.clusterID = clusterID
        self.jobID = jobID
        self.jobName = jobName
        self.owner = owner
        self.state = state
        self.rawState = rawState
        self.submitTime = submitTime
        self.startTime = startTime
        self.elapsedSeconds = elapsedSeconds
        self.pendingReason = pendingReason?.trimmedOrEmpty.nilIfEmpty
        self.dependencyExpression = dependencyExpression?.trimmedOrEmpty.nilIfEmpty
        self.dependencyJobIDs = dependencyJobIDs
        self.dependencyIsActive = dependencyIsActive
        self.qosName = qosName?.trimmedOrEmpty.nilIfEmpty
        self.gpuCount = gpuCount
        self.nodeCount = nodeCount
    }

    public var id: String {
        "\(clusterID.rawValue):\(jobID)"
    }

    public var snapshot: JobSnapshot {
        JobSnapshot(
            jobID: jobID,
            owner: owner,
            state: state,
            rawState: rawState,
            jobName: jobName,
            submitTime: submitTime,
            startTime: startTime,
            endTime: nil,
            elapsedSeconds: elapsedSeconds,
            pendingReason: pendingReason,
            dependencyExpression: dependencyExpression,
            dependencyJobIDs: dependencyJobIDs,
            dependencyIsActive: dependencyIsActive
        )
    }

    public var dependencyStatus: JobDependencyStatus {
        JobDependencyStatus.derive(
            state: state,
            pendingReason: pendingReason,
            hasDependencyInfo: hasDependencies,
            dependencyIsActive: dependencyIsActive
        )
    }

    public var hasDependencies: Bool {
        !(dependencyExpression?.trimmedOrEmpty ?? "").isEmpty || !dependencyJobIDs.isEmpty
    }

    public func depends(on jobID: String) -> Bool {
        let candidate = JobIdentifier.baseID(for: jobID)
        return dependencyJobIDs.contains { JobIdentifier.baseID(for: $0) == candidate }
    }
}

public struct WatchedJob: Identifiable, Codable, Hashable, Sendable {
    public let clusterID: ClusterID
    public var jobID: String
    public var jobName: String
    public var owner: String
    public var state: NormalizedJobState
    public var rawState: String
    public var notificationSent: Bool
    public var submitTime: Date?
    public var startTime: Date?
    public var endTime: Date?
    public var elapsedSeconds: TimeInterval?
    public var pendingReason: String?
    public var dependencyExpression: String?
    public var dependencyJobIDs: [String]
    public var dependencyIsActive: Bool
    public var firstSeenAt: Date
    public var lastUpdatedAt: Date
    public var lastSuccessfulRefreshAt: Date?
    public var isStale: Bool

    public init(
        clusterID: ClusterID,
        jobID: String,
        jobName: String,
        owner: String,
        state: NormalizedJobState,
        rawState: String,
        notificationSent: Bool = false,
        submitTime: Date? = nil,
        startTime: Date? = nil,
        endTime: Date? = nil,
        elapsedSeconds: TimeInterval? = nil,
        pendingReason: String? = nil,
        dependencyExpression: String? = nil,
        dependencyJobIDs: [String] = [],
        dependencyIsActive: Bool = false,
        firstSeenAt: Date,
        lastUpdatedAt: Date,
        lastSuccessfulRefreshAt: Date?,
        isStale: Bool = false
    ) {
        self.clusterID = clusterID
        self.jobID = jobID
        self.jobName = jobName
        self.owner = owner
        self.state = state
        self.rawState = rawState
        self.notificationSent = notificationSent
        self.submitTime = submitTime
        self.startTime = startTime
        self.endTime = endTime
        self.elapsedSeconds = elapsedSeconds
        self.pendingReason = pendingReason?.trimmedOrEmpty.nilIfEmpty
        self.dependencyExpression = dependencyExpression?.trimmedOrEmpty.nilIfEmpty
        self.dependencyJobIDs = dependencyJobIDs
        self.dependencyIsActive = dependencyIsActive
        self.firstSeenAt = firstSeenAt
        self.lastUpdatedAt = lastUpdatedAt
        self.lastSuccessfulRefreshAt = lastSuccessfulRefreshAt
        self.isStale = isStale
    }

    public init(currentJob: CurrentJob, now: Date) {
        self.init(
            clusterID: currentJob.clusterID,
            jobID: currentJob.jobID,
            jobName: currentJob.jobName,
            owner: currentJob.owner,
            state: currentJob.state,
            rawState: currentJob.rawState,
            submitTime: currentJob.submitTime,
            startTime: currentJob.startTime,
            endTime: nil,
            elapsedSeconds: currentJob.elapsedSeconds,
            pendingReason: currentJob.pendingReason,
            dependencyExpression: currentJob.dependencyExpression,
            dependencyJobIDs: currentJob.dependencyJobIDs,
            dependencyIsActive: currentJob.dependencyIsActive,
            firstSeenAt: now,
            lastUpdatedAt: now,
            lastSuccessfulRefreshAt: now
        )
    }

    public var id: String {
        "\(clusterID.rawValue):\(jobID)"
    }

    public var isTerminal: Bool {
        state.isTerminal
    }

    public var dependencyStatus: JobDependencyStatus {
        JobDependencyStatus.derive(
            state: state,
            pendingReason: pendingReason,
            hasDependencyInfo: hasDependencies,
            dependencyIsActive: dependencyIsActive
        )
    }

    public var hasDependencies: Bool {
        !(dependencyExpression?.trimmedOrEmpty ?? "").isEmpty || !dependencyJobIDs.isEmpty
    }

    public mutating func apply(snapshot: JobSnapshot, refreshedAt: Date) {
        let nextSubmitTime = snapshot.submitTime ?? submitTime
        let nextStartTime = snapshot.startTime ?? startTime
        let nextEndTime = snapshot.endTime ?? endTime
        let nextElapsedSeconds = snapshot.elapsedSeconds ?? elapsedSeconds
        let nextPendingReason = snapshot.pendingReason
        let nextDependencyExpression = snapshot.dependencyExpression ?? dependencyExpression
        let nextDependencyJobIDs = snapshot.dependencyExpression == nil ? dependencyJobIDs : snapshot.dependencyJobIDs
        let nextDependencyIsActive = snapshot.dependencyIsActive

        let hasMeaningfulChange =
            jobName != snapshot.jobName
            || owner != snapshot.owner
            || state != snapshot.state
            || rawState != snapshot.rawState
            || submitTime != nextSubmitTime
            || startTime != nextStartTime
            || endTime != nextEndTime
            || pendingReason != nextPendingReason
            || dependencyExpression != nextDependencyExpression
            || dependencyJobIDs != nextDependencyJobIDs
            || dependencyIsActive != nextDependencyIsActive
            || isStale

        jobName = snapshot.jobName
        owner = snapshot.owner
        state = snapshot.state
        rawState = snapshot.rawState
        submitTime = nextSubmitTime
        startTime = nextStartTime
        endTime = nextEndTime
        elapsedSeconds = nextElapsedSeconds
        pendingReason = nextPendingReason
        dependencyExpression = nextDependencyExpression
        dependencyJobIDs = nextDependencyJobIDs
        dependencyIsActive = nextDependencyIsActive

        if hasMeaningfulChange {
            lastUpdatedAt = refreshedAt
        }
        lastSuccessfulRefreshAt = refreshedAt
        isStale = false
    }

    public mutating func markStale() {
        isStale = true
    }

    public mutating func markMissingFromScheduler(at refreshedAt: Date) {
        let hasMeaningfulChange =
            state != .unknown
            || rawState != "NOT_IN_QUEUE"
            || pendingReason != nil
            || dependencyIsActive
            || isStale

        state = .unknown
        rawState = "NOT_IN_QUEUE"
        pendingReason = nil
        dependencyIsActive = false

        if hasMeaningfulChange {
            lastUpdatedAt = refreshedAt
        }
        lastSuccessfulRefreshAt = refreshedAt
        isStale = false
    }

    public mutating func markRefreshed(at refreshedAt: Date) {
        lastSuccessfulRefreshAt = refreshedAt
        isStale = false
    }

    public func waitingDuration(at date: Date) -> TimeInterval? {
        guard let submitTime else { return nil }
        if let startTime {
            return max(0, startTime.timeIntervalSince(submitTime))
        }
        if state == .pending {
            return max(0, date.timeIntervalSince(submitTime))
        }
        return nil
    }

    public func runningDuration(at date: Date) -> TimeInterval? {
        if let startTime {
            if let endTime {
                return max(0, endTime.timeIntervalSince(startTime))
            }
            if state.isTerminal, let elapsedSeconds {
                return elapsedSeconds
            }
            return max(0, date.timeIntervalSince(startTime))
        }
        return elapsedSeconds
    }

    public func depends(on jobID: String) -> Bool {
        let candidate = JobIdentifier.baseID(for: jobID)
        return dependencyJobIDs.contains { JobIdentifier.baseID(for: $0) == candidate }
    }
}

private extension String {
    var nilIfEmpty: String? {
        let trimmed = trimmedOrEmpty
        return trimmed.isEmpty ? nil : trimmed
    }
}
