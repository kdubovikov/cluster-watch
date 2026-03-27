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
        case .failed, .cancelled, .timeout, .outOfMemory, .nodeFail, .preempted:
            return "Failed"
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
        jobID.split(separator: ".").first.map(String.init) ?? jobID
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
        dependencyIsActive: Bool = false
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
