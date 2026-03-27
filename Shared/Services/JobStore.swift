import Foundation
import Observation

@MainActor
@Observable
public final class JobStore {
    public var clusters: [ClusterConfig]
    public var globalUsernameFilter: String
    public var pollIntervalSeconds: Double
    public var watchedJobs: [WatchedJob]
    public var browseSearchText: String = ""

    public private(set) var currentJobsByCluster: [ClusterID: [CurrentJob]]
    public private(set) var reachabilityByCluster: [ClusterID: ClusterReachabilityState]

    @ObservationIgnored private let persistence: any PersistenceStoring
    @ObservationIgnored private let slurmClient: any SlurmClientProtocol
    @ObservationIgnored private let notificationManager: any NotificationManaging
    @ObservationIgnored private let pollingCoordinator: PollingCoordinator
    @ObservationIgnored private let nowProvider: @Sendable () -> Date
    @ObservationIgnored private var started = false
    @ObservationIgnored private var bootstrapped = false
    @ObservationIgnored private var refreshingClusterIDs: Set<ClusterID> = []

    public init(
        persistence: any PersistenceStoring,
        slurmClient: any SlurmClientProtocol,
        notificationManager: any NotificationManaging,
        pollingCoordinator: PollingCoordinator,
        nowProvider: @escaping @Sendable () -> Date = { Date() },
        initialState: PersistedAppState = PersistedAppState.defaultState()
    ) {
        self.persistence = persistence
        self.slurmClient = slurmClient
        self.notificationManager = notificationManager
        self.pollingCoordinator = pollingCoordinator
        self.nowProvider = nowProvider

        self.clusters = Self.normalizedClusters(from: initialState.clusters)
        self.globalUsernameFilter = initialState.globalUsernameFilter.trimmedOrEmpty
        self.pollIntervalSeconds = max(5, initialState.pollIntervalSeconds)
        self.watchedJobs = initialState.watchedJobs
        self.currentJobsByCluster = Dictionary(uniqueKeysWithValues: ClusterID.allCases.map { ($0, []) })
        self.reachabilityByCluster = Self.normalizedReachability(from: initialState.reachabilityByCluster)
    }

    public convenience init(initialState: PersistedAppState = PersistedAppState.defaultState()) {
        self.init(
            persistence: PersistenceStore(),
            slurmClient: SlurmClient(),
            notificationManager: NotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            initialState: initialState
        )
    }

    public var groupedWatchedJobs: [GroupedJobsViewModel.Section] {
        GroupedJobsViewModel.sections(for: watchedJobs, referenceDate: nowProvider())
    }

    public var visibleCurrentJobs: [CurrentJob] {
        let query = browseSearchText.trimmedOrEmpty.lowercased()

        return clusters
            .filter(\.isEnabled)
            .filter { reachabilityByCluster[$0.id]?.status != .unreachable }
            .flatMap { currentJobsByCluster[$0.id] ?? [] }
            .filter { job in
                !isWatched(job)
            }
            .filter { job in
                guard !query.isEmpty else { return true }
                return job.jobID.lowercased().contains(query)
                    || job.jobName.lowercased().contains(query)
                    || clusterName(for: job.clusterID).lowercased().contains(query)
                    || (job.pendingReason?.lowercased().contains(query) ?? false)
                    || (job.dependencyExpression?.lowercased().contains(query) ?? false)
            }
            .sorted(by: compareCurrentJobs)
    }

    public func loadPersistedState() async {
        guard let persisted = await persistence.load() else {
            persistAsync()
            return
        }

        clusters = Self.normalizedClusters(from: persisted.clusters)
        globalUsernameFilter = persisted.globalUsernameFilter.trimmedOrEmpty.isEmpty ? NSUserName() : persisted.globalUsernameFilter.trimmedOrEmpty
        pollIntervalSeconds = max(5, persisted.pollIntervalSeconds)
        watchedJobs = persisted.watchedJobs
        reachabilityByCluster = Self.normalizedReachability(from: persisted.reachabilityByCluster)
    }

    public func start() {
        guard !started else { return }
        started = true

        Task {
            await notificationManager.requestAuthorizationIfNeeded()
        }

        configurePolling()

        Task {
            await refreshAll()
        }
    }

    public func bootstrap() async {
        guard !bootstrapped else { return }
        bootstrapped = true
        await loadPersistedState()
        start()
    }

    public func applySettings(clusters: [ClusterConfig], globalUsernameFilter: String, pollIntervalSeconds: Double) {
        self.clusters = Self.normalizedClusters(from: clusters)
        self.globalUsernameFilter = globalUsernameFilter.trimmedOrEmpty.isEmpty ? NSUserName() : globalUsernameFilter.trimmedOrEmpty
        self.pollIntervalSeconds = max(5, pollIntervalSeconds)
        configurePolling()
        persistAsync()

        Task {
            await refreshAll()
        }
    }

    public func watch(job: CurrentJob) {
        let now = nowProvider()

        if let index = watchedJobs.firstIndex(where: { $0.clusterID == job.clusterID && $0.jobID == job.jobID }) {
            let originalNotificationState = watchedJobs[index].notificationSent
            let originalFirstSeenAt = watchedJobs[index].firstSeenAt
            watchedJobs[index].apply(snapshot: job.snapshot, refreshedAt: now)
            watchedJobs[index].notificationSent = originalNotificationState
            watchedJobs[index].firstSeenAt = originalFirstSeenAt
        } else {
            watchedJobs.insert(WatchedJob(currentJob: job, now: now), at: 0)
        }

        persistAsync()
    }

    public func unwatch(job: WatchedJob) {
        watchedJobs.removeAll { $0.id == job.id }
        persistAsync()
    }

    public func clearCompleted() {
        watchedJobs.removeAll(where: { $0.isTerminal })
        persistAsync()
    }

    public func isWatched(_ currentJob: CurrentJob) -> Bool {
        watchedJobs.contains { $0.clusterID == currentJob.clusterID && $0.jobID == currentJob.jobID }
    }

    public func clusterName(for clusterID: ClusterID) -> String {
        clusters.first(where: { $0.id == clusterID })?.displayName ?? clusterID.defaultDisplayName
    }

    public func watchedDependencies(for job: WatchedJob) -> [WatchedJob] {
        watchedJobs
            .filter { $0.clusterID == job.clusterID && $0.id != job.id && job.depends(on: $0.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependents(for job: WatchedJob) -> [WatchedJob] {
        watchedJobs
            .filter { $0.clusterID == job.clusterID && $0.id != job.id && $0.depends(on: job.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependencies(for job: CurrentJob) -> [WatchedJob] {
        watchedJobs
            .filter { $0.clusterID == job.clusterID && job.depends(on: $0.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependents(for job: CurrentJob) -> [WatchedJob] {
        watchedJobs
            .filter { $0.clusterID == job.clusterID && $0.depends(on: job.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func refreshAll() async {
        let enabledClusterIDs = clusters.filter(\.isEnabled).map(\.id)

        switch enabledClusterIDs.count {
        case 0:
            return
        case 1:
            await refreshCluster(id: enabledClusterIDs[0])
        default:
            async let firstRefresh: Void = refreshCluster(id: enabledClusterIDs[0])
            async let secondRefresh: Void = refreshCluster(id: enabledClusterIDs[1])
            _ = await (firstRefresh, secondRefresh)
        }
    }

    public func refreshCluster(id: ClusterID) async {
        guard !refreshingClusterIDs.contains(id) else { return }
        guard let cluster = clusters.first(where: { $0.id == id }), cluster.isEnabled else { return }

        refreshingClusterIDs.insert(id)
        defer {
            refreshingClusterIDs.remove(id)
            persistAsync()
        }

        var state = reachabilityByCluster[id] ?? ClusterReachabilityState()
        state.status = .checking
        state.lastErrorMessage = nil
        reachabilityByCluster[id] = state

        do {
            let username = cluster.effectiveUsername(globalUsername: globalUsernameFilter)
            let currentJobs = try await slurmClient.fetchCurrentJobs(for: cluster, username: username)
            let refreshedAt = nowProvider()

            currentJobsByCluster[id] = currentJobs

            state.status = .reachable
            state.lastSuccessfulRefresh = refreshedAt
            state.lastErrorMessage = nil
            reachabilityByCluster[id] = state

            await reconcileWatchedJobs(for: cluster, currentJobs: currentJobs, refreshedAt: refreshedAt)
        } catch let error as SlurmClientError {
            switch error {
            case .queryFailed(let message):
                currentJobsByCluster[id] = []
                state.status = .reachable
                state.lastErrorMessage = message
                reachabilityByCluster[id] = state
            case .invalidConfiguration, .commandFailed:
                currentJobsByCluster[id] = []

                state.status = .unreachable
                state.lastErrorMessage = error.localizedDescription
                reachabilityByCluster[id] = state

                markClusterJobsStale(clusterID: id)
            }
        } catch {
            currentJobsByCluster[id] = []

            state.status = .unreachable
            state.lastErrorMessage = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            reachabilityByCluster[id] = state

            markClusterJobsStale(clusterID: id)
        }
    }

    public func reachability(for clusterID: ClusterID) -> ClusterReachabilityState {
        reachabilityByCluster[clusterID] ?? ClusterReachabilityState()
    }

    private func configurePolling() {
        pollingCoordinator.start(
            intervalProvider: { [weak self] in
                self?.pollIntervalSeconds ?? 30
            },
            refreshAction: { [weak self] in
                await self?.refreshAll()
            }
        )
    }

    private func reconcileWatchedJobs(for cluster: ClusterConfig, currentJobs: [CurrentJob], refreshedAt: Date) async {
        let currentByID = Dictionary(uniqueKeysWithValues: currentJobs.map { ($0.jobID, $0) })
        var updatedJobs = watchedJobs
        var notifications: [WatchedJob] = []

        for index in updatedJobs.indices where updatedJobs[index].clusterID == cluster.id {
            updatedJobs[index].isStale = false

            if let currentJob = currentByID[updatedJobs[index].jobID] {
                let wasTerminal = updatedJobs[index].isTerminal
                updatedJobs[index].apply(snapshot: currentJob.snapshot, refreshedAt: refreshedAt)
                markNotificationIfNeeded(job: &updatedJobs[index], wasTerminal: wasTerminal, notifications: &notifications)
                continue
            }

            do {
                if let historical = try await slurmClient.fetchHistoricalJob(for: cluster, jobID: updatedJobs[index].jobID) {
                    let wasTerminal = updatedJobs[index].isTerminal
                    updatedJobs[index].apply(snapshot: historical, refreshedAt: refreshedAt)
                    markNotificationIfNeeded(job: &updatedJobs[index], wasTerminal: wasTerminal, notifications: &notifications)
                }
            } catch {
                continue
            }
        }

        watchedJobs = updatedJobs

        for job in notifications {
            await notificationManager.sendTerminalNotification(for: job, clusterName: cluster.displayName)
        }
    }

    private func markNotificationIfNeeded(job: inout WatchedJob, wasTerminal: Bool, notifications: inout [WatchedJob]) {
        guard !wasTerminal, job.isTerminal, !job.notificationSent else { return }
        job.notificationSent = true
        notifications.append(job)
    }

    private func markClusterJobsStale(clusterID: ClusterID) {
        var updatedJobs = watchedJobs
        for index in updatedJobs.indices where updatedJobs[index].clusterID == clusterID {
            updatedJobs[index].markStale()
        }
        watchedJobs = updatedJobs
    }

    private func persistAsync() {
        let snapshot = PersistedAppState(
            clusters: clusters,
            globalUsernameFilter: globalUsernameFilter,
            pollIntervalSeconds: pollIntervalSeconds,
            watchedJobs: watchedJobs,
            reachabilityByCluster: Dictionary(
                uniqueKeysWithValues: reachabilityByCluster.map { ($0.key.rawValue, $0.value) }
            )
        )

        Task {
            try? await persistence.save(snapshot)
        }
    }

    private static func normalizedClusters(from persistedClusters: [ClusterConfig]) -> [ClusterConfig] {
        let clusterLookup = Dictionary(uniqueKeysWithValues: persistedClusters.map { ($0.id, $0) })
        return ClusterID.allCases.map { clusterLookup[$0] ?? ClusterConfig.defaultValue(for: $0) }
    }

    private static func normalizedReachability(from persistedReachability: [String: ClusterReachabilityState]) -> [ClusterID: ClusterReachabilityState] {
        var result = Dictionary(uniqueKeysWithValues: ClusterID.allCases.map { ($0, ClusterReachabilityState()) })

        for (key, value) in persistedReachability {
            if let clusterID = ClusterID(rawValue: key) {
                result[clusterID] = value
            }
        }

        return result
    }

    private func compareCurrentJobs(lhs: CurrentJob, rhs: CurrentJob) -> Bool {
        if lhs.state.sortPriority != rhs.state.sortPriority {
            return lhs.state.sortPriority < rhs.state.sortPriority
        }

        let lhsDate = lhs.startTime ?? lhs.submitTime ?? .distantPast
        let rhsDate = rhs.startTime ?? rhs.submitTime ?? .distantPast

        if lhsDate != rhsDate {
            return lhsDate > rhsDate
        }

        return lhs.jobID > rhs.jobID
    }

    private func compareWatchedJobs(lhs: WatchedJob, rhs: WatchedJob) -> Bool {
        if lhs.state.sortPriority != rhs.state.sortPriority {
            return lhs.state.sortPriority < rhs.state.sortPriority
        }

        if lhs.lastUpdatedAt != rhs.lastUpdatedAt {
            return lhs.lastUpdatedAt > rhs.lastUpdatedAt
        }

        return lhs.jobID > rhs.jobID
    }
}
