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
    public var activeLogTail: JobLogTailSession?
    public var activeLaunchCommand: JobLaunchSession?

    public private(set) var currentJobsByCluster: [ClusterID: [CurrentJob]]
    public private(set) var reachabilityByCluster: [ClusterID: ClusterReachabilityState]
    public private(set) var clusterLoadByCluster: [ClusterID: ClusterLoadSnapshot]
    public private(set) var logPathsByJobKey: [String: JobLogPaths] = [:]
    public private(set) var launchDetailsByJobKey: [String: JobLaunchDetails] = [:]

    @ObservationIgnored private let persistence: any PersistenceStoring
    @ObservationIgnored private let slurmClient: any SlurmClientProtocol
    @ObservationIgnored private let notificationManager: any NotificationManaging
    @ObservationIgnored private let pollingCoordinator: PollingCoordinator
    @ObservationIgnored private let nowProvider: @Sendable () -> Date
    @ObservationIgnored private var started = false
    @ObservationIgnored private var bootstrapped = false
    @ObservationIgnored private var refreshingClusterIDs: Set<ClusterID> = []
    @ObservationIgnored private var fetchingLogPathJobKeys: Set<String> = []
    @ObservationIgnored private var fetchingLaunchDetailsJobKeys: Set<String> = []

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

        let normalizedClusters = Self.normalizedClusters(from: initialState.clusters)
        let normalizedReachability = Self.normalizedReachability(
            from: initialState.reachabilityByCluster,
            clusterIDs: normalizedClusters.map(\.id)
        )

        self.clusters = normalizedClusters
        self.globalUsernameFilter = initialState.globalUsernameFilter.trimmedOrEmpty
        self.pollIntervalSeconds = max(5, initialState.pollIntervalSeconds)
        self.watchedJobs = initialState.watchedJobs
        self.currentJobsByCluster = Self.emptyCurrentJobs(for: normalizedClusters)
        self.reachabilityByCluster = normalizedReachability
        self.clusterLoadByCluster = Self.emptyClusterLoad(for: normalizedClusters)
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
        currentJobsByCluster = Self.emptyCurrentJobs(for: clusters)
        reachabilityByCluster = Self.normalizedReachability(
            from: persisted.reachabilityByCluster,
            clusterIDs: clusters.map(\.id)
        )
        clusterLoadByCluster = Self.emptyClusterLoad(for: clusters)
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
        self.currentJobsByCluster = Self.normalizedCurrentJobs(from: currentJobsByCluster, clusterIDs: self.clusters.map(\.id))
        self.reachabilityByCluster = Self.normalizedReachability(
            from: Dictionary(uniqueKeysWithValues: reachabilityByCluster.map { ($0.key.rawValue, $0.value) }),
            clusterIDs: self.clusters.map(\.id)
        )
        self.clusterLoadByCluster = Self.normalizedClusterLoad(
            from: clusterLoadByCluster,
            clusterIDs: self.clusters.map(\.id)
        )
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

    public func watch(jobs: [CurrentJob]) {
        let now = nowProvider()

        for job in jobs.reversed() {
            if let index = watchedJobs.firstIndex(where: { $0.clusterID == job.clusterID && $0.jobID == job.jobID }) {
                let originalNotificationState = watchedJobs[index].notificationSent
                let originalFirstSeenAt = watchedJobs[index].firstSeenAt
                watchedJobs[index].apply(snapshot: job.snapshot, refreshedAt: now)
                watchedJobs[index].notificationSent = originalNotificationState
                watchedJobs[index].firstSeenAt = originalFirstSeenAt
            } else {
                watchedJobs.insert(WatchedJob(currentJob: job, now: now), at: 0)
            }
        }

        persistAsync()
    }

    public func unwatch(job: WatchedJob) {
        watchedJobs.removeAll { $0.id == job.id }
        persistAsync()
    }

    public func unwatch(jobs: [WatchedJob]) {
        let jobIDs = Set(jobs.map(\.id))
        watchedJobs.removeAll { jobIDs.contains($0.id) }
        persistAsync()
    }

    public func clearCompleted() {
        watchedJobs.removeAll(where: { $0.isTerminal })
        persistAsync()
    }

    public func isWatched(_ currentJob: CurrentJob) -> Bool {
        watchedJobs.contains { $0.clusterID == currentJob.clusterID && $0.jobID == currentJob.jobID }
    }

    public func logPaths(for job: CurrentJob) -> JobLogPaths? {
        logPathsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func logPaths(for job: WatchedJob) -> JobLogPaths? {
        logPathsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func launchDetails(for job: CurrentJob) -> JobLaunchDetails? {
        launchDetailsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func launchDetails(for job: WatchedJob) -> JobLaunchDetails? {
        launchDetailsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func prefetchLogPaths(for job: CurrentJob) async {
        guard job.state != .pending else { return }
        await prefetchLogPaths(clusterID: job.clusterID, jobID: job.jobID)
    }

    public func prefetchLogPaths(for job: WatchedJob) async {
        guard job.state != .pending else { return }
        await prefetchLogPaths(clusterID: job.clusterID, jobID: job.jobID)
    }

    public func prepareLogTail(for job: CurrentJob) async -> Bool {
        guard job.state != .pending else { return false }
        await prefetchLogPaths(for: job)
        guard let paths = logPaths(for: job),
              let preferredStream = preferredLogStream(for: paths) else {
            return false
        }

        activeLogTail = JobLogTailSession(
            clusterID: job.clusterID,
            clusterName: clusterName(for: job.clusterID),
            jobID: job.jobID,
            jobName: job.jobName,
            paths: paths,
            preferredStream: preferredStream
        )
        return true
    }

    public func prepareLogTail(for job: WatchedJob) async -> Bool {
        guard job.state != .pending else { return false }
        await prefetchLogPaths(for: job)
        guard let paths = logPaths(for: job),
              let preferredStream = preferredLogStream(for: paths) else {
            return false
        }

        activeLogTail = JobLogTailSession(
            clusterID: job.clusterID,
            clusterName: clusterName(for: job.clusterID),
            jobID: job.jobID,
            jobName: job.jobName,
            paths: paths,
            preferredStream: preferredStream
        )
        return true
    }

    public func prepareLaunchCommand(for job: CurrentJob) async -> Bool {
        let details = await fetchLaunchDetails(clusterID: job.clusterID, jobID: job.jobID) ?? JobLaunchDetails()

        activeLaunchCommand = JobLaunchSession(
            clusterID: job.clusterID,
            clusterName: clusterName(for: job.clusterID),
            jobID: job.jobID,
            jobName: job.jobName,
            details: details,
            preferredMode: details.preferredMode
        )
        return true
    }

    public func prepareLaunchCommand(for job: WatchedJob) async -> Bool {
        let details = await fetchLaunchDetails(clusterID: job.clusterID, jobID: job.jobID) ?? JobLaunchDetails()

        activeLaunchCommand = JobLaunchSession(
            clusterID: job.clusterID,
            clusterName: clusterName(for: job.clusterID),
            jobID: job.jobID,
            jobName: job.jobName,
            details: details,
            preferredMode: details.preferredMode
        )
        return true
    }

    public func closeActiveLogTail() {
        activeLogTail = nil
    }

    public func closeActiveLaunchCommand() {
        activeLaunchCommand = nil
    }

    public func tailLog(
        session: JobLogTailSession,
        stream: JobLogStream,
        lineCount: Int = 200
    ) async throws -> String {
        guard let cluster = clusters.first(where: { $0.id == session.clusterID }) else {
            throw SlurmClientError.invalidConfiguration("Cluster configuration not found.")
        }
        guard let path = session.path(for: stream) else {
            throw SlurmClientError.invalidConfiguration("No \(stream.title.lowercased()) path available.")
        }

        return try await slurmClient.tailLog(for: cluster, remotePath: path, lineCount: lineCount)
    }

    public func clusterName(for clusterID: ClusterID) -> String {
        if let configured = clusters.first(where: { $0.id == clusterID })?.displayName.trimmedOrEmpty,
           !configured.isEmpty {
            return configured
        }
        return "Unknown Cluster"
    }

    public func hasWatchedJobs(for clusterID: ClusterID) -> Bool {
        watchedJobs.contains { $0.clusterID == clusterID }
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

        await withTaskGroup(of: Void.self) { group in
            for clusterID in enabledClusterIDs {
                group.addTask { [weak self] in
                    await self?.refreshCluster(id: clusterID)
                }
            }
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

            await refreshClusterLoad(for: cluster, username: username, currentJobs: currentJobs, refreshedAt: refreshedAt)
            await reconcileWatchedJobs(for: cluster, currentJobs: currentJobs, refreshedAt: refreshedAt)
        } catch let error as SlurmClientError {
            switch error {
            case .queryFailed(let message):
                currentJobsByCluster[id] = []
                state.status = .reachable
                state.lastErrorMessage = message
                reachabilityByCluster[id] = state
                clusterLoadByCluster[id] = ClusterLoadSnapshot.unknown(
                    message: message,
                    lastUpdatedAt: clusterLoadByCluster[id]?.lastUpdatedAt
                )
            case .invalidConfiguration, .commandFailed:
                currentJobsByCluster[id] = []

                state.status = .unreachable
                state.lastErrorMessage = error.localizedDescription
                reachabilityByCluster[id] = state
                clusterLoadByCluster[id] = ClusterLoadSnapshot.unknown(
                    message: error.localizedDescription,
                    lastUpdatedAt: clusterLoadByCluster[id]?.lastUpdatedAt
                )

                markClusterJobsStale(clusterID: id)
            }
        } catch {
            currentJobsByCluster[id] = []

            state.status = .unreachable
            state.lastErrorMessage = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            reachabilityByCluster[id] = state
            clusterLoadByCluster[id] = ClusterLoadSnapshot.unknown(
                message: (error as? LocalizedError)?.errorDescription ?? error.localizedDescription,
                lastUpdatedAt: clusterLoadByCluster[id]?.lastUpdatedAt
            )

            markClusterJobsStale(clusterID: id)
        }
    }

    public func reachability(for clusterID: ClusterID) -> ClusterReachabilityState {
        reachabilityByCluster[clusterID] ?? ClusterReachabilityState()
    }

    public func clusterLoad(for clusterID: ClusterID) -> ClusterLoadSnapshot {
        clusterLoadByCluster[clusterID] ?? ClusterLoadSnapshot.unknown()
    }

    private func prefetchLogPaths(clusterID: ClusterID, jobID: String) async {
        let key = jobKey(clusterID: clusterID, jobID: jobID)
        guard logPathsByJobKey[key] == nil else { return }
        guard !fetchingLogPathJobKeys.contains(key) else { return }
        guard let cluster = clusters.first(where: { $0.id == clusterID }), cluster.isEnabled else { return }

        fetchingLogPathJobKeys.insert(key)
        defer { fetchingLogPathJobKeys.remove(key) }

        do {
            if let logPaths = try await slurmClient.fetchLogPaths(for: cluster, jobID: jobID),
               logPaths.hasAnyPath {
                logPathsByJobKey[key] = logPaths
            }
        } catch {
            return
        }
    }

    private func fetchLaunchDetails(clusterID: ClusterID, jobID: String) async -> JobLaunchDetails? {
        let key = jobKey(clusterID: clusterID, jobID: jobID)
        if let cached = launchDetailsByJobKey[key] {
            return cached
        }

        guard !fetchingLaunchDetailsJobKeys.contains(key) else { return nil }
        guard let cluster = clusters.first(where: { $0.id == clusterID }), cluster.isEnabled else { return nil }

        fetchingLaunchDetailsJobKeys.insert(key)
        defer { fetchingLaunchDetailsJobKeys.remove(key) }

        do {
            guard let details = try await slurmClient.fetchLaunchDetails(for: cluster, jobID: jobID),
                  details.hasAnyContent else {
                return nil
            }

            launchDetailsByJobKey[key] = details
            return details
        } catch {
            return nil
        }
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
        var updatedJobs = watchedJobs.filter { $0.clusterID == cluster.id }
        var notifications: [WatchedJob] = []

        for index in updatedJobs.indices {
            updatedJobs[index].isStale = false

            if let currentJob = currentByID[updatedJobs[index].jobID] {
                if updatedJobs[index].isTerminal, !currentJob.state.isTerminal {
                    updatedJobs[index].markRefreshed(at: refreshedAt)
                    continue
                }

                let wasTerminal = updatedJobs[index].isTerminal
                updatedJobs[index].apply(snapshot: currentJob.snapshot, refreshedAt: refreshedAt)
                markNotificationIfNeeded(job: &updatedJobs[index], wasTerminal: wasTerminal, notifications: &notifications)
                continue
            }

            do {
                if let historical = try await slurmClient.fetchHistoricalJob(for: cluster, jobID: updatedJobs[index].jobID) {
                    if updatedJobs[index].isTerminal, !historical.state.isTerminal {
                        updatedJobs[index].markRefreshed(at: refreshedAt)
                        continue
                    }

                    let wasTerminal = updatedJobs[index].isTerminal
                    updatedJobs[index].apply(snapshot: historical, refreshedAt: refreshedAt)
                    markNotificationIfNeeded(job: &updatedJobs[index], wasTerminal: wasTerminal, notifications: &notifications)
                }
            } catch {
                continue
            }
        }

        mergeWatchedJobs(updatedJobs, for: cluster.id)

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
        var updatedJobs = watchedJobs.filter { $0.clusterID == clusterID }
        for index in updatedJobs.indices {
            updatedJobs[index].markStale()
        }
        mergeWatchedJobs(updatedJobs, for: clusterID)
    }

    private func mergeWatchedJobs(_ updatedJobs: [WatchedJob], for clusterID: ClusterID) {
        let updatedJobsByID = Dictionary(uniqueKeysWithValues: updatedJobs.map { ($0.id, $0) })
        watchedJobs = watchedJobs.map { existingJob in
            guard existingJob.clusterID == clusterID else { return existingJob }
            return updatedJobsByID[existingJob.id] ?? existingJob
        }
    }

    private func preferredLogStream(for logPaths: JobLogPaths) -> JobLogStream? {
        if logPaths.stdoutPath != nil { return .stdout }
        if logPaths.stderrPath != nil { return .stderr }
        return nil
    }

    private func refreshClusterLoad(
        for cluster: ClusterConfig,
        username: String,
        currentJobs: [CurrentJob],
        refreshedAt: Date
    ) async {
        do {
            clusterLoadByCluster[cluster.id] = try await slurmClient.fetchClusterLoad(
                for: cluster,
                username: username,
                currentJobs: currentJobs
            )
        } catch {
            clusterLoadByCluster[cluster.id] = ClusterLoadSnapshot.unknown(
                message: (error as? LocalizedError)?.errorDescription ?? error.localizedDescription,
                lastUpdatedAt: refreshedAt
            )
        }
    }

    private func jobKey(clusterID: ClusterID, jobID: String) -> String {
        "\(clusterID.rawValue):\(jobID)"
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
        var seenIDs = Set<ClusterID>()
        var result: [ClusterConfig] = []

        for (index, cluster) in persistedClusters.enumerated() {
            var normalized = cluster
            let displayName = normalized.displayName.trimmedOrEmpty
            normalized.displayName = displayName.isEmpty ? "Cluster \(index + 1)" : displayName

            while seenIDs.contains(normalized.id) {
                normalized = normalized.withID(.new())
            }

            seenIDs.insert(normalized.id)
            result.append(normalized)
        }

        return result
    }

    private static func emptyCurrentJobs(for clusters: [ClusterConfig]) -> [ClusterID: [CurrentJob]] {
        Dictionary(uniqueKeysWithValues: clusters.map { ($0.id, []) })
    }

    private static func emptyClusterLoad(for clusters: [ClusterConfig]) -> [ClusterID: ClusterLoadSnapshot] {
        Dictionary(uniqueKeysWithValues: clusters.map { ($0.id, ClusterLoadSnapshot.unknown()) })
    }

    private static func normalizedCurrentJobs(from existingJobs: [ClusterID: [CurrentJob]], clusterIDs: [ClusterID]) -> [ClusterID: [CurrentJob]] {
        Dictionary(uniqueKeysWithValues: clusterIDs.map { ($0, existingJobs[$0] ?? []) })
    }

    private static func normalizedClusterLoad(
        from existingLoad: [ClusterID: ClusterLoadSnapshot],
        clusterIDs: [ClusterID]
    ) -> [ClusterID: ClusterLoadSnapshot] {
        Dictionary(uniqueKeysWithValues: clusterIDs.map { ($0, existingLoad[$0] ?? ClusterLoadSnapshot.unknown()) })
    }

    private static func normalizedReachability(
        from persistedReachability: [String: ClusterReachabilityState],
        clusterIDs: [ClusterID]
    ) -> [ClusterID: ClusterReachabilityState] {
        Dictionary(uniqueKeysWithValues: clusterIDs.map { clusterID in
            (clusterID, persistedReachability[clusterID.rawValue] ?? ClusterReachabilityState())
        })
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
