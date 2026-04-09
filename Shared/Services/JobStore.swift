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
    public var isDemoDataEnabled: Bool

    public private(set) var currentJobsByCluster: [ClusterID: [CurrentJob]]
    public private(set) var reachabilityByCluster: [ClusterID: ClusterReachabilityState]
    public private(set) var clusterLoadByCluster: [ClusterID: ClusterLoadSnapshot]
    public private(set) var logPathsByJobKey: [String: JobLogPaths] = [:]
    public private(set) var launchDetailsByJobKey: [String: JobLaunchDetails] = [:]
    #if DEBUG
    private var demoState: DemoDataState?
    #endif

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
        self.isDemoDataEnabled = initialState.isDemoDataEnabled
        self.currentJobsByCluster = Self.emptyCurrentJobs(for: normalizedClusters)
        self.reachabilityByCluster = normalizedReachability
        self.clusterLoadByCluster = Self.emptyClusterLoad(for: normalizedClusters)
        #if DEBUG
        if self.isDemoDataEnabled {
            self.demoState = Self.makeDemoDataState(now: nowProvider())
        }
        #endif
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
        GroupedJobsViewModel.sections(for: effectiveWatchedJobs, referenceDate: nowProvider())
    }

    public var visibleCurrentJobs: [CurrentJob] {
        let query = browseSearchText.trimmedOrEmpty.lowercased()

        return displayClusters
            .filter(\.isEnabled)
            .filter { effectiveReachabilityByCluster[$0.id]?.status != .unreachable }
            .flatMap { effectiveCurrentJobsByCluster[$0.id] ?? [] }
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

    public var displayClusters: [ClusterConfig] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.clusters
        }
        #endif
        return clusters
    }

    public var hasTerminalDisplayedWatchedJobs: Bool {
        effectiveWatchedJobs.contains(where: \.isTerminal)
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
        isDemoDataEnabled = persisted.isDemoDataEnabled
        #if DEBUG
        demoState = isDemoDataEnabled ? Self.makeDemoDataState(now: nowProvider()) : nil
        #endif
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

    #if DEBUG
    public func applySettings(clusters: [ClusterConfig], globalUsernameFilter: String, pollIntervalSeconds: Double, isDemoDataEnabled: Bool) {
        applySettingsImpl(
            clusters: clusters,
            globalUsernameFilter: globalUsernameFilter,
            pollIntervalSeconds: pollIntervalSeconds,
            isDemoDataEnabled: isDemoDataEnabled
        )
    }
    #else
    public func applySettings(clusters: [ClusterConfig], globalUsernameFilter: String, pollIntervalSeconds: Double) {
        applySettingsImpl(
            clusters: clusters,
            globalUsernameFilter: globalUsernameFilter,
            pollIntervalSeconds: pollIntervalSeconds
        )
    }
    #endif

    private func applySettingsImpl(
        clusters: [ClusterConfig],
        globalUsernameFilter: String,
        pollIntervalSeconds: Double,
        isDemoDataEnabled: Bool = false
    ) {
        self.clusters = Self.normalizedClusters(from: clusters)
        self.globalUsernameFilter = globalUsernameFilter.trimmedOrEmpty.isEmpty ? NSUserName() : globalUsernameFilter.trimmedOrEmpty
        self.pollIntervalSeconds = max(5, pollIntervalSeconds)
        self.isDemoDataEnabled = isDemoDataEnabled
        #if DEBUG
        demoState = isDemoDataEnabled ? Self.makeDemoDataState(now: nowProvider()) : nil
        if isDemoDataEnabled {
            activeLogTail = nil
            activeLaunchCommand = nil
        }
        #endif
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
        #if DEBUG
        if isDemoDataEnabled {
            demoState?.watch(job: job, now: nowProvider())
            return
        }
        #endif
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
        #if DEBUG
        if isDemoDataEnabled {
            for job in jobs {
                demoState?.watch(job: job, now: nowProvider())
            }
            return
        }
        #endif
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
        #if DEBUG
        if isDemoDataEnabled {
            demoState?.unwatch(jobIDs: [job.id])
            return
        }
        #endif
        watchedJobs.removeAll { $0.id == job.id }
        persistAsync()
    }

    public func unwatch(jobs: [WatchedJob]) {
        #if DEBUG
        if isDemoDataEnabled {
            demoState?.unwatch(jobIDs: Set(jobs.map(\.id)))
            return
        }
        #endif
        let jobIDs = Set(jobs.map(\.id))
        watchedJobs.removeAll { jobIDs.contains($0.id) }
        persistAsync()
    }

    public func clearCompleted() {
        #if DEBUG
        if isDemoDataEnabled {
            demoState?.clearCompleted()
            return
        }
        #endif
        watchedJobs.removeAll(where: { $0.isTerminal })
        persistAsync()
    }

    public func cancel(job: CurrentJob) async -> Bool {
        guard !job.state.isTerminal else { return false }
        return await cancel(jobIDsByCluster: [job.clusterID: [job.jobID]])
    }

    public func cancel(job: WatchedJob) async -> Bool {
        guard !job.isTerminal, !job.isStale else { return false }
        return await cancel(jobIDsByCluster: [job.clusterID: [job.jobID]])
    }

    public func cancel(jobs: [CurrentJob]) async -> Bool {
        let jobIDsByCluster = Dictionary(grouping: jobs.filter { !$0.state.isTerminal }, by: \.clusterID)
            .mapValues { jobs in jobs.map(\.jobID) }
        return await cancel(jobIDsByCluster: jobIDsByCluster)
    }

    public func cancel(jobs: [WatchedJob]) async -> Bool {
        let jobIDsByCluster = Dictionary(grouping: jobs.filter { !$0.isTerminal && !$0.isStale }, by: \.clusterID)
            .mapValues { jobs in jobs.map(\.jobID) }
        return await cancel(jobIDsByCluster: jobIDsByCluster)
    }

    public func isWatched(_ currentJob: CurrentJob) -> Bool {
        effectiveWatchedJobs.contains { $0.clusterID == currentJob.clusterID && $0.jobID == currentJob.jobID }
    }

    public func logPaths(for job: CurrentJob) -> JobLogPaths? {
        effectiveLogPathsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func logPaths(for job: WatchedJob) -> JobLogPaths? {
        effectiveLogPathsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func launchDetails(for job: CurrentJob) -> JobLaunchDetails? {
        effectiveLaunchDetailsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func launchDetails(for job: WatchedJob) -> JobLaunchDetails? {
        effectiveLaunchDetailsByJobKey[jobKey(clusterID: job.clusterID, jobID: job.jobID)]
    }

    public func prefetchLogPaths(for job: CurrentJob) async {
        #if DEBUG
        if isDemoDataEnabled { return }
        #endif
        guard job.state != .pending else { return }
        await prefetchLogPaths(clusterID: job.clusterID, jobID: job.jobID)
    }

    public func prefetchLogPaths(for job: WatchedJob) async {
        #if DEBUG
        if isDemoDataEnabled { return }
        #endif
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
        #if DEBUG
        if isDemoDataEnabled {
            return demoState?.tailOutput(for: session, stream: stream, lineCount: lineCount) ?? ""
        }
        #endif
        guard let cluster = clusters.first(where: { $0.id == session.clusterID }) else {
            throw SlurmClientError.invalidConfiguration("Cluster configuration not found.")
        }
        guard let path = session.path(for: stream) else {
            throw SlurmClientError.invalidConfiguration("No \(stream.title.lowercased()) path available.")
        }

        return try await slurmClient.tailLog(for: cluster, remotePath: path, lineCount: lineCount)
    }

    public func clusterName(for clusterID: ClusterID) -> String {
        if let configured = displayClusters.first(where: { $0.id == clusterID })?.displayName.trimmedOrEmpty,
           !configured.isEmpty {
            return configured
        }
        return "Unknown Cluster"
    }

    public func hasWatchedJobs(for clusterID: ClusterID) -> Bool {
        watchedJobs.contains { $0.clusterID == clusterID }
    }

    public func watchedDependencies(for job: WatchedJob) -> [WatchedJob] {
        effectiveWatchedJobs
            .filter { $0.clusterID == job.clusterID && $0.id != job.id && job.depends(on: $0.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependents(for job: WatchedJob) -> [WatchedJob] {
        effectiveWatchedJobs
            .filter { $0.clusterID == job.clusterID && $0.id != job.id && $0.depends(on: job.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependencies(for job: CurrentJob) -> [WatchedJob] {
        effectiveWatchedJobs
            .filter { $0.clusterID == job.clusterID && job.depends(on: $0.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func watchedDependents(for job: CurrentJob) -> [WatchedJob] {
        effectiveWatchedJobs
            .filter { $0.clusterID == job.clusterID && $0.depends(on: job.jobID) }
            .sorted(by: compareWatchedJobs)
    }

    public func refreshAll() async {
        #if DEBUG
        if isDemoDataEnabled {
            refreshDemoData()
            return
        }
        #endif
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
        #if DEBUG
        if isDemoDataEnabled {
            refreshDemoData()
            return
        }
        #endif
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
        effectiveReachabilityByCluster[clusterID] ?? ClusterReachabilityState()
    }

    public func clusterLoad(for clusterID: ClusterID) -> ClusterLoadSnapshot {
        effectiveClusterLoadByCluster[clusterID] ?? ClusterLoadSnapshot.unknown()
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

    private var effectiveWatchedJobs: [WatchedJob] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.watchedJobs
        }
        #endif
        return watchedJobs
    }

    private var effectiveCurrentJobsByCluster: [ClusterID: [CurrentJob]] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.currentJobsByCluster
        }
        #endif
        return currentJobsByCluster
    }

    private var effectiveReachabilityByCluster: [ClusterID: ClusterReachabilityState] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.reachabilityByCluster
        }
        #endif
        return reachabilityByCluster
    }

    private var effectiveClusterLoadByCluster: [ClusterID: ClusterLoadSnapshot] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.clusterLoadByCluster
        }
        #endif
        return clusterLoadByCluster
    }

    private var effectiveLogPathsByJobKey: [String: JobLogPaths] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.logPathsByJobKey
        }
        #endif
        return logPathsByJobKey
    }

    private var effectiveLaunchDetailsByJobKey: [String: JobLaunchDetails] {
        #if DEBUG
        if isDemoDataEnabled, let demoState {
            return demoState.launchDetailsByJobKey
        }
        #endif
        return launchDetailsByJobKey
    }

    private func cancel(jobIDsByCluster: [ClusterID: [String]]) async -> Bool {
        #if DEBUG
        if isDemoDataEnabled {
            let cleanedByCluster = jobIDsByCluster
                .mapValues { Array(Set($0.map(\.trimmedOrEmpty).filter { !$0.isEmpty })) }
                .filter { !$0.value.isEmpty }
            guard !cleanedByCluster.isEmpty else { return false }
            demoState?.cancel(jobIDsByCluster: cleanedByCluster, now: nowProvider())
            return true
        }
        #endif
        let cleanedByCluster = jobIDsByCluster
            .mapValues { Array(Set($0.map(\.trimmedOrEmpty).filter { !$0.isEmpty })).sorted() }
            .filter { !$0.value.isEmpty }

        guard !cleanedByCluster.isEmpty else { return false }

        var refreshedClusterIDs: [ClusterID] = []
        var allSucceeded = true

        for (clusterID, jobIDs) in cleanedByCluster {
            guard let cluster = clusters.first(where: { $0.id == clusterID }), cluster.isEnabled else {
                allSucceeded = false
                continue
            }

            do {
                try await slurmClient.cancelJobs(for: cluster, jobIDs: jobIDs)
                refreshedClusterIDs.append(clusterID)
            } catch {
                allSucceeded = false
            }
        }

        for clusterID in refreshedClusterIDs {
            await refreshCluster(id: clusterID)
        }

        return allSucceeded && !refreshedClusterIDs.isEmpty
    }

    private func fetchLaunchDetails(clusterID: ClusterID, jobID: String) async -> JobLaunchDetails? {
        let key = jobKey(clusterID: clusterID, jobID: jobID)
        #if DEBUG
        if isDemoDataEnabled {
            return demoState?.launchDetailsByJobKey[key]
        }
        #endif
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
            ),
            isDemoDataEnabled: isDemoDataEnabled
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

    #if DEBUG
    private func refreshDemoData() {
        demoState?.refresh(now: nowProvider())
    }

    private static func makeDemoDataState(now: Date) -> DemoDataState {
        DemoDataState.make(now: now)
    }
    #endif
}

#if DEBUG
private struct DemoDataState {
    var clusters: [ClusterConfig]
    var currentJobsByCluster: [ClusterID: [CurrentJob]]
    var watchedJobs: [WatchedJob]
    var reachabilityByCluster: [ClusterID: ClusterReachabilityState]
    var clusterLoadByCluster: [ClusterID: ClusterLoadSnapshot]
    var logPathsByJobKey: [String: JobLogPaths]
    var launchDetailsByJobKey: [String: JobLaunchDetails]
    var logLinesByJobStreamKey: [String: [String]]

    static func make(now: Date) -> DemoDataState {
        let alphaID = ClusterID(rawValue: "demo-cluster-alpha")
        let betaID = ClusterID(rawValue: "demo-cluster-beta")
        let gammaID = ClusterID(rawValue: "demo-cluster-gamma")

        let clusters = [
            ClusterConfig(id: alphaID, displayName: "Demo Cluster Alpha", sshAlias: "demo-alpha", usernameOverride: "demo-user"),
            ClusterConfig(id: betaID, displayName: "Demo Cluster Beta", sshAlias: "demo-beta", usernameOverride: "demo-user"),
            ClusterConfig(id: gammaID, displayName: "Demo Cluster Gamma", sshAlias: "demo-gamma", usernameOverride: "demo-user")
        ]

        let alphaRunning = CurrentJob(
            clusterID: alphaID,
            jobID: "910101",
            jobName: "feature-extract-a1",
            owner: "demo-user",
            state: .running,
            rawState: "RUNNING",
            submitTime: now.addingTimeInterval(-8_400),
            startTime: now.addingTimeInterval(-7_800),
            elapsedSeconds: 7_800,
            qosName: "priority",
            gpuCount: 1,
            nodeCount: 1
        )
        let alphaBlocked = CurrentJob(
            clusterID: alphaID,
            jobID: "910102",
            jobName: "aggregate-batches-a1",
            owner: "demo-user",
            state: .pending,
            rawState: "PENDING",
            submitTime: now.addingTimeInterval(-7_700),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:910101",
            dependencyJobIDs: ["910101"],
            dependencyIsActive: true,
            qosName: "priority",
            gpuCount: 1,
            nodeCount: 1
        )
        let alphaPublish = CurrentJob(
            clusterID: alphaID,
            jobID: "910103",
            jobName: "publish-report-a1",
            owner: "demo-user",
            state: .pending,
            rawState: "PENDING",
            submitTime: now.addingTimeInterval(-7_600),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:910102",
            dependencyJobIDs: ["910102"],
            dependencyIsActive: true,
            qosName: "standard",
            gpuCount: 1,
            nodeCount: 1
        )
        let alphaEval = CurrentJob(
            clusterID: alphaID,
            jobID: "910120",
            jobName: "evaluate-suite-a2",
            owner: "demo-user",
            state: .running,
            rawState: "RUNNING",
            submitTime: now.addingTimeInterval(-2_100),
            startTime: now.addingTimeInterval(-1_500),
            elapsedSeconds: 1_500,
            qosName: "standard",
            gpuCount: 1,
            nodeCount: 1
        )

        let betaRunning = CurrentJob(
            clusterID: betaID,
            jobID: "920202",
            jobName: "prep-index-b1",
            owner: "demo-user",
            state: .running,
            rawState: "RUNNING",
            submitTime: now.addingTimeInterval(-5_100),
            startTime: now.addingTimeInterval(-4_200),
            elapsedSeconds: 4_200,
            qosName: "batch",
            gpuCount: 2,
            nodeCount: 1
        )
        let betaDependent = CurrentJob(
            clusterID: betaID,
            jobID: "920203",
            jobName: "validate-index-b1",
            owner: "demo-user",
            state: .pending,
            rawState: "PENDING",
            submitTime: now.addingTimeInterval(-3_900),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:920202",
            dependencyJobIDs: ["920202"],
            dependencyIsActive: true,
            qosName: "batch",
            gpuCount: 1,
            nodeCount: 1
        )
        let betaPending = CurrentJob(
            clusterID: betaID,
            jobID: "920201",
            jobName: "train-ranker-b1",
            owner: "demo-user",
            state: .pending,
            rawState: "PENDING",
            submitTime: now.addingTimeInterval(-1_800),
            pendingReason: "Assoc Grp GRES",
            qosName: "interactive",
            gpuCount: 2,
            nodeCount: 1
        )
        let betaShip = CurrentJob(
            clusterID: betaID,
            jobID: "920240",
            jobName: "ship-dashboard-b1",
            owner: "demo-user",
            state: .running,
            rawState: "RUNNING",
            submitTime: now.addingTimeInterval(-1_200),
            startTime: now.addingTimeInterval(-720),
            elapsedSeconds: 720,
            qosName: "interactive",
            gpuCount: 1,
            nodeCount: 1
        )

        let watchedJobs = [
            WatchedJob(currentJob: alphaRunning, now: now.addingTimeInterval(-7_800)),
            WatchedJob(currentJob: alphaBlocked, now: now.addingTimeInterval(-7_700)),
            WatchedJob(currentJob: alphaPublish, now: now.addingTimeInterval(-7_600)),
            WatchedJob(currentJob: betaRunning, now: now.addingTimeInterval(-4_200)),
            WatchedJob(currentJob: betaDependent, now: now.addingTimeInterval(-3_900)),
            WatchedJob(
                clusterID: betaID,
                jobID: "920150",
                jobName: "summarize-benchmark-b0",
                owner: "demo-user",
                state: .completed,
                rawState: "COMPLETED",
                notificationSent: true,
                submitTime: now.addingTimeInterval(-95_400),
                startTime: now.addingTimeInterval(-94_800),
                endTime: now.addingTimeInterval(-91_800),
                elapsedSeconds: 3_000,
                firstSeenAt: now.addingTimeInterval(-94_800),
                lastUpdatedAt: now.addingTimeInterval(-91_800),
                lastSuccessfulRefreshAt: now.addingTimeInterval(-300)
            ),
            WatchedJob(
                clusterID: gammaID,
                jobID: "930301",
                jobName: "sync-catalog-g1",
                owner: "demo-user",
                state: .running,
                rawState: "RUNNING",
                submitTime: now.addingTimeInterval(-18_000),
                startTime: now.addingTimeInterval(-17_400),
                elapsedSeconds: 17_400,
                firstSeenAt: now.addingTimeInterval(-17_400),
                lastUpdatedAt: now.addingTimeInterval(-2_700),
                lastSuccessfulRefreshAt: now.addingTimeInterval(-2_700),
                isStale: true
            )
        ]

        let currentJobsByCluster: [ClusterID: [CurrentJob]] = [
            alphaID: [alphaRunning, alphaBlocked, alphaPublish, alphaEval],
            betaID: [betaRunning, betaDependent, betaPending, betaShip],
            gammaID: []
        ]

        let reachabilityByCluster: [ClusterID: ClusterReachabilityState] = [
            alphaID: ClusterReachabilityState(status: .reachable, lastSuccessfulRefresh: now.addingTimeInterval(-40)),
            betaID: ClusterReachabilityState(status: .reachable, lastSuccessfulRefresh: now.addingTimeInterval(-55)),
            gammaID: ClusterReachabilityState(
                status: .unreachable,
                lastSuccessfulRefresh: now.addingTimeInterval(-2_700),
                lastErrorMessage: "Demo endpoint timed out"
            )
        ]

        let clusterLoadByCluster: [ClusterID: ClusterLoadSnapshot] = [
            alphaID: ClusterLoadSnapshot(
                level: .busy,
                jobCount: 14,
                pendingJobCount: 4,
                scopedFreeGPUCount: 3,
                scopedTotalGPUCount: 8,
                scopedGPUDescription: "All QoS",
                qosGPUAvailabilities: [
                    ClusterQoSGPUAvailability(qosName: "priority", freeGPUCount: 1, totalGPUCount: 2),
                    ClusterQoSGPUAvailability(qosName: "standard", freeGPUCount: 2, totalGPUCount: 6)
                ],
                freeCPUCount: 96,
                totalCPUCount: 128,
                freeGPUCount: 6,
                totalGPUCount: 16,
                freeNodeCount: 2,
                totalNodeCount: 4,
                jobHeadroom: 2,
                accessiblePartitions: ["gpu-a"],
                lastUpdatedAt: now.addingTimeInterval(-40)
            ),
            betaID: ClusterLoadSnapshot(
                level: .open,
                jobCount: 7,
                pendingJobCount: 1,
                scopedFreeGPUCount: 5,
                scopedTotalGPUCount: 10,
                scopedGPUDescription: "All QoS",
                qosGPUAvailabilities: [
                    ClusterQoSGPUAvailability(qosName: "batch", freeGPUCount: 3, totalGPUCount: 6),
                    ClusterQoSGPUAvailability(qosName: "interactive", freeGPUCount: 2, totalGPUCount: 4)
                ],
                freeCPUCount: 220,
                totalCPUCount: 256,
                freeGPUCount: 8,
                totalGPUCount: 12,
                freeNodeCount: 2,
                totalNodeCount: 3,
                jobHeadroom: 6,
                accessiblePartitions: ["gpu-b"],
                lastUpdatedAt: now.addingTimeInterval(-55)
            ),
            gammaID: ClusterLoadSnapshot.unknown(message: "Load unavailable while the demo cluster is unreachable.", lastUpdatedAt: now.addingTimeInterval(-2_700))
        ]

        let logPathsByJobKey = [
            jobKey(clusterID: alphaID, jobID: "910101"): JobLogPaths(stdoutPath: "/demo/logs/feature-extract-a1-910101.out", stderrPath: "/demo/logs/feature-extract-a1-910101.err"),
            jobKey(clusterID: alphaID, jobID: "910120"): JobLogPaths(stdoutPath: "/demo/logs/evaluate-suite-a2-910120.out", stderrPath: "/demo/logs/evaluate-suite-a2-910120.err"),
            jobKey(clusterID: betaID, jobID: "920202"): JobLogPaths(stdoutPath: "/demo/logs/prep-index-b1-920202.out", stderrPath: "/demo/logs/prep-index-b1-920202.err"),
            jobKey(clusterID: betaID, jobID: "920240"): JobLogPaths(stdoutPath: "/demo/logs/ship-dashboard-b1-920240.out", stderrPath: "/demo/logs/ship-dashboard-b1-920240.err"),
            jobKey(clusterID: betaID, jobID: "920150"): JobLogPaths(stdoutPath: "/demo/logs/summarize-benchmark-b0-920150.out", stderrPath: "/demo/logs/summarize-benchmark-b0-920150.err"),
            jobKey(clusterID: gammaID, jobID: "930301"): JobLogPaths(stdoutPath: "/demo/logs/sync-catalog-g1-930301.out", stderrPath: "/demo/logs/sync-catalog-g1-930301.err")
        ]

        let launchDetailsByJobKey = [
            jobKey(clusterID: alphaID, jobID: "910101"): JobLaunchDetails(
                commandText: "python jobs/feature_extract.py --dataset shard-a1 --output demo/feature-a1",
                batchScriptText: "#!/bin/bash\npython jobs/feature_extract.py \\\n  --dataset shard-a1 \\\n  --output demo/feature-a1",
                workDirectory: "/demo/workflows/alpha"
            ),
            jobKey(clusterID: alphaID, jobID: "910120"): JobLaunchDetails(
                commandText: "python jobs/evaluate_suite.py --suite smoke-a2 --report demo/report-a2.json",
                batchScriptText: "#!/bin/bash\npython jobs/evaluate_suite.py \\\n  --suite smoke-a2 \\\n  --report demo/report-a2.json",
                workDirectory: "/demo/workflows/alpha"
            ),
            jobKey(clusterID: betaID, jobID: "920202"): JobLaunchDetails(
                commandText: "python jobs/prep_index.py --source sample-b1 --out demo/index-b1",
                batchScriptText: "#!/bin/bash\npython jobs/prep_index.py \\\n  --source sample-b1 \\\n  --out demo/index-b1",
                workDirectory: "/demo/workflows/beta"
            ),
            jobKey(clusterID: betaID, jobID: "920240"): JobLaunchDetails(
                commandText: "python jobs/ship_dashboard.py --input demo/index-b1 --publish",
                batchScriptText: "#!/bin/bash\npython jobs/ship_dashboard.py --input demo/index-b1 --publish",
                workDirectory: "/demo/workflows/beta"
            ),
            jobKey(clusterID: betaID, jobID: "920150"): JobLaunchDetails(
                commandText: "python jobs/summarize_benchmark.py --input demo/results-b0 --format markdown",
                batchScriptText: "#!/bin/bash\npython jobs/summarize_benchmark.py --input demo/results-b0 --format markdown",
                workDirectory: "/demo/workflows/beta"
            ),
            jobKey(clusterID: gammaID, jobID: "930301"): JobLaunchDetails(
                commandText: "python jobs/sync_catalog.py --source archive-g1 --dest mirror-g1",
                batchScriptText: "#!/bin/bash\npython jobs/sync_catalog.py --source archive-g1 --dest mirror-g1",
                workDirectory: "/demo/workflows/gamma"
            )
        ]

        let logLinesByJobStreamKey = [
            logKey(clusterID: alphaID, jobID: "910101", stream: .stdout): [
                "08:00:12 stage=read shard=00 rows=1024",
                "08:03:34 stage=transform shard=00 vectors=1024",
                "08:05:55 stage=write shard=00 output=demo/feature-a1/part-0000.parquet",
                "08:07:10 stage=read shard=01 rows=1024",
                "08:09:48 stage=transform shard=01 vectors=1024",
                "08:12:01 stage=write shard=01 output=demo/feature-a1/part-0001.parquet"
            ],
            logKey(clusterID: alphaID, jobID: "910120", stream: .stdout): [
                "09:41:02 suite=smoke-a2 case=latency status=pass",
                "09:41:30 suite=smoke-a2 case=accuracy status=pass",
                "09:42:04 suite=smoke-a2 case=stability status=pass"
            ],
            logKey(clusterID: betaID, jobID: "920202", stream: .stdout): [
                "10:02:11 phase=scan chunks=12",
                "10:05:44 phase=normalize records=24000",
                "10:08:17 phase=merge segments=4",
                "10:10:59 phase=write path=demo/index-b1"
            ],
            logKey(clusterID: betaID, jobID: "920240", stream: .stdout): [
                "10:21:00 publish target=demo-dashboard",
                "10:21:08 upload completed",
                "10:21:15 cache invalidated"
            ],
            logKey(clusterID: betaID, jobID: "920150", stream: .stdout): [
                "07:15:00 summary start",
                "07:16:15 summary wrote demo/results-b0/overview.md"
            ],
            logKey(clusterID: gammaID, jobID: "930301", stream: .stdout): [
                "06:40:12 sync start mirror-g1",
                "06:44:55 sync stalled waiting for endpoint"
            ]
        ]

        return DemoDataState(
            clusters: clusters,
            currentJobsByCluster: currentJobsByCluster,
            watchedJobs: watchedJobs,
            reachabilityByCluster: reachabilityByCluster,
            clusterLoadByCluster: clusterLoadByCluster,
            logPathsByJobKey: logPathsByJobKey,
            launchDetailsByJobKey: launchDetailsByJobKey,
            logLinesByJobStreamKey: logLinesByJobStreamKey
        )
    }

    mutating func watch(job: CurrentJob, now: Date) {
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

    mutating func unwatch(jobIDs: some Sequence<String>) {
        let jobIDSet = Set(jobIDs)
        watchedJobs.removeAll { jobIDSet.contains($0.id) }
    }

    mutating func clearCompleted() {
        watchedJobs.removeAll(where: \.isTerminal)
    }

    mutating func cancel(jobIDsByCluster: [ClusterID: [String]], now: Date) {
        for (clusterID, jobIDs) in jobIDsByCluster {
            let targetIDs = Set(jobIDs)

            if var jobs = currentJobsByCluster[clusterID] {
                jobs.removeAll { targetIDs.contains($0.jobID) }
                currentJobsByCluster[clusterID] = jobs
            }

            for index in watchedJobs.indices where watchedJobs[index].clusterID == clusterID && targetIDs.contains(watchedJobs[index].jobID) {
                watchedJobs[index].state = .cancelled
                watchedJobs[index].rawState = "CANCELLED"
                watchedJobs[index].endTime = now
                watchedJobs[index].lastUpdatedAt = now
                watchedJobs[index].lastSuccessfulRefreshAt = now
                watchedJobs[index].notificationSent = true
                watchedJobs[index].isStale = false
            }
        }
    }

    mutating func refresh(now: Date) {
        for cluster in clusters {
            guard var reachability = reachabilityByCluster[cluster.id] else { continue }
            if reachability.status == .reachable {
                reachability.lastSuccessfulRefresh = now
                reachability.lastErrorMessage = nil
                reachabilityByCluster[cluster.id] = reachability
            }

            if var load = clusterLoadByCluster[cluster.id], load.level != .unknown {
                load.lastUpdatedAt = now
                clusterLoadByCluster[cluster.id] = load
            }
        }

        for index in watchedJobs.indices where !watchedJobs[index].isStale {
            watchedJobs[index].lastSuccessfulRefreshAt = now
        }
    }

    func tailOutput(for session: JobLogTailSession, stream: JobLogStream, lineCount: Int) -> String {
        let key = Self.logKey(clusterID: session.clusterID, jobID: session.jobID, stream: stream)
        let lines = logLinesByJobStreamKey[key] ?? ["No demo log lines for this stream yet."]
        return lines.suffix(max(1, lineCount)).joined(separator: "\n")
    }

    private static func jobKey(clusterID: ClusterID, jobID: String) -> String {
        "\(clusterID.rawValue):\(jobID)"
    }

    private static func logKey(clusterID: ClusterID, jobID: String, stream: JobLogStream) -> String {
        "\(clusterID.rawValue):\(jobID):\(stream.rawValue)"
    }
}
#endif
