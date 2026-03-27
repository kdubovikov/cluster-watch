import Foundation
import XCTest
@testable import ClusterWatchCore

@MainActor
final class JobStoreTests: XCTestCase {
    private let camdID = ClusterID(rawValue: "camd")
    private let csccID = ClusterID(rawValue: "cscc")

    func testUnreachableClusterKeepsWatchedJobAndMarksItStale() async {
        let watchedJob = WatchedJob(
            clusterID: camdID,
            jobID: "12345",
            jobName: "train-model",
            owner: "kirill",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 200),
            endTime: nil,
            elapsedSeconds: 60,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 260),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 260),
            isStale: false
        )

        let persistence = InMemoryPersistenceStore()
        let notifications = MockNotificationManager()
        let client = MockSlurmClient(
            currentResults: [camdID: .failure(SlurmClientError.commandFailed("Host unreachable"))],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            nowProvider: { Date(timeIntervalSince1970: 300) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: camdID)

        XCTAssertEqual(store.watchedJobs.count, 1)
        XCTAssertTrue(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].state, .running)
        XCTAssertEqual(store.reachability(for: camdID).status, .unreachable)
    }

    func testTerminalTransitionSendsNotificationOnlyOnce() async {
        let initialJob = WatchedJob(
            clusterID: camdID,
            jobID: "12345",
            jobName: "train-model",
            owner: "kirill",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 200),
            endTime: nil,
            elapsedSeconds: 100,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 250),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 250),
            isStale: false
        )

        let persistence = InMemoryPersistenceStore()
        let notifications = MockNotificationManager()
        let historicalSnapshot = JobSnapshot(
            jobID: "12345",
            owner: "kirill",
            state: .completed,
            rawState: "COMPLETED",
            jobName: "train-model",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 200),
            endTime: Date(timeIntervalSince1970: 400),
            elapsedSeconds: 200
        )

        let client = MockSlurmClient(
            currentResults: [camdID: .success([])],
            historicalResults: ["camd:12345": .success(historicalSnapshot)]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: camdID)
        await store.refreshCluster(id: camdID)

        XCTAssertEqual(store.watchedJobs[0].state, .completed)
        XCTAssertTrue(store.watchedJobs[0].notificationSent)
        XCTAssertEqual(notifications.sentJobIDs, ["12345"])
    }

    func testTerminalWatchedJobDoesNotRegressToRunningIfCurrentQueryStillShowsIt() async {
        let completedJob = WatchedJob(
            clusterID: camdID,
            jobID: "38071",
            jobName: "pes2o-filter-q35-7n-r5",
            owner: "salem.lahlou",
            state: .completed,
            rawState: "COMPLETED",
            notificationSent: true,
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            endTime: Date(timeIntervalSince1970: 200),
            elapsedSeconds: 100,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 200),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 200),
            isStale: false
        )

        let lingeringCurrentJob = CurrentJob(
            clusterID: camdID,
            jobID: "38071",
            jobName: "pes2o-filter-q35-7n-r5",
            owner: "salem.lahlou",
            state: .running,
            rawState: "COMPLETING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            elapsedSeconds: 101
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [camdID: .success([lingeringCurrentJob])],
                historicalResults: [:]
            ),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 260) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "salem.lahlou",
                pollIntervalSeconds: 30,
                watchedJobs: [completedJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: camdID)

        XCTAssertEqual(store.watchedJobs[0].state, .completed)
        XCTAssertEqual(store.watchedJobs[0].rawState, "COMPLETED")
        XCTAssertEqual(store.watchedJobs[0].endTime, Date(timeIntervalSince1970: 200))
        XCTAssertEqual(store.watchedJobs[0].lastSuccessfulRefreshAt, Date(timeIntervalSince1970: 260))
        XCTAssertEqual(store.watchedJobs[0].lastUpdatedAt, Date(timeIntervalSince1970: 200))
    }

    func testMissingHistoricalRecordKeepsLastKnownStateButClearsStale() async {
        let initialJob = WatchedJob(
            clusterID: csccID,
            jobID: "999",
            jobName: "wait-job",
            owner: "kirill",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: nil,
            endTime: nil,
            elapsedSeconds: nil,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 120),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120),
            isStale: true
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [csccID: .success([])],
                historicalResults: ["cscc:999": .success(nil)]
            ),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: csccID)

        XCTAssertEqual(store.watchedJobs[0].state, .pending)
        XCTAssertFalse(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].lastUpdatedAt, Date(timeIntervalSince1970: 120))
    }

    func testConcurrentClusterRefreshesDoNotOverwriteOtherClusterUpdates() async {
        let camdJob = WatchedJob(
            clusterID: camdID,
            jobID: "38071",
            jobName: "pes2o-filter-q35-7n-r5",
            owner: "salem.lahlou",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            endTime: nil,
            elapsedSeconds: 50,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 100),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 100),
            isStale: false
        )

        let csccJob = WatchedJob(
            clusterID: csccID,
            jobID: "148463",
            jobName: "tb_gsm8k_t10",
            owner: "kirill.dubovikov",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: nil,
            endTime: nil,
            elapsedSeconds: 0,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 100),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 100),
            isStale: false
        )

        let camdHistorical = JobSnapshot(
            jobID: "38071",
            owner: "salem.lahlou",
            state: .completed,
            rawState: "COMPLETED",
            jobName: "pes2o-filter-q35-7n-r5",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            endTime: Date(timeIntervalSince1970: 200),
            elapsedSeconds: 100
        )

        let csccCurrent = CurrentJob(
            clusterID: csccID,
            jobID: "148463",
            jobName: "tb_gsm8k_t10",
            owner: "kirill.dubovikov",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 120),
            elapsedSeconds: 30
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [
                    camdID: .success([]),
                    csccID: .success([csccCurrent]),
                ],
                historicalResults: [
                    "camd:38071": .success(camdHistorical),
                ],
                historicalDelaysNanoseconds: [
                    "camd:38071": 150_000_000
                ]
            ),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 250) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill.dubovikov",
                pollIntervalSeconds: 30,
                watchedJobs: [camdJob, csccJob],
                reachabilityByCluster: [:]
            )
        )

        async let camdRefresh: Void = store.refreshCluster(id: camdID)
        async let csccRefresh: Void = store.refreshCluster(id: csccID)
        _ = await (camdRefresh, csccRefresh)

        let refreshedCAMDJob = try XCTUnwrap(store.watchedJobs.first { $0.jobID == "38071" })
        XCTAssertEqual(refreshedCAMDJob.state, .completed)
        XCTAssertTrue(refreshedCAMDJob.notificationSent)

        let refreshedCSCCJob = try XCTUnwrap(store.watchedJobs.first { $0.jobID == "148463" })
        XCTAssertEqual(refreshedCSCCJob.state, .running)
        XCTAssertEqual(refreshedCSCCJob.rawState, "RUNNING")
        XCTAssertEqual(refreshedCSCCJob.startTime, Date(timeIntervalSince1970: 120))
    }

    func testWatchedDependencyHelpersExposeUpstreamAndDownstreamJobs() async {
        let dependencyJob = WatchedJob(
            clusterID: camdID,
            jobID: "12345",
            jobName: "preprocess",
            owner: "kirill",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 120),
            endTime: nil,
            elapsedSeconds: 50,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 150),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 150),
            isStale: false
        )

        let dependentJob = WatchedJob(
            clusterID: camdID,
            jobID: "22300",
            jobName: "train-model",
            owner: "kirill",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 160),
            startTime: nil,
            endTime: nil,
            elapsedSeconds: nil,
            pendingReason: "Dependency",
            dependencyExpression: "afterok:12345",
            dependencyJobIDs: ["12345"],
            dependencyIsActive: true,
            firstSeenAt: Date(timeIntervalSince1970: 160),
            lastUpdatedAt: Date(timeIntervalSince1970: 200),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 200),
            isStale: false
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(currentResults: [:], historicalResults: [:]),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [dependencyJob, dependentJob],
                reachabilityByCluster: [:]
            )
        )

        XCTAssertEqual(store.watchedDependencies(for: dependentJob).map(\.jobID), ["12345"])
        XCTAssertEqual(store.watchedDependents(for: dependencyJob).map(\.jobID), ["22300"])
    }

    func testPrepareLogTailCreatesSessionForDetectedStdoutPath() async {
        let watchedJob = WatchedJob(
            clusterID: csccID,
            jobID: "148463",
            jobName: "tb_gsm8k_t10",
            owner: "kirill.dubovikov",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 120),
            endTime: nil,
            elapsedSeconds: 30,
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 120),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120),
            isStale: false
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [:],
                historicalResults: [:],
                logPathResults: [
                    "cscc:148463": .success(
                        JobLogPaths(
                            stdoutPath: "/logs/tb_gsm8k_t10-148463.out",
                            stderrPath: "/logs/tb_gsm8k_t10-148463.err"
                        )
                    )
                ]
            ),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "kirill.dubovikov",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        let opened = await store.prepareLogTail(for: watchedJob)

        XCTAssertTrue(opened)
        XCTAssertEqual(store.activeLogTail?.jobID, "148463")
        XCTAssertEqual(store.activeLogTail?.preferredStream, .stdout)
        XCTAssertEqual(store.activeLogTail?.paths.stderrPath, "/logs/tb_gsm8k_t10-148463.err")
    }

    private func sampleClusters() -> [ClusterConfig] {
        [
            ClusterConfig(id: camdID, displayName: "CAMD", sshAlias: "camd1"),
            ClusterConfig(id: csccID, displayName: "CSCC", sshAlias: "cscc")
        ]
    }
}

private actor InMemoryPersistenceStore: PersistenceStoring {
    private var storedState: PersistedAppState?

    init(storedState: PersistedAppState? = nil) {
        self.storedState = storedState
    }

    func load() async -> PersistedAppState? {
        storedState
    }

    func save(_ state: PersistedAppState) async throws {
        storedState = state
    }
}

private actor MockSlurmClient: SlurmClientProtocol {
    private let currentResults: [ClusterID: Result<[CurrentJob], Error>]
    private let historicalResults: [String: Result<JobSnapshot?, Error>]
    private let logPathResults: [String: Result<JobLogPaths?, Error>]
    private let tailResults: [String: Result<String, Error>]
    private let currentDelaysNanoseconds: [ClusterID: UInt64]
    private let historicalDelaysNanoseconds: [String: UInt64]

    init(
        currentResults: [ClusterID: Result<[CurrentJob], Error>],
        historicalResults: [String: Result<JobSnapshot?, Error>],
        logPathResults: [String: Result<JobLogPaths?, Error>] = [:],
        tailResults: [String: Result<String, Error>] = [:],
        currentDelaysNanoseconds: [ClusterID: UInt64] = [:],
        historicalDelaysNanoseconds: [String: UInt64] = [:]
    ) {
        self.currentResults = currentResults
        self.historicalResults = historicalResults
        self.logPathResults = logPathResults
        self.tailResults = tailResults
        self.currentDelaysNanoseconds = currentDelaysNanoseconds
        self.historicalDelaysNanoseconds = historicalDelaysNanoseconds
    }

    func fetchCurrentJobs(for cluster: ClusterConfig, username: String) async throws -> [CurrentJob] {
        if let delay = currentDelaysNanoseconds[cluster.id] {
            try? await Task.sleep(nanoseconds: delay)
        }
        if let result = currentResults[cluster.id] {
            return try result.get()
        }
        return []
    }

    func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot? {
        let key = "\(cluster.id.rawValue):\(jobID)"
        if let delay = historicalDelaysNanoseconds[key] {
            try? await Task.sleep(nanoseconds: delay)
        }
        if let result = historicalResults[key] {
            return try result.get()
        }
        return nil
    }

    func fetchLogPaths(for cluster: ClusterConfig, jobID: String) async throws -> JobLogPaths? {
        if let result = logPathResults["\(cluster.id.rawValue):\(jobID)"] {
            return try result.get()
        }
        return nil
    }

    func tailLog(for cluster: ClusterConfig, remotePath: String, lineCount: Int) async throws -> String {
        if let result = tailResults["\(cluster.id.rawValue):\(remotePath)"] {
            return try result.get()
        }
        return ""
    }
}

private final class MockNotificationManager: NotificationManaging {
    private(set) var sentJobIDs: [String] = []

    func requestAuthorizationIfNeeded() async {}

    func sendTerminalNotification(for job: WatchedJob, clusterName: String) async {
        sentJobIDs.append(job.jobID)
    }
}
