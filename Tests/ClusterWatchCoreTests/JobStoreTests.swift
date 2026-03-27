import Foundation
import XCTest
@testable import ClusterWatchCore

@MainActor
final class JobStoreTests: XCTestCase {
    func testUnreachableClusterKeepsWatchedJobAndMarksItStale() async {
        let watchedJob = WatchedJob(
            clusterID: .camd,
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
            currentResults: [.camd: .failure(SlurmClientError.commandFailed("Host unreachable"))],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            nowProvider: { Date(timeIntervalSince1970: 300) },
            initialState: PersistedAppState(
                clusters: ClusterConfig.defaultClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: .camd)

        XCTAssertEqual(store.watchedJobs.count, 1)
        XCTAssertTrue(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].state, .running)
        XCTAssertEqual(store.reachability(for: .camd).status, .unreachable)
    }

    func testTerminalTransitionSendsNotificationOnlyOnce() async {
        let initialJob = WatchedJob(
            clusterID: .camd,
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
            currentResults: [.camd: .success([])],
            historicalResults: ["camd:12345": .success(historicalSnapshot)]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: ClusterConfig.defaultClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: .camd)
        await store.refreshCluster(id: .camd)

        XCTAssertEqual(store.watchedJobs[0].state, .completed)
        XCTAssertTrue(store.watchedJobs[0].notificationSent)
        XCTAssertEqual(notifications.sentJobIDs, ["12345"])
    }

    func testMissingHistoricalRecordKeepsLastKnownStateButClearsStale() async {
        let initialJob = WatchedJob(
            clusterID: .cscc,
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
                currentResults: [.cscc: .success([])],
                historicalResults: ["cscc:999": .success(nil)]
            ),
            notificationManager: MockNotificationManager(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: ClusterConfig.defaultClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: .cscc)

        XCTAssertEqual(store.watchedJobs[0].state, .pending)
        XCTAssertFalse(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].lastUpdatedAt, Date(timeIntervalSince1970: 120))
    }

    func testWatchedDependencyHelpersExposeUpstreamAndDownstreamJobs() async {
        let dependencyJob = WatchedJob(
            clusterID: .camd,
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
            clusterID: .camd,
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
                clusters: ClusterConfig.defaultClusters(),
                globalUsernameFilter: "kirill",
                pollIntervalSeconds: 30,
                watchedJobs: [dependencyJob, dependentJob],
                reachabilityByCluster: [:]
            )
        )

        XCTAssertEqual(store.watchedDependencies(for: dependentJob).map(\.jobID), ["12345"])
        XCTAssertEqual(store.watchedDependents(for: dependencyJob).map(\.jobID), ["22300"])
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

    init(
        currentResults: [ClusterID: Result<[CurrentJob], Error>],
        historicalResults: [String: Result<JobSnapshot?, Error>]
    ) {
        self.currentResults = currentResults
        self.historicalResults = historicalResults
    }

    func fetchCurrentJobs(for cluster: ClusterConfig, username: String) async throws -> [CurrentJob] {
        if let result = currentResults[cluster.id] {
            return try result.get()
        }
        return []
    }

    func fetchHistoricalJob(for cluster: ClusterConfig, jobID: String) async throws -> JobSnapshot? {
        if let result = historicalResults["\(cluster.id.rawValue):\(jobID)"] {
            return try result.get()
        }
        return nil
    }
}

private final class MockNotificationManager: NotificationManaging {
    private(set) var sentJobIDs: [String] = []

    func requestAuthorizationIfNeeded() async {}

    func sendTerminalNotification(for job: WatchedJob, clusterName: String) async {
        sentJobIDs.append(job.jobID)
    }
}
