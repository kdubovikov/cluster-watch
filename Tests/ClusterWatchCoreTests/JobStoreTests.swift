import Foundation
import XCTest
@testable import ClusterWatchCore

@MainActor
final class JobStoreTests: XCTestCase {
    private let alphaClusterID = ClusterID(rawValue: "cluster-alpha")
    private let betaClusterID = ClusterID(rawValue: "cluster-beta")

    func testUnreachableClusterKeepsWatchedJobAndMarksItStale() async {
        let watchedJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "12345",
            jobName: "train-model",
            owner: "test-user",
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
            currentResults: [alphaClusterID: .failure(SlurmClientError.commandFailed("Host unreachable"))],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 300) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: alphaClusterID)

        XCTAssertEqual(store.watchedJobs.count, 1)
        XCTAssertTrue(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].state, NormalizedJobState.running)
        XCTAssertEqual(store.reachability(for: alphaClusterID).status, ClusterReachabilityState.Status.unreachable)
    }

    func testTerminalTransitionSendsNotificationOnlyOnce() async {
        let initialJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "12345",
            jobName: "train-model",
            owner: "test-user",
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
            owner: "test-user",
            state: .completed,
            rawState: "COMPLETED",
            jobName: "train-model",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 200),
            endTime: Date(timeIntervalSince1970: 400),
            elapsedSeconds: 200
        )

        let client = MockSlurmClient(
            currentResults: [alphaClusterID: .success([])],
            historicalResults: ["cluster-alpha:12345": .success(historicalSnapshot)]
        )

        let store = JobStore(
            persistence: persistence,
            slurmClient: client,
            notificationManager: notifications,
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: alphaClusterID)
        await store.refreshCluster(id: alphaClusterID)

        XCTAssertEqual(store.watchedJobs[0].state, NormalizedJobState.completed)
        XCTAssertTrue(store.watchedJobs[0].notificationSent)
        XCTAssertEqual(notifications.sentJobIDs, ["12345"])
    }

    func testTerminalWatchedJobDoesNotRegressToRunningIfCurrentQueryStillShowsIt() async {
        let completedJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "41001",
            jobName: "prepare-data",
            owner: "owner-a",
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
            clusterID: alphaClusterID,
            jobID: "41001",
            jobName: "prepare-data",
            owner: "owner-a",
            state: .running,
            rawState: "COMPLETING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            elapsedSeconds: 101
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [alphaClusterID: .success([lingeringCurrentJob])],
                historicalResults: [:]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 260) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [completedJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: alphaClusterID)

        XCTAssertEqual(store.watchedJobs[0].state, NormalizedJobState.completed)
        XCTAssertEqual(store.watchedJobs[0].rawState, "COMPLETED")
        XCTAssertEqual(store.watchedJobs[0].endTime, Date(timeIntervalSince1970: 200))
        XCTAssertEqual(store.watchedJobs[0].lastSuccessfulRefreshAt, Date(timeIntervalSince1970: 260))
        XCTAssertEqual(store.watchedJobs[0].lastUpdatedAt, Date(timeIntervalSince1970: 200))
    }

    func testMissingHistoricalRecordKeepsLastKnownStateButClearsStale() async {
        let initialJob = WatchedJob(
            clusterID: betaClusterID,
            jobID: "999",
            jobName: "wait-job",
            owner: "test-user",
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
                currentResults: [betaClusterID: .success([])],
                historicalResults: ["cluster-beta:999": .success(nil)]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [initialJob],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: betaClusterID)

        XCTAssertEqual(store.watchedJobs[0].state, NormalizedJobState.pending)
        XCTAssertFalse(store.watchedJobs[0].isStale)
        XCTAssertEqual(store.watchedJobs[0].lastUpdatedAt, Date(timeIntervalSince1970: 120))
    }

    func testSuccessfulRefreshPublishesClusterLoadSnapshot() async {
        let currentJob = CurrentJob(
            clusterID: alphaClusterID,
            jobID: "123",
            jobName: "queue-check",
            owner: "test-user",
            state: .running,
            rawState: "RUNNING"
        )

        let expectedLoad = ClusterLoadSnapshot(
            level: .busy,
            jobCount: 12,
            pendingJobCount: 4,
            freeGPUCount: 8,
            totalGPUCount: 64,
            jobHeadroom: 4,
            accessiblePartitions: ["gpu"],
            lastUpdatedAt: Date(timeIntervalSince1970: 300)
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [alphaClusterID: .success([currentJob])],
                historicalResults: [:],
                clusterLoadResults: [alphaClusterID: .success(expectedLoad)]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 300) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: alphaClusterID)

        XCTAssertEqual(store.clusterLoad(for: alphaClusterID), expectedLoad)
        XCTAssertEqual(store.reachability(for: alphaClusterID).status, .reachable)
    }

    func testClusterLoadFailureDoesNotMarkReachableClusterDown() async {
        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(
                currentResults: [alphaClusterID: .success([])],
                historicalResults: [:],
                clusterLoadResults: [alphaClusterID: .failure(SlurmClientError.commandFailed("sacctmgr timeout"))]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 300) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [],
                reachabilityByCluster: [:]
            )
        )

        await store.refreshCluster(id: alphaClusterID)

        XCTAssertEqual(store.reachability(for: alphaClusterID).status, .reachable)
        XCTAssertEqual(store.clusterLoad(for: alphaClusterID).level, .unknown)
        XCTAssertEqual(store.clusterLoad(for: alphaClusterID).message, "sacctmgr timeout")
    }

    func testConcurrentClusterRefreshesDoNotOverwriteOtherClusterUpdates() async throws {
        let alphaJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "41001",
            jobName: "prepare-data",
            owner: "owner-a",
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

        let betaJob = WatchedJob(
            clusterID: betaClusterID,
            jobID: "52001",
            jobName: "train-model",
            owner: "owner-b",
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

        let alphaHistorical = JobSnapshot(
            jobID: "41001",
            owner: "owner-a",
            state: .completed,
            rawState: "COMPLETED",
            jobName: "prepare-data",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 100),
            endTime: Date(timeIntervalSince1970: 200),
            elapsedSeconds: 100
        )

        let betaCurrent = CurrentJob(
            clusterID: betaClusterID,
            jobID: "52001",
            jobName: "train-model",
            owner: "owner-b",
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
                    alphaClusterID: .success([]),
                    betaClusterID: .success([betaCurrent]),
                ],
                historicalResults: [
                    "cluster-alpha:41001": .success(alphaHistorical),
                ],
                historicalDelaysNanoseconds: [
                    "cluster-alpha:41001": 150_000_000
                ]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 250) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-b",
                pollIntervalSeconds: 30,
                watchedJobs: [alphaJob, betaJob],
                reachabilityByCluster: [:]
            )
        )

        let alphaRefresh = Task { await store.refreshCluster(id: alphaClusterID) }
        let betaRefresh = Task { await store.refreshCluster(id: betaClusterID) }
        _ = await (alphaRefresh.value, betaRefresh.value)

        let refreshedAlphaJob = try XCTUnwrap(store.watchedJobs.first { $0.jobID == "41001" })
        XCTAssertEqual(refreshedAlphaJob.state, NormalizedJobState.completed)
        XCTAssertTrue(refreshedAlphaJob.notificationSent)

        let refreshedBetaJob = try XCTUnwrap(store.watchedJobs.first { $0.jobID == "52001" })
        XCTAssertEqual(refreshedBetaJob.state, NormalizedJobState.running)
        XCTAssertEqual(refreshedBetaJob.rawState, "RUNNING")
        XCTAssertEqual(refreshedBetaJob.startTime, Date(timeIntervalSince1970: 120))
    }

    func testWatchedDependencyHelpersExposeUpstreamAndDownstreamJobs() async {
        let dependencyJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "12345",
            jobName: "prepare-data",
            owner: "test-user",
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
            clusterID: alphaClusterID,
            jobID: "22300",
            jobName: "train-model",
            owner: "test-user",
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
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "test-user",
                pollIntervalSeconds: 30,
                watchedJobs: [dependencyJob, dependentJob],
                reachabilityByCluster: [:]
            )
        )

        XCTAssertEqual(store.watchedDependencies(for: dependentJob).map { $0.jobID }, ["12345"])
        XCTAssertEqual(store.watchedDependents(for: dependencyJob).map { $0.jobID }, ["22300"])
    }

    func testPrepareLogTailCreatesSessionForDetectedStdoutPath() async {
        let watchedJob = WatchedJob(
            clusterID: betaClusterID,
            jobID: "52001",
            jobName: "train-model",
            owner: "owner-b",
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
                    "cluster-beta:52001": .success(
                        JobLogPaths(
                            stdoutPath: "/logs/train-model-52001.out",
                            stderrPath: "/logs/train-model-52001.err"
                        )
                    )
                ]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-b",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        let opened = await store.prepareLogTail(for: watchedJob)

        XCTAssertTrue(opened)
        XCTAssertEqual(store.activeLogTail?.jobID, "52001")
        XCTAssertEqual(store.activeLogTail?.preferredStream, .stdout)
        XCTAssertEqual(store.activeLogTail?.paths.stderrPath, "/logs/train-model-52001.err")
    }

    func testPrepareLaunchCommandCreatesSessionPreferringBatchScript() async {
        let watchedJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "41001",
            jobName: "prepare-data",
            owner: "owner-a",
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
                launchDetailsResults: [
                    "cluster-alpha:41001": .success(
                        JobLaunchDetails(
                            commandText: "/opt/jobs/run-train.sh --epochs 3",
                            batchScriptText: "#!/bin/bash\npython train.py --epochs 3",
                            workDirectory: "/home/owner-a/project"
                        )
                    )
                ]
            ),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 500) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [watchedJob],
                reachabilityByCluster: [:]
            )
        )

        let opened = await store.prepareLaunchCommand(for: watchedJob)

        XCTAssertTrue(opened)
        XCTAssertEqual(store.activeLaunchCommand?.jobID, "41001")
        XCTAssertEqual(store.activeLaunchCommand?.preferredMode, .batchScript)
        XCTAssertEqual(store.activeLaunchCommand?.details.workDirectory, "/home/owner-a/project")
        XCTAssertEqual(
            store.activeLaunchCommand?.details.content(for: .batchScript),
            "#!/bin/bash\npython train.py --epochs 3"
        )
    }

    func testCancelCurrentJobCallsSlurmClient() async {
        let job = CurrentJob(
            clusterID: alphaClusterID,
            jobID: "50100",
            jobName: "train-model",
            owner: "owner-a",
            state: .running,
            rawState: "RUNNING"
        )

        let client = MockSlurmClient(
            currentResults: [alphaClusterID: .success([])],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: client,
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [],
                reachabilityByCluster: [:]
            )
        )

        let cancelled = await store.cancel(job: job)
        let cancelRequests = await client.cancelRequests()

        XCTAssertTrue(cancelled)
        XCTAssertEqual(cancelRequests, ["cluster-alpha:50100"])
    }

    func testCancelWatchedJobSkipsStaleEntries() async {
        let job = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "50101",
            jobName: "pending-work",
            owner: "owner-a",
            state: .pending,
            rawState: "PENDING",
            firstSeenAt: Date(timeIntervalSince1970: 100),
            lastUpdatedAt: Date(timeIntervalSince1970: 120),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120),
            isStale: true
        )

        let client = MockSlurmClient(
            currentResults: [:],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: client,
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [job],
                reachabilityByCluster: [:]
            )
        )

        let cancelled = await store.cancel(job: job)
        let cancelRequests = await client.cancelRequests()

        XCTAssertFalse(cancelled)
        XCTAssertEqual(cancelRequests, [])
    }

    func testCancelWatchedJobGroupBatchesIDsPerCluster() async {
        let jobs = [
            WatchedJob(
                clusterID: alphaClusterID,
                jobID: "50102",
                jobName: "train-a",
                owner: "owner-a",
                state: .running,
                rawState: "RUNNING",
                firstSeenAt: Date(timeIntervalSince1970: 100),
                lastUpdatedAt: Date(timeIntervalSince1970: 120),
                lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120)
            ),
            WatchedJob(
                clusterID: alphaClusterID,
                jobID: "50103",
                jobName: "train-b",
                owner: "owner-a",
                state: .pending,
                rawState: "PENDING",
                firstSeenAt: Date(timeIntervalSince1970: 100),
                lastUpdatedAt: Date(timeIntervalSince1970: 120),
                lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120)
            ),
            WatchedJob(
                clusterID: alphaClusterID,
                jobID: "50104",
                jobName: "train-c",
                owner: "owner-a",
                state: .completed,
                rawState: "COMPLETED",
                notificationSent: true,
                firstSeenAt: Date(timeIntervalSince1970: 100),
                lastUpdatedAt: Date(timeIntervalSince1970: 120),
                lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 120)
            )
        ]

        let client = MockSlurmClient(
            currentResults: [alphaClusterID: .success([])],
            historicalResults: [:]
        )

        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: client,
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: jobs,
                reachabilityByCluster: [:]
            )
        )

        let cancelled = await store.cancel(jobs: jobs)
        let cancelRequests = await client.cancelRequests()

        XCTAssertTrue(cancelled)
        XCTAssertEqual(cancelRequests, ["cluster-alpha:50102,50103"])
    }

    #if DEBUG
    func testDemoModeUsesGenericFixtureData() async {
        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: MockSlurmClient(currentResults: [:], historicalResults: [:]),
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 1_000_000) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [],
                reachabilityByCluster: [:],
                isDemoDataEnabled: true
            )
        )

        XCTAssertTrue(store.isDemoDataEnabled)
        XCTAssertEqual(store.displayClusters.map(\.displayName), ["Demo Cluster Alpha", "Demo Cluster Beta", "Demo Cluster Gamma"])
        XCTAssertTrue(store.visibleCurrentJobs.contains(where: { $0.jobName == "evaluate-suite-a2" }))
        XCTAssertTrue(store.groupedWatchedJobs.flatMap(\.groups).flatMap(\.jobs).contains(where: { $0.jobName == "sync-catalog-g1" }))
    }

    func testDemoModeCancelDoesNotCallRealSlurmClient() async throws {
        let client = MockSlurmClient(currentResults: [:], historicalResults: [:])
        let store = JobStore(
            persistence: InMemoryPersistenceStore(),
            slurmClient: client,
            notificationManager: MockNotificationManager(),
            pollingCoordinator: PollingCoordinator(),
            nowProvider: { Date(timeIntervalSince1970: 1_000_000) },
            initialState: PersistedAppState(
                clusters: sampleClusters(),
                globalUsernameFilter: "owner-a",
                pollIntervalSeconds: 30,
                watchedJobs: [],
                reachabilityByCluster: [:],
                isDemoDataEnabled: true
            )
        )

        let job = try XCTUnwrap(store.groupedWatchedJobs.flatMap(\.groups).flatMap(\.jobs).first(where: { $0.jobID == "910101" }))

        let cancelled = await store.cancel(job: job)
        let cancelRequests = await client.cancelRequests()

        XCTAssertTrue(cancelled)
        XCTAssertTrue(cancelRequests.isEmpty)
        XCTAssertTrue(store.groupedWatchedJobs.flatMap(\.groups).flatMap(\.jobs).contains(where: { $0.jobID == "910101" && $0.state == .cancelled }))
    }
    #endif

    private func sampleClusters() -> [ClusterConfig] {
        [
            ClusterConfig(id: alphaClusterID, displayName: "Cluster Alpha", sshAlias: "alpha-login"),
            ClusterConfig(id: betaClusterID, displayName: "Cluster Beta", sshAlias: "beta-login")
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
    private let clusterLoadResults: [ClusterID: Result<ClusterLoadSnapshot, Error>]
    private let logPathResults: [String: Result<JobLogPaths?, Error>]
    private let launchDetailsResults: [String: Result<JobLaunchDetails?, Error>]
    private let tailResults: [String: Result<String, Error>]
    private let cancelResults: [String: Result<Void, Error>]
    private let currentDelaysNanoseconds: [ClusterID: UInt64]
    private let historicalDelaysNanoseconds: [String: UInt64]
    private var recordedCancelRequests: [String] = []

    init(
        currentResults: [ClusterID: Result<[CurrentJob], Error>],
        historicalResults: [String: Result<JobSnapshot?, Error>],
        clusterLoadResults: [ClusterID: Result<ClusterLoadSnapshot, Error>] = [:],
        logPathResults: [String: Result<JobLogPaths?, Error>] = [:],
        launchDetailsResults: [String: Result<JobLaunchDetails?, Error>] = [:],
        tailResults: [String: Result<String, Error>] = [:],
        cancelResults: [String: Result<Void, Error>] = [:],
        currentDelaysNanoseconds: [ClusterID: UInt64] = [:],
        historicalDelaysNanoseconds: [String: UInt64] = [:]
    ) {
        self.currentResults = currentResults
        self.historicalResults = historicalResults
        self.clusterLoadResults = clusterLoadResults
        self.logPathResults = logPathResults
        self.launchDetailsResults = launchDetailsResults
        self.tailResults = tailResults
        self.cancelResults = cancelResults
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

    func fetchClusterLoad(for cluster: ClusterConfig, username: String, currentJobs: [CurrentJob]) async throws -> ClusterLoadSnapshot {
        if let result = clusterLoadResults[cluster.id] {
            return try result.get()
        }
        return .unknown()
    }

    func fetchLogPaths(for cluster: ClusterConfig, jobID: String) async throws -> JobLogPaths? {
        if let result = logPathResults["\(cluster.id.rawValue):\(jobID)"] {
            return try result.get()
        }
        return nil
    }

    func fetchLaunchDetails(for cluster: ClusterConfig, jobID: String) async throws -> JobLaunchDetails? {
        if let result = launchDetailsResults["\(cluster.id.rawValue):\(jobID)"] {
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

    func cancelJob(for cluster: ClusterConfig, jobID: String) async throws {
        try await cancelJobs(for: cluster, jobIDs: [jobID])
    }

    func cancelJobs(for cluster: ClusterConfig, jobIDs: [String]) async throws {
        let key = "\(cluster.id.rawValue):\(jobIDs.joined(separator: ","))"
        recordedCancelRequests.append(key)
        if let result = cancelResults[key] {
            try result.get()
        }
    }

    func cancelRequests() -> [String] {
        recordedCancelRequests
    }
}

private final class MockNotificationManager: NotificationManaging {
    private(set) var sentJobIDs: [String] = []

    func requestAuthorizationIfNeeded() async {}

    func sendTerminalNotification(for job: WatchedJob, clusterName: String) async {
        sentJobIDs.append(job.jobID)
    }
}
