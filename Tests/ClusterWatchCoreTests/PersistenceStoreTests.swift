import Foundation
import XCTest
@testable import ClusterWatchCore

final class PersistenceStoreTests: XCTestCase {
    func testRoundTripPersistsClustersAndWatchedJobs() async throws {
        let temporaryDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let fileURL = temporaryDirectory.appendingPathComponent("state.json")

        let store = PersistenceStore(fileURL: fileURL)
        let state = PersistedAppState(
            clusters: [
                ClusterConfig(id: .camd, displayName: "CAMD", sshAlias: "camd1", sshUsername: "", isEnabled: true, usernameOverride: "kirill")
            ],
            globalUsernameFilter: "kirill",
            pollIntervalSeconds: 45,
            watchedJobs: [
                WatchedJob(
                    clusterID: .camd,
                    jobID: "12345",
                    jobName: "train-model",
                    owner: "kirill",
                    state: .running,
                    rawState: "RUNNING",
                    submitTime: Date(timeIntervalSince1970: 100),
                    startTime: Date(timeIntervalSince1970: 200),
                    endTime: nil,
                    elapsedSeconds: 50,
                    firstSeenAt: Date(timeIntervalSince1970: 100),
                    lastUpdatedAt: Date(timeIntervalSince1970: 250),
                    lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 250),
                    isStale: false
                )
            ],
            reachabilityByCluster: [
                ClusterID.camd.rawValue: ClusterReachabilityState(status: .reachable, lastSuccessfulRefresh: Date(timeIntervalSince1970: 250), lastErrorMessage: nil)
            ]
        )

        try await store.save(state)
        let loaded = await store.load()

        XCTAssertEqual(loaded, state)
    }
}
