import Foundation
import XCTest
@testable import ClusterWatchCore

final class PersistenceStoreTests: XCTestCase {
    private let alphaClusterID = ClusterID(rawValue: "cluster-alpha")

    func testRoundTripPersistsClustersAndWatchedJobs() async throws {
        let temporaryDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let fileURL = temporaryDirectory.appendingPathComponent("state.json")

        let store = PersistenceStore(fileURL: fileURL)
        let state = PersistedAppState(
            clusters: [
                ClusterConfig(id: alphaClusterID, displayName: "Cluster Alpha", sshAlias: "alpha-login", sshUsername: "", isEnabled: true, usernameOverride: "test-user")
            ],
            globalUsernameFilter: "test-user",
            pollIntervalSeconds: 45,
            watchedJobs: [
                WatchedJob(
                    clusterID: alphaClusterID,
                    jobID: "12345",
                    jobName: "train-model",
                    owner: "test-user",
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
                alphaClusterID.rawValue: ClusterReachabilityState(status: .reachable, lastSuccessfulRefresh: Date(timeIntervalSince1970: 250), lastErrorMessage: nil)
            ]
        )

        try await store.save(state)
        let loaded = await store.load()

        XCTAssertEqual(loaded, state)
    }

    func testPersistedStateDecodesStringBackedClusterIDs() throws {
        let json = """
        {
          "clusters": [
            {
              "displayName": "Cluster Alpha",
              "id": "cluster-alpha",
              "isEnabled": true,
              "sshAlias": "alpha-login",
              "sshUsername": "",
              "usernameOverride": "owner-a"
            },
            {
              "displayName": "Cluster Beta",
              "id": "cluster-beta",
              "isEnabled": true,
              "sshAlias": "beta-login",
              "sshUsername": "",
              "usernameOverride": ""
            }
          ],
          "globalUsernameFilter": "test-user",
          "pollIntervalSeconds": 30,
          "reachabilityByCluster": {
            "cluster-alpha": {
              "lastErrorMessage": null,
              "lastSuccessfulRefresh": "2026-03-27T09:00:00Z",
              "status": "reachable"
            }
          },
          "watchedJobs": []
        }
        """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let state = try decoder.decode(PersistedAppState.self, from: Data(json.utf8))

        XCTAssertEqual(state.clusters.map(\.id.rawValue), ["cluster-alpha", "cluster-beta"])
        XCTAssertEqual(state.clusters.first?.usernameOverride, "owner-a")
        XCTAssertEqual(state.reachabilityByCluster["cluster-alpha"]?.status, .reachable)
    }
}
