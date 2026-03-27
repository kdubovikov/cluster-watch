import Foundation
import XCTest
@testable import ClusterWatchCore

final class PersistenceStoreTests: XCTestCase {
    private let camdID = ClusterID(rawValue: "camd")

    func testRoundTripPersistsClustersAndWatchedJobs() async throws {
        let temporaryDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let fileURL = temporaryDirectory.appendingPathComponent("state.json")

        let store = PersistenceStore(fileURL: fileURL)
        let state = PersistedAppState(
            clusters: [
                ClusterConfig(id: camdID, displayName: "CAMD", sshAlias: "camd1", sshUsername: "", isEnabled: true, usernameOverride: "kirill")
            ],
            globalUsernameFilter: "kirill",
            pollIntervalSeconds: 45,
            watchedJobs: [
                WatchedJob(
                    clusterID: camdID,
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
                camdID.rawValue: ClusterReachabilityState(status: .reachable, lastSuccessfulRefresh: Date(timeIntervalSince1970: 250), lastErrorMessage: nil)
            ]
        )

        try await store.save(state)
        let loaded = await store.load()

        XCTAssertEqual(loaded, state)
    }

    func testLegacyStateDecodesOldFixedClusterIDs() throws {
        let json = """
        {
          "clusters": [
            {
              "displayName": "CAMD",
              "id": "camd",
              "isEnabled": true,
              "sshAlias": "camd1",
              "sshUsername": "",
              "usernameOverride": "salem.lahlou"
            },
            {
              "displayName": "CSCC",
              "id": "cscc",
              "isEnabled": true,
              "sshAlias": "cscc",
              "sshUsername": "",
              "usernameOverride": ""
            }
          ],
          "globalUsernameFilter": "kirill",
          "pollIntervalSeconds": 30,
          "reachabilityByCluster": {
            "camd": {
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

        XCTAssertEqual(state.clusters.map(\.id.rawValue), ["camd", "cscc"])
        XCTAssertEqual(state.clusters.first?.usernameOverride, "salem.lahlou")
        XCTAssertEqual(state.reachabilityByCluster["camd"]?.status, .reachable)
    }
}
