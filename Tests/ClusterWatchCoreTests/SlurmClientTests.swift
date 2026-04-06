import XCTest
@testable import ClusterWatchCore

final class SlurmClientTests: XCTestCase {
    func testTimestampOffsetUsesRemoteUTCOffsetToTranslateClusterTimes() {
        let localTimeZone = TimeZone(secondsFromGMT: 3 * 3_600)!
        let now = Date(timeIntervalSince1970: 1_774_810_000)

        let offset = SlurmClient.timestampOffset(
            fromRemoteUTCOffset: "+0000",
            now: now,
            localTimeZone: localTimeZone
        )

        XCTAssertEqual(offset, 10_800)
    }

    func testTimestampOffsetHandlesNegativeRemoteOffset() {
        let localTimeZone = TimeZone(secondsFromGMT: 3 * 3_600)!
        let now = Date(timeIntervalSince1970: 1_774_810_000)

        let offset = SlurmClient.timestampOffset(
            fromRemoteUTCOffset: "-0700",
            now: now,
            localTimeZone: localTimeZone
        )

        XCTAssertEqual(offset, 36_000)
    }

    func testTimestampOffsetRejectsInvalidValues() {
        XCTAssertNil(
            SlurmClient.timestampOffset(
                fromRemoteUTCOffset: "UTC",
                now: Date()
            )
        )
    }

    func testClusterLoadSupportBuildsAccessibleScopeAndHeadroom() {
        let userRows = [
            ClusterLoadAssociationUserRow(account: "research", partition: nil, qosValues: ["gpu"])
        ]
        let userAssocRows = [
            ClusterLoadAssociationLimitRow(
                account: "research",
                user: nil,
                partition: nil,
                qosValues: ["gpu"],
                maxJobs: 10,
                maxSubmit: 8,
                groupGPUCount: nil,
                maxGPUCount: nil
            )
        ]
        let accountAssocRows: [ClusterLoadAssociationLimitRow] = []
        let qosRows = [
            ClusterLoadQoSRow(
                name: "gpu",
                maxJobsPerUser: 6,
                maxSubmitPerUser: 7,
                maxGPUPerUser: 12,
                maxGPUPerAccount: nil,
                groupGPUCount: nil
            )
        ]
        let partitions = [
            ClusterLoadPartitionRow(name: "gpu", allowAccounts: ["research"]),
            ClusterLoadPartitionRow(name: "debug", allowAccounts: ["other"])
        ]

        let discovery = ClusterLoadSupport.makeDiscoveryContext(
            userRows: userRows,
            userAssocRows: userAssocRows,
            accountAssocRows: accountAssocRows,
            qosRows: qosRows,
            partitions: partitions,
            configOutput: "AccountingStorageEnforce = associations,limits,qos"
        )

        let currentJobs = [
            CurrentJob(
                clusterID: ClusterID(rawValue: "alpha"),
                jobID: "1",
                jobName: "a",
                owner: "u",
                state: .running,
                rawState: "RUNNING",
                qosName: "gpu",
                gpuCount: 2,
                nodeCount: 1
            ),
            CurrentJob(clusterID: ClusterID(rawValue: "alpha"), jobID: "2", jobName: "b", owner: "u", state: .pending, rawState: "PENDING")
        ]

        let resources = ClusterLoadResourceSummary(
            freeCPUCount: 48,
            totalCPUCount: 64,
            freeGPUCount: 12,
            totalGPUCount: 16,
            freeNodeCount: 3,
            totalNodeCount: 4
        )

        let snapshot = ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: currentJobs,
            queueSummary: ClusterQueueSummary(totalJobCount: 14, pendingJobCount: 6),
            resourceSummary: resources,
            userRunningGPUByQOS: ["gpu": 2],
            accountRunningGPUByQOS: [:],
            scopedRunningGPUCount: 0,
            scopedRunningNodeCount: 0,
            configuredGPUCap: nil,
            configuredNodeCap: nil,
            now: Date(timeIntervalSince1970: 100),
            message: nil
        )

        XCTAssertEqual(discovery.accessiblePartitions, ["gpu"])
        XCTAssertTrue(discovery.hasPartitionMetadata)
        XCTAssertEqual(snapshot.jobHeadroom, 5)
        XCTAssertEqual(snapshot.jobCount, 14)
        XCTAssertEqual(snapshot.pendingJobCount, 6)
        XCTAssertEqual(snapshot.primaryFreeResourceText, "Free 10 GPU")
        XCTAssertEqual(snapshot.scopedGPUDescription, "All QoS")
        XCTAssertEqual(snapshot.qosSummaryText, "QoS gpu 10/12")
        XCTAssertEqual(snapshot.summaryText, "Jobs 14 • Free 10 GPU • Headroom 5 jobs")
        XCTAssertEqual(snapshot.level, .busy)
    }

    func testClusterLoadSupportAggregatesAcrossMultipleQoSLanes() {
        let discovery = ClusterLoadDiscoveryContext(
            accessiblePartitions: ["gpu"],
            accessibleAccounts: ["research"],
            preferredQOSOrder: ["gtqos", "stqos", "xdqos"],
            userGPUCapByQOS: ["gtqos": 12, "stqos": 4, "xdqos": 64],
            accountGPUCapByQOS: [:],
            maxRunningJobs: nil,
            maxSubmittedJobs: nil,
            limitsEnforced: true,
            hasPartitionMetadata: true
        )

        let currentJobs = [
            CurrentJob(
                clusterID: ClusterID(rawValue: "alpha"),
                jobID: "1",
                jobName: "a",
                owner: "u",
                state: .running,
                rawState: "RUNNING",
                qosName: "gtqos",
                gpuCount: 1,
                nodeCount: 1
            )
        ]

        let resources = ClusterLoadResourceSummary(
            freeCPUCount: 128,
            totalCPUCount: 256,
            freeGPUCount: 412,
            totalGPUCount: 1072,
            freeNodeCount: 55,
            totalNodeCount: 134
        )

        let snapshot = ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: currentJobs,
            queueSummary: ClusterQueueSummary(totalJobCount: 56, pendingJobCount: 0),
            resourceSummary: resources,
            userRunningGPUByQOS: ["gtqos": 1],
            accountRunningGPUByQOS: [:],
            scopedRunningGPUCount: 0,
            scopedRunningNodeCount: 0,
            configuredGPUCap: nil,
            configuredNodeCap: nil,
            now: Date(timeIntervalSince1970: 100),
            message: nil
        )

        XCTAssertEqual(snapshot.scopedFreeGPUCount, 79)
        XCTAssertEqual(snapshot.scopedTotalGPUCount, 80)
        XCTAssertEqual(snapshot.summaryText, "Jobs 56 • Free 79 GPU")
        XCTAssertEqual(snapshot.qosSummaryText, "QoS gtqos 11/12 • stqos 4/4 • xdqos 64/64")
        XCTAssertEqual(snapshot.detailResourceText, "Cluster GPU 412/1072 free • CPU 128/256 free • Nodes 55/134 free")
    }

    func testClusterLoadSupportLabelsUnscopedCapacityAsClusterFree() {
        let discovery = ClusterLoadDiscoveryContext(
            accessiblePartitions: ["gpuhigh2"],
            accessibleAccounts: ["research"],
            preferredQOSOrder: ["normal"],
            userGPUCapByQOS: [:],
            accountGPUCapByQOS: [:],
            maxRunningJobs: 40,
            maxSubmittedJobs: 100,
            limitsEnforced: true,
            hasPartitionMetadata: true
        )

        let resources = ClusterLoadResourceSummary(
            freeCPUCount: 926,
            totalCPUCount: 1024,
            freeGPUCount: 24,
            totalGPUCount: 64,
            freeNodeCount: 3,
            totalNodeCount: 8
        )

        let snapshot = ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: [],
            queueSummary: ClusterQueueSummary(totalJobCount: 53, pendingJobCount: 51),
            resourceSummary: resources,
            userRunningGPUByQOS: [:],
            accountRunningGPUByQOS: [:],
            scopedRunningGPUCount: 0,
            scopedRunningNodeCount: 0,
            configuredGPUCap: nil,
            configuredNodeCap: nil,
            now: Date(timeIntervalSince1970: 100),
            message: nil
        )

        XCTAssertEqual(snapshot.primaryFreeResourceText, "Cluster free 24 GPU")
        XCTAssertEqual(snapshot.summaryText, "Jobs 53 • Cluster free 24 GPU • Headroom 40 jobs")
    }

    func testClusterLoadSupportConfiguredGPUCapLimitsScopedHeadroom() {
        let discovery = ClusterLoadDiscoveryContext(
            accessiblePartitions: ["gpuhigh2"],
            accessibleAccounts: ["research"],
            preferredQOSOrder: ["normal"],
            userGPUCapByQOS: [:],
            accountGPUCapByQOS: [:],
            maxRunningJobs: 40,
            maxSubmittedJobs: 100,
            limitsEnforced: true,
            hasPartitionMetadata: true
        )

        let resources = ClusterLoadResourceSummary(
            freeCPUCount: 926,
            totalCPUCount: 1024,
            freeGPUCount: 24,
            totalGPUCount: 64,
            freeNodeCount: 3,
            totalNodeCount: 8
        )

        let snapshot = ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: [],
            queueSummary: ClusterQueueSummary(totalJobCount: 53, pendingJobCount: 51),
            resourceSummary: resources,
            userRunningGPUByQOS: [:],
            accountRunningGPUByQOS: [:],
            scopedRunningGPUCount: 40,
            scopedRunningNodeCount: 5,
            configuredGPUCap: 40,
            configuredNodeCap: nil,
            now: Date(timeIntervalSince1970: 100),
            message: nil
        )

        XCTAssertEqual(snapshot.primaryFreeResourceText, "Free 0 GPU")
        XCTAssertEqual(snapshot.scopedDetailText, "Configured GPU cap GPU 0/40 free")
        XCTAssertEqual(snapshot.summaryText, "Jobs 53 • Free 0 GPU • Headroom 40 jobs")
        XCTAssertEqual(snapshot.level, .full)
    }

    func testClusterLoadSupportConfiguredNodeCapCanDeriveScopedGPUHeadroom() {
        let discovery = ClusterLoadDiscoveryContext(
            accessiblePartitions: ["gpuhigh2"],
            accessibleAccounts: ["research"],
            preferredQOSOrder: ["normal"],
            userGPUCapByQOS: [:],
            accountGPUCapByQOS: [:],
            maxRunningJobs: nil,
            maxSubmittedJobs: nil,
            limitsEnforced: true,
            hasPartitionMetadata: true
        )

        let resources = ClusterLoadResourceSummary(
            freeCPUCount: 926,
            totalCPUCount: 1024,
            freeGPUCount: 24,
            totalGPUCount: 64,
            freeNodeCount: 3,
            totalNodeCount: 8
        )

        let snapshot = ClusterLoadSupport.makeSnapshot(
            discovery: discovery,
            currentJobs: [],
            queueSummary: ClusterQueueSummary(totalJobCount: 53, pendingJobCount: 51),
            resourceSummary: resources,
            userRunningGPUByQOS: [:],
            accountRunningGPUByQOS: [:],
            scopedRunningGPUCount: 40,
            scopedRunningNodeCount: 5,
            configuredGPUCap: nil,
            configuredNodeCap: 5,
            now: Date(timeIntervalSince1970: 100),
            message: nil
        )

        XCTAssertEqual(snapshot.scopedFreeGPUCount, 0)
        XCTAssertEqual(snapshot.scopedTotalGPUCount, 40)
        XCTAssertEqual(snapshot.scopedFreeNodeCount, 0)
        XCTAssertEqual(snapshot.scopedTotalNodeCount, 5)
        XCTAssertEqual(snapshot.scopedDetailText, "Configured node cap GPU 0/40 free • Configured node cap nodes 0/5 free")
    }

    func testRunningGPUUsageByQOSMultipliesPerNodeGPUByNodeCount() {
        let jobs = [
            CurrentJob(
                clusterID: ClusterID(rawValue: "alpha"),
                jobID: "42",
                jobName: "distributed",
                owner: "u",
                state: .running,
                rawState: "RUNNING",
                qosName: "normal",
                gpuCount: 8,
                nodeCount: 3
            )
        ]

        XCTAssertEqual(ClusterLoadSupport.runningGPUUsageByQOS(jobs), ["normal": 24])
    }

    func testClusterLoadSupportParsesNodeTRESAndSelectsGPUTotals() {
        let output = """
        NodeName=node-a Partitions=gpu CfgTRES=cpu=128,mem=512000M,gres/gpu=8,gres/gpu:a100=8 AllocTRES=cpu=64,mem=128000M,gres/gpu=2,gres/gpu:a100=2
        NodeName=node-b Partitions=gpu,debug CfgTRES=cpu=64,mem=256000M,gres/gpu=4,gres/gpu:a100=4 AllocTRES=cpu=64,mem=256000M,gres/gpu=4,gres/gpu:a100=4
        """

        let nodes = ClusterLoadSupport.parseNodeRows(output)
        let summary = ClusterLoadSupport.makeResourceSummary(nodes: nodes, accessiblePartitions: ["gpu"])

        XCTAssertEqual(summary.totalNodeCount, 2)
        XCTAssertEqual(summary.totalCPUCount, 192)
        XCTAssertEqual(summary.freeCPUCount, 64)
        XCTAssertEqual(summary.totalGPUCount, 12)
        XCTAssertEqual(summary.freeGPUCount, 6)
        XCTAssertEqual(summary.freeNodeCount, 1)
    }
}
