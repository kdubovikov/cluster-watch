import Foundation
import XCTest
@testable import ClusterWatchCore

final class GroupedJobsViewModelTests: XCTestCase {
    private let alphaClusterID = ClusterID(rawValue: "cluster-alpha")
    private let betaClusterID = ClusterID(rawValue: "cluster-beta")

    func testBucketsAndOrderingPreferRunningThenRecent() {
        let calendar = Calendar(identifier: .gregorian)
        let referenceDate = calendar.date(from: DateComponents(year: 2026, month: 3, day: 27, hour: 12, minute: 0))!

        let runningJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "300",
            jobName: "running",
            owner: "test-user",
            state: .running,
            rawState: "RUNNING",
            submitTime: calendar.date(byAdding: .hour, value: -4, to: referenceDate),
            startTime: calendar.date(byAdding: .hour, value: -2, to: referenceDate),
            endTime: nil,
            elapsedSeconds: 7_200,
            firstSeenAt: calendar.date(byAdding: .hour, value: -4, to: referenceDate)!,
            lastUpdatedAt: calendar.date(byAdding: .minute, value: -5, to: referenceDate)!,
            lastSuccessfulRefreshAt: referenceDate,
            isStale: false
        )

        let completedJob = WatchedJob(
            clusterID: alphaClusterID,
            jobID: "100",
            jobName: "completed",
            owner: "test-user",
            state: .completed,
            rawState: "COMPLETED",
            submitTime: calendar.date(byAdding: .day, value: -1, to: referenceDate),
            startTime: calendar.date(byAdding: .day, value: -1, to: referenceDate),
            endTime: calendar.date(byAdding: .hour, value: -20, to: referenceDate),
            elapsedSeconds: 1_200,
            firstSeenAt: calendar.date(byAdding: .day, value: -1, to: referenceDate)!,
            lastUpdatedAt: calendar.date(byAdding: .hour, value: -20, to: referenceDate)!,
            lastSuccessfulRefreshAt: referenceDate,
            isStale: false
        )

        let pendingJob = WatchedJob(
            clusterID: betaClusterID,
            jobID: "200",
            jobName: "pending",
            owner: "test-user",
            state: .pending,
            rawState: "PENDING",
            submitTime: calendar.date(byAdding: .hour, value: -6, to: referenceDate),
            startTime: nil,
            endTime: nil,
            elapsedSeconds: nil,
            firstSeenAt: calendar.date(byAdding: .hour, value: -6, to: referenceDate)!,
            lastUpdatedAt: calendar.date(byAdding: .minute, value: -2, to: referenceDate)!,
            lastSuccessfulRefreshAt: referenceDate,
            isStale: false
        )

        let sections = GroupedJobsViewModel.sections(
            for: [completedJob, pendingJob, runningJob],
            referenceDate: referenceDate,
            calendar: calendar
        )

        XCTAssertEqual(sections.first?.bucket, .today)
        XCTAssertEqual(sections.first?.jobs.map(\.jobID), ["300", "200"])
        XCTAssertEqual(sections.last?.bucket, .yesterday)
        XCTAssertEqual(sections.last?.jobs.first?.jobID, "100")
    }

    func testCurrentGroupsNestDependencyChainInBrowseOrder() {
        let root = CurrentJob(
            clusterID: alphaClusterID,
            jobID: "41001",
            jobName: "prepare-data",
            owner: "owner-a",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 120),
            elapsedSeconds: 120
        )

        let child = CurrentJob(
            clusterID: alphaClusterID,
            jobID: "41002",
            jobName: "train-model",
            owner: "owner-a",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:41001(unfulfilled)",
            dependencyJobIDs: ["41001"],
            dependencyIsActive: true
        )

        let grandchild = CurrentJob(
            clusterID: alphaClusterID,
            jobID: "41003",
            jobName: "assemble-results",
            owner: "owner-a",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:41002(unfulfilled)",
            dependencyJobIDs: ["41002"],
            dependencyIsActive: true
        )

        let standalone = CurrentJob(
            clusterID: betaClusterID,
            jobID: "52001",
            jobName: "standalone-job",
            owner: "owner-b",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 200),
            startTime: Date(timeIntervalSince1970: 220),
            elapsedSeconds: 60
        )

        let groups = GroupedJobsViewModel.currentGroups(
            for: [child, standalone, grandchild, root]
        )

        XCTAssertEqual(groups.count, 2)
        XCTAssertEqual(groups[0].rows.map(\.job.jobID), ["52001"])
        XCTAssertEqual(groups[1].rows.map(\.job.jobID), ["41001", "41002", "41003"])
        XCTAssertEqual(groups[1].rows.map(\.depth), [0, 1, 2])
        XCTAssertEqual(groups[1].rows.map(\.parentJobID), [nil, root.id, child.id])
    }
}
