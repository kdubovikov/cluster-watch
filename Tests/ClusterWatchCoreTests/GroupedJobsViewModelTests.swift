import Foundation
import XCTest
@testable import ClusterWatchCore

final class GroupedJobsViewModelTests: XCTestCase {
    private let camdID = ClusterID(rawValue: "camd")
    private let csccID = ClusterID(rawValue: "cscc")

    func testBucketsAndOrderingPreferRunningThenRecent() {
        let calendar = Calendar(identifier: .gregorian)
        let referenceDate = calendar.date(from: DateComponents(year: 2026, month: 3, day: 27, hour: 12, minute: 0))!

        let runningJob = WatchedJob(
            clusterID: camdID,
            jobID: "300",
            jobName: "running",
            owner: "kirill",
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
            clusterID: camdID,
            jobID: "100",
            jobName: "completed",
            owner: "kirill",
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
            clusterID: csccID,
            jobID: "200",
            jobName: "pending",
            owner: "kirill",
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
            clusterID: camdID,
            jobID: "38071",
            jobName: "pes2o-filter-q35-7n-r5",
            owner: "salem.lahlou",
            state: .running,
            rawState: "RUNNING",
            submitTime: Date(timeIntervalSince1970: 100),
            startTime: Date(timeIntervalSince1970: 120),
            elapsedSeconds: 120
        )

        let child = CurrentJob(
            clusterID: camdID,
            jobID: "38072",
            jobName: "pes2o-hn-q35-7n-r4",
            owner: "salem.lahlou",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:38071(unfulfilled)",
            dependencyJobIDs: ["38071"],
            dependencyIsActive: true
        )

        let grandchild = CurrentJob(
            clusterID: camdID,
            jobID: "38073",
            jobName: "assemble-norag-all",
            owner: "salem.lahlou",
            state: .pending,
            rawState: "PENDING",
            submitTime: Date(timeIntervalSince1970: 100),
            pendingReason: "Dependency",
            dependencyExpression: "afterok:38072(unfulfilled)",
            dependencyJobIDs: ["38072"],
            dependencyIsActive: true
        )

        let standalone = CurrentJob(
            clusterID: csccID,
            jobID: "148463",
            jobName: "tb_gsm8k_t10",
            owner: "kirill.dubovikov",
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
        XCTAssertEqual(groups[0].rows.map(\.job.jobID), ["148463"])
        XCTAssertEqual(groups[1].rows.map(\.job.jobID), ["38071", "38072", "38073"])
        XCTAssertEqual(groups[1].rows.map(\.depth), [0, 1, 2])
        XCTAssertEqual(groups[1].rows.map(\.parentJobID), [nil, root.id, child.id])
    }
}
