import XCTest
@testable import ClusterWatchCore

final class SlurmParsingTests: XCTestCase {
    func testParseCurrentJobsExtractsTimingFields() {
        let output = """
        12345|kirill|RUNNING|train-model|2026-03-27T09:00:00|2026-03-27T09:10:00|01:25:00|NULL|None
        12346|kirill|PENDING|preprocess|2026-03-27T10:00:00|N/A|00:15:00|afterok:12345|Dependency
        """

        let jobs = SlurmParsing.parseCurrentJobs(output: output, clusterID: .camd)

        XCTAssertEqual(jobs.count, 2)
        XCTAssertEqual(jobs[0].jobID, "12345")
        XCTAssertEqual(jobs[0].state, .running)
        XCTAssertEqual(jobs[0].elapsedSeconds, 5_100)
        XCTAssertEqual(jobs[1].state, .pending)
        XCTAssertNil(jobs[1].startTime)
        XCTAssertEqual(jobs[1].dependencyExpression, "afterok:12345")
        XCTAssertEqual(jobs[1].dependencyJobIDs, ["12345"])
        XCTAssertEqual(jobs[1].dependencyStatus, .waiting)
    }

    func testParseHistoricalJobPrefersPrimaryRowOverSteps() {
        let output = """
        12345|kirill|COMPLETED|train-model|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        12345.batch|kirill|COMPLETED|batch|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        12345.extern|kirill|COMPLETED|extern|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        """

        let snapshot = SlurmParsing.parseHistoricalJob(output: output, clusterID: .camd, requestedJobID: "12345")

        XCTAssertEqual(snapshot?.jobID, "12345")
        XCTAssertEqual(snapshot?.state, .completed)
        XCTAssertEqual(snapshot?.jobName, "train-model")
    }

    func testStateNormalizationHandlesTerminalVariants() {
        XCTAssertEqual(NormalizedJobState(rawSlurmState: "CANCELLED by 1000"), .cancelled)
        XCTAssertEqual(NormalizedJobState(rawSlurmState: "OUT_OF_MEMORY"), .outOfMemory)
        XCTAssertEqual(NormalizedJobState(rawSlurmState: "NODE_FAIL"), .nodeFail)
        XCTAssertEqual(NormalizedJobState(rawSlurmState: "PREEMPTED"), .preempted)
    }

    func testDependencyParserExtractsMultipleJobIDs() {
        let expression = "afterok:12345:12346,afterany:22300"

        XCTAssertEqual(SlurmDependencyParser.parseJobIDs(from: expression), ["12345", "12346", "22300"])
    }
}
