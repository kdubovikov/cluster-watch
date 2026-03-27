import XCTest
@testable import ClusterWatchCore

final class SlurmParsingTests: XCTestCase {
    private let alphaClusterID = ClusterID(rawValue: "cluster-alpha")

    func testParseCurrentJobsExtractsTimingFields() {
        let output = """
        12345|test-user|RUNNING|train-model|2026-03-27T09:00:00|2026-03-27T09:10:00|01:25:00|NULL|None
        12346|test-user|PENDING|prepare-data|2026-03-27T10:00:00|N/A|00:15:00|afterok:12345|Dependency
        """

        let jobs = SlurmParsing.parseCurrentJobs(output: output, clusterID: alphaClusterID)

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
        12345|test-user|COMPLETED|train-model|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        12345.batch|test-user|COMPLETED|batch|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        12345.extern|test-user|COMPLETED|extern|2026-03-27T09:00:00|2026-03-27T09:10:00|2026-03-27T11:00:00|01:50:00
        """

        let snapshot = SlurmParsing.parseHistoricalJob(output: output, clusterID: alphaClusterID, requestedJobID: "12345")

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

    func testParseScontrolLogPathsExtractsExpandedStdoutAndStderr() {
        let output = """
        JobId=20001 JobName=train-model
           UserId=test-user(1001) GroupId=test-group(1001) MCS_label=N/A
           WorkDir=/home/test-user/project
           StdErr=/home/test-user/project/logs/train-model-20001.err
           StdIn=/dev/null
           StdOut=/home/test-user/project/logs/train-model-20001.out
        """

        let logPaths = SlurmParsing.parseScontrolLogPaths(output: output)

        XCTAssertEqual(logPaths?.stdoutPath, "/home/test-user/project/logs/train-model-20001.out")
        XCTAssertEqual(logPaths?.stderrPath, "/home/test-user/project/logs/train-model-20001.err")
        XCTAssertEqual(logPaths?.workDirectory, "/home/test-user/project")
    }

    func testParseHistoricalLogPathsPrefersPrimaryRow() {
        let output = """
        20001|/logs/%x-%j.out|/logs/%x-%j.err|/workdir
        20001.batch|||
        20001.extern|||
        """

        let logPaths = SlurmParsing.parseHistoricalLogPaths(output: output, requestedJobID: "20001")

        XCTAssertEqual(logPaths?.stdoutPath, "/logs/%x-%j.out")
        XCTAssertEqual(logPaths?.stderrPath, "/logs/%x-%j.err")
        XCTAssertEqual(logPaths?.workDirectory, "/workdir")
    }
}
