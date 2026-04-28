import XCTest
@testable import ClusterWatchCore

final class JobFormattingTests: XCTestCase {
    func testFormatsLongPythonCommandIntoWrappedArguments() {
        let command = """
        python scripts/assemble_dataset.py output.dir=/tmp/output manifest.path=/tmp/output/manifest.json rag.path=/tmp/output/rag.parquet llm.path=/tmp/output/llm.parquet overwrite=true
        """

        let formatted = JobFormatting.formattedLaunchContent(command, mode: .command)

        XCTAssertEqual(
            formatted,
            """
            python scripts/assemble_dataset.py \\
              output.dir=/tmp/output \\
              manifest.path=/tmp/output/manifest.json \\
              rag.path=/tmp/output/rag.parquet \\
              llm.path=/tmp/output/llm.parquet \\
              overwrite=true
            """
        )
    }

    func testFormatsWrappedBatchScriptBodyButPreservesHeaders() {
        let script = """
        #!/bin/sh
        # This script was created by sbatch --wrap.

        python scripts/assemble_dataset.py output.dir=/tmp/output manifest.path=/tmp/output/manifest.json rag.path=/tmp/output/rag.parquet llm.path=/tmp/output/llm.parquet overwrite=true
        """

        let formatted = JobFormatting.formattedLaunchContent(script, mode: .batchScript)

        XCTAssertEqual(
            formatted,
            """
            #!/bin/sh
            # This script was created by sbatch --wrap.

            python scripts/assemble_dataset.py \\
              output.dir=/tmp/output \\
              manifest.path=/tmp/output/manifest.json \\
              rag.path=/tmp/output/rag.parquet \\
              llm.path=/tmp/output/llm.parquet \\
              overwrite=true
            """
        )
    }

    func testLeavesShortCommandsUnchanged() {
        let command = "python train.py --config configs/train.yaml"

        XCTAssertEqual(
            JobFormatting.formattedLaunchContent(command, mode: .command),
            command
        )
    }

    func testUnknownWatchedJobDoesNotFormatAsStillRunning() {
        let job = WatchedJob(
            clusterID: ClusterID(rawValue: "alpha"),
            jobID: "999",
            jobName: "missing",
            owner: "user",
            state: .unknown,
            rawState: "NOT_IN_QUEUE",
            startTime: Date(timeIntervalSince1970: 100),
            elapsedSeconds: 240,
            firstSeenAt: Date(timeIntervalSince1970: 0),
            lastUpdatedAt: Date(timeIntervalSince1970: 200),
            lastSuccessfulRefreshAt: Date(timeIntervalSince1970: 200)
        )

        XCTAssertEqual(
            JobFormatting.timingSummary(for: job, now: Date(timeIntervalSince1970: 500)),
            "Wait unknown • Last ran 4m 0s"
        )
    }
}
