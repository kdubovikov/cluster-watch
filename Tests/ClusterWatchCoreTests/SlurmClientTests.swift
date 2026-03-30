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
}
