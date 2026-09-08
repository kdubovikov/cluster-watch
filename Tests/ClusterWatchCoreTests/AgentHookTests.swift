import Foundation
import XCTest
@testable import ClusterWatchCore

@MainActor
final class AgentHookTests: XCTestCase {
    private let cluster = ClusterConfig(id: ClusterID(rawValue: "alpha"), displayName: "Alpha", sshAlias: "alpha-ssh")
    private let destination = HookDestination(executable: "/usr/bin/true")
    private let time = Date(timeIntervalSince1970: 1000)

    private func snapshot(_ state: NormalizedJobState, id: String = "123") -> JobSnapshot {
        JobSnapshot(jobID: id, owner: "me", state: state, rawState: state.rawValue.uppercased(),
                    jobName: "job $(touch unwanted) `command`", submitTime: nil, startTime: nil,
                    endTime: nil, elapsedSeconds: nil)
    }

    func testInitialMatchTransitionsDeduplicationAndTerminalRetirement() throws {
        let store = TestHookStorage()
        let service = try AgentHookService(storage: store)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123", "123", "123_7"],
                                        states: [.running, .completed], destination: destination)
        let ids: Set<String> = [sub.id]
        XCTAssertEqual(sub.jobs.count, 2)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: ids, now: time)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: ids, now: time)
        XCTAssertEqual(service.state.deliveries.count, 1)
        XCTAssertNil(service.state.deliveries[0].event.previousState)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.pending)], subscriptionIDs: ids)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: ids)
        XCTAssertEqual(service.state.deliveries.count, 2)
        XCTAssertEqual(service.state.deliveries[1].event.previousState, .pending)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: ids)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: ids)
        XCTAssertEqual(service.state.deliveries.count, 3)
        XCTAssertFalse(service.state.subscriptions[0].completed)
        try service.observe(cluster: cluster, snapshots: ["123_7": snapshot(.failed, id: "123_7")], subscriptionIDs: ids)
        XCTAssertTrue(service.state.subscriptions[0].completed)
        XCTAssertEqual(service.state.deliveries.count, 3, "Unselected terminal still retires the job")
    }

    func testMultipleSubscribersAndRegistrationDuringPoll() throws {
        let service = try AgentHookService(storage: TestHookStorage())
        let first = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        let pollIDs = service.activeIDs(clusterID: cluster.id)
        let second = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: pollIDs)
        XCTAssertEqual(service.state.deliveries.map(\.event.subscriptionID), [first.id])
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)],
                            subscriptionIDs: service.activeIDs(clusterID: cluster.id))
        XCTAssertEqual(service.state.deliveries.map(\.event.subscriptionID), [first.id, second.id])
    }

    func testMissingObservationsAndPauseNeverInventTransitions() throws {
        let service = try AgentHookService(storage: TestHookStorage())
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running, .unknown], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id])
        try service.observe(cluster: cluster, snapshots: [:], subscriptionIDs: [sub.id])
        XCTAssertEqual(service.state.subscriptions[0].jobs[0].lastState, .running)
        XCTAssertEqual(service.state.deliveries.count, 1)
        var disabled = cluster
        disabled.isEnabled = false
        try service.updateClusters([disabled], demo: false)
        XCTAssertTrue(service.activeIDs(clusterID: cluster.id).isEmpty)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.unknown)], subscriptionIDs: [sub.id])
        XCTAssertEqual(service.state.deliveries.count, 1)
        try service.updateClusters([], demo: false)
        XCTAssertNotNil(service.state.subscriptions[0].error)
        try service.updateClusters([cluster], demo: false)
        XCTAssertNil(service.state.subscriptions[0].error)
        XCTAssertEqual(service.activeIDs(clusterID: cluster.id), [sub.id])
    }

    func testPersistenceFailureDoesNotAdvanceBaselineOrSend() async throws {
        let store = TestHookStorage()
        let executor = RecordingHookExecutor()
        let service = try AgentHookService(storage: store, executor: executor)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        store.failSave = true
        XCTAssertThrowsError(try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id]))
        XCTAssertNil(service.state.subscriptions[0].jobs[0].lastState)
        await service.deliverDue()
        let calls = await executor.events
        XCTAssertTrue(calls.isEmpty)
        store.failSave = false
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id], now: time)
        store.failSave = true
        await service.deliverDue(now: time)
        let stillNoCalls = await executor.events
        XCTAssertTrue(stillNoCalls.isEmpty, "Must persist delivering state before executing")
    }

    func testBoundedRetriesStableIDAndManualRetry() async throws {
        let store = TestHookStorage()
        let executor = RecordingHookExecutor(succeeds: false, time: time)
        let service = try AgentHookService(storage: store, executor: executor)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.completed], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: [sub.id], now: time)
        let id = service.state.deliveries[0].id
        await service.deliverDue(now: time)
        XCTAssertEqual(service.state.deliveries[0].nextAttemptAt, time.addingTimeInterval(5))
        await service.deliverDue(now: time.addingTimeInterval(4))
        XCTAssertEqual(service.state.deliveries[0].attempts, 1)
        await service.deliverDue(now: time.addingTimeInterval(5))
        XCTAssertEqual(service.state.deliveries[0].nextAttemptAt, time.addingTimeInterval(30))
        await service.deliverDue(now: time.addingTimeInterval(30))
        XCTAssertEqual(service.state.deliveries[0].nextAttemptAt, time.addingTimeInterval(120))
        await service.deliverDue(now: time.addingTimeInterval(120))
        XCTAssertEqual(service.state.deliveries[0].status, "failed")
        let events = await executor.events
        XCTAssertEqual(events.map(\.id), Array(repeating: id, count: 4))
        await executor.setSuccess(true)
        try service.retry(eventID: id, now: time)
        await service.deliverDue(now: time)
        XCTAssertEqual(service.state.deliveries[0].status, "delivered")
        XCTAssertEqual(service.state.deliveries[0].id, id)
    }

    func testAcknowledgementWriteFailureRecoversWithoutSendingAgain() async throws {
        let store = TestHookStorage()
        let executor = RecordingHookExecutor()
        let service = try AgentHookService(storage: store, executor: executor)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id], now: time)
        store.failOnSaveNumber = 4 // subscribe, observation, delivering, acknowledgement
        await service.deliverDue(now: time)
        XCTAssertEqual(service.state.deliveries[0].status, "delivering")
        XCTAssertNotNil(service.lastError)
        await service.deliverDue(now: time)
        XCTAssertEqual(service.state.deliveries[0].status, "delivered")
        let calls = await executor.events
        XCTAssertEqual(calls.count, 1)
    }

    func testRestartRecoveryAndCodexQueuedLabel() async throws {
        let store = TestHookStorage()
        let service = try AgentHookService(storage: store)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.completed],
            destination: HookDestination(executable: "/usr/bin/true", codexThread: UUID().uuidString))
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: [sub.id], now: time)
        store.state.deliveries[0].status = "delivering"
        store.state.deliveries[0].attempts = 1
        let restored = try AgentHookService(storage: store, executor: RecordingHookExecutor())
        XCTAssertEqual(restored.state.deliveries[0].status, "pending")
        await restored.deliverDue(now: time)
        XCTAssertEqual(restored.state.deliveries[0].status, "queued")
        XCTAssertEqual(restored.state.deliveries[0].attempts, 2)
        try restored.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: [sub.id])
        XCTAssertEqual(restored.state.deliveries.count, 1)
        store.state.deliveries[0].status = "delivering"
        store.state.deliveries[0].attempts = 4
        let exhausted = try AgentHookService(storage: store)
        XCTAssertEqual(exhausted.state.deliveries[0].status, "failed")
    }

    func testDemoAndUnsubscribePreventExecution() async throws {
        let executor = RecordingHookExecutor()
        let service = try AgentHookService(storage: TestHookStorage(), executor: executor)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id], now: time)
        try service.updateClusters([cluster], demo: true)
        await service.deliverDue(now: time)
        let calls = await executor.events
        XCTAssertTrue(calls.isEmpty)
        try service.updateClusters([cluster], demo: false)
        try service.unsubscribe(id: sub.id)
        await service.deliverDue(now: time)
        XCTAssertEqual(service.state.deliveries[0].status, "cancelled")
        XCTAssertThrowsError(try service.retry(eventID: service.state.deliveries[0].id))
    }

    func testUnsubscribeDuringDeliveryCancelsRemainingEvents() async throws {
        let executor = PausedHookExecutor()
        let service = try AgentHookService(storage: TestHookStorage(), executor: executor)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123", "456"], states: [.running], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running), "456": snapshot(.running, id: "456")],
                            subscriptionIDs: [sub.id], now: time)
        let delivery = Task { await service.deliverDue(now: time) }
        while await executor.count == 0 { await Task.yield() }
        await service.deliverDue(now: time) // Reentrant delivery cannot send twice.
        try service.unsubscribe(id: sub.id)
        await executor.finish()
        await delivery.value
        XCTAssertEqual(service.state.deliveries.map(\.status), ["cancelled", "cancelled"])
        let count = await executor.count
        XCTAssertEqual(count, 1)
    }

    func testValidationAndProtocol() throws {
        let service = try AgentHookService(storage: TestHookStorage())
        for id in ["12.batch", "12_[1-4]", "12;echo hi", ""] {
            XCTAssertThrowsError(try service.subscribe(cluster: cluster, jobIDs: [id], states: [.running], destination: destination))
        }
        XCTAssertThrowsError(try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [], destination: destination))
        XCTAssertThrowsError(try service.handle(HookRequest(command: "subscribe", cluster: "alpha-ssh", jobIDs: ["123"],
            states: ["typo"], destination: destination), clusters: [cluster]))
        let response = try service.handle(HookRequest(command: "subscribe", cluster: "alpha-ssh", jobIDs: ["123"],
            states: ["terminal"], destination: destination), clusters: [cluster])
        XCTAssertEqual(response.subscriptions?.first?.states, Set(NormalizedJobState.allCases.filter(\.isTerminal)))
    }

    func testFileStoreRoundTripAndCorruptStateIsNotOverwritten() throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try HookFileStore(directory: directory)
        let service = try AgentHookService(storage: store)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.completed], destination: destination, now: time)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: [sub.id], now: time)
        XCTAssertEqual(try store.load(), service.state)
        let eventURL = try store.writeEvent(service.state.deliveries[0].event)
        XCTAssertEqual(try HookJSON.decode(HookEvent.self, from: Data(contentsOf: eventURL)), service.state.deliveries[0].event)
        let url = directory.appendingPathComponent("state.json")
        try Data("invalid".utf8).write(to: url)
        XCTAssertThrowsError(try AgentHookService(storage: store))
        XCTAssertEqual(try String(contentsOf: url, encoding: .utf8), "invalid")
    }

    func testGenericProcessReceivesJSONAndCodexUsesLiteralArguments() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try HookFileStore(directory: directory)
        let service = try AgentHookService(storage: store)
        let sub = try service.subscribe(cluster: cluster, jobIDs: ["123"], states: [.running], destination: destination)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.running)], subscriptionIDs: [sub.id], now: time)
        let event = service.state.deliveries[0].event
        let file = try store.writeEvent(event)
        let cat = await HookProcessExecutor().execute(destination: HookDestination(executable: "/bin/cat"), event: event, eventFile: file)
        XCTAssertTrue(cat.succeeded)
        XCTAssertEqual(try HookJSON.decode(HookEvent.self, from: Data(cat.stdout.utf8)), event)
        let thread = UUID().uuidString
        let echo = await HookProcessExecutor().execute(destination: HookDestination(executable: "/bin/echo", codexThread: thread), event: event, eventFile: file)
        XCTAssertTrue(echo.succeeded)
        XCTAssertTrue(echo.stdout.hasPrefix("queue --thread \(thread) --message [Cluster Watch job status]"))
        XCTAssertTrue(echo.stdout.contains("$(touch unwanted) `command`"))
        XCTAssertFalse(FileManager.default.fileExists(atPath: directory.appendingPathComponent("unwanted").path))
        let timeout = await HookProcessExecutor(timeout: 0.05).execute(
            destination: HookDestination(executable: "/bin/sleep", arguments: ["10"]), event: event, eventFile: file)
        XCTAssertTrue(timeout.timedOut)
        XCTAssertFalse(timeout.succeeded)
        let noisy = await HookProcessExecutor(timeout: 0.05).execute(
            destination: HookDestination(executable: "/usr/bin/yes"), event: event, eventFile: file)
        XCTAssertEqual(noisy.stdout.utf8.count, 16_384)
        XCTAssertTrue(noisy.timedOut)
    }

    func testSocketRoundTripAndSingleServerLock() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("control.sock").path
        let server = try HookSocketServer(path: path) { request in
            HookResponse(ok: request.command == "clusters")
        }
        XCTAssertThrowsError(try HookSocketServer(path: path) { _ in HookResponse() })
        let response = try await Task.detached { try HookSocket.send(HookRequest(command: "clusters"), path: path) }.value
        XCTAssertTrue(response.ok)
        withExtendedLifetime(server) {}
    }

    func testSocketSubscriptionManagementRoundTrip() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("control.sock").path
        let service = try AgentHookService(storage: HookFileStore(directory: directory))
        let cluster = self.cluster
        let server = try HookSocketServer(path: path) { request in
            do { return try await service.handle(request, clusters: [cluster]) }
            catch { return HookResponse(ok: false, error: error.localizedDescription) }
        }
        let request = HookRequest(command: "subscribe", cluster: cluster.sshAlias, jobIDs: ["123"],
                                  states: ["terminal"], destination: destination)
        let response = try await Task.detached { try HookSocket.send(request, path: path) }.value
        let sub = try XCTUnwrap(response.subscriptions?.first)
        XCTAssertTrue(response.ok)
        try service.observe(cluster: cluster, snapshots: ["123": snapshot(.completed)], subscriptionIDs: [sub.id], now: time)
        let events = try await Task.detached { try HookSocket.send(HookRequest(command: "events.list", id: sub.id), path: path) }.value
        XCTAssertEqual(events.events?.count, 1)
        let cancel = try await Task.detached { try HookSocket.send(HookRequest(command: "unsubscribe", id: sub.id), path: path) }.value
        XCTAssertEqual(cancel.subscriptions?.first?.cancelled, true)
        XCTAssertEqual(service.state.deliveries[0].status, "cancelled")
        withExtendedLifetime(server) {}
    }

    private func temporaryDirectory() throws -> URL {
        let directory = URL(fileURLWithPath: "/private/tmp/cwh-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }
}

private final class TestHookStorage: HookStateStoring {
    var state = HookState()
    var failSave = false
    var failOnSaveNumber: Int?
    var saveCount = 0
    func load() throws -> HookState { state }
    func save(_ state: HookState) throws {
        saveCount += 1
        if failSave || saveCount == failOnSaveNumber { throw HookError("Injected write failure") }
        self.state = state
    }
    func writeEvent(_ event: HookEvent) throws -> URL { URL(fileURLWithPath: "/private/tmp/event-\(event.id).json") }
}

private actor RecordingHookExecutor: HookExecuting {
    var events: [HookEvent] = []
    var succeeds: Bool
    let time: Date
    init(succeeds: Bool = true, time: Date = Date(timeIntervalSince1970: 1000)) {
        self.succeeds = succeeds
        self.time = time
    }
    func setSuccess(_ value: Bool) { succeeds = value }
    func execute(destination: HookDestination, event: HookEvent, eventFile: URL) async -> HookExecutionResult {
        events.append(event)
        return HookExecutionResult(exitCode: succeeds ? 0 : 1, finishedAt: time)
    }
}

private actor PausedHookExecutor: HookExecuting {
    var count = 0
    private var continuation: CheckedContinuation<HookExecutionResult, Never>?
    func execute(destination: HookDestination, event: HookEvent, eventFile: URL) async -> HookExecutionResult {
        count += 1
        return await withCheckedContinuation { continuation = $0 }
    }
    func finish() {
        continuation?.resume(returning: HookExecutionResult(exitCode: 1))
        continuation = nil
    }
}
