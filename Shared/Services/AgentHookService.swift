import Foundation

public struct HookError: LocalizedError, Sendable {
    public let message: String
    public init(_ message: String) { self.message = message }
    public var errorDescription: String? { message }
}

public enum HookJSON {
    public static func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(value)
    }

    public static func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(type, from: data)
    }

    public static var directory: URL {
        PersistenceStore.defaultFileURL().deletingLastPathComponent().appendingPathComponent("hooks")
    }
}

public struct HookDestination: Codable, Equatable, Sendable {
    public var executable: String
    public var arguments: [String]
    public var codexThread: String?
    /// Claude Code cloud session ID or claude.ai/code URL, passed to `claude --cloud`.
    public var claudeSession: String?
    /// OpenCode session ID (`ses_...`); the app admits one queued input through
    /// `opencode api post /api/session/<id>/prompt`.
    public var opencodeSession: String?
    /// Inbox socket of a live local Claude Code session (`CLAUDE_CODE_MESSAGING_SOCKET`).
    /// Events are posted straight into that session without starting a process.
    public var claudeSocket: String?
    /// Record-only: events are appended to the subscription's inbox file for
    /// `cluster-watch events follow`, without running a process.
    public var inbox: Bool?

    public init(executable: String, arguments: [String] = [], codexThread: String? = nil,
                claudeSession: String? = nil, opencodeSession: String? = nil, claudeSocket: String? = nil,
                inbox: Bool? = nil) {
        self.executable = executable
        self.arguments = arguments
        self.codexThread = codexThread
        self.claudeSession = claudeSession
        self.opencodeSession = opencodeSession
        self.claudeSocket = claudeSocket
        self.inbox = inbox
    }

    public static let inboxOnly = HookDestination(executable: "", inbox: true)

    /// Agent destinations only confirm that the CLI accepted the message.
    var isAgentQueue: Bool {
        codexThread != nil || claudeSession != nil || opencodeSession != nil || claudeSocket != nil
    }

    func validate() throws {
        if inbox == true {
            guard executable.isEmpty, arguments.isEmpty, codexThread == nil, claudeSession == nil,
                  opencodeSession == nil, claudeSocket == nil else {
                throw HookError("Inbox destination cannot be combined with another destination.")
            }
            return
        }
        if let claudeSocket {
            guard executable.isEmpty, arguments.isEmpty, codexThread == nil, claudeSession == nil,
                  opencodeSession == nil, ClaudeSessionSocket.isValid(path: claudeSocket) else {
                throw HookError("Claude socket destination requires an absolute Unix socket path and no other destination.")
            }
            return
        }
        guard executable.hasPrefix("/"), !executable.contains("\0"),
              FileManager.default.isExecutableFile(atPath: executable),
              arguments.allSatisfy({ !$0.contains("\0") }) else {
            throw HookError("Destination must be an absolute executable path with valid arguments.")
        }
        if let codexThread {
            guard UUID(uuidString: codexThread) != nil, arguments.isEmpty else {
                throw HookError("Codex destination requires a thread UUID and no generic arguments.")
            }
        }
        if let claudeSession {
            guard codexThread == nil, arguments.isEmpty, !claudeSession.isEmpty, claudeSession.count <= 512,
                  !claudeSession.hasPrefix("-"),
                  claudeSession.unicodeScalars.allSatisfy({ !CharacterSet.whitespacesAndNewlines.contains($0) &&
                      !CharacterSet.controlCharacters.contains($0) }) else {
                throw HookError("Claude destination requires a session ID or claude.ai URL and no generic arguments.")
            }
        }
        if let opencodeSession {
            guard codexThread == nil, claudeSession == nil, arguments.isEmpty, opencodeSession.hasPrefix("ses"),
                  opencodeSession.count <= 512,
                  opencodeSession.unicodeScalars.allSatisfy({ !CharacterSet.whitespacesAndNewlines.contains($0) &&
                      !CharacterSet.controlCharacters.contains($0) }) else {
                throw HookError("OpenCode destination requires a session ID starting with ses and no generic arguments.")
            }
        }
    }
}

/// Per-subscription JSON Lines file: one compact HookEvent per line, append-only.
public enum HookInbox {
    public static func url(subscriptionID: String, directory: URL = HookJSON.directory) -> URL {
        directory.appendingPathComponent("inbox").appendingPathComponent("\(subscriptionID).jsonl")
    }

    static func append(_ event: HookEvent, directory: URL) throws {
        let url = url(subscriptionID: event.subscriptionID, directory: directory)
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true,
                                               attributes: [.posixPermissions: 0o700])
        let fd = open(url.path, O_WRONLY | O_APPEND | O_CREAT, 0o600)
        guard fd >= 0 else { throw HookError("Cannot open inbox file: \(url.path)") }
        defer { close(fd) }
        // One write per line keeps each event whole for concurrent readers.
        let line = try HookJSON.encode(event) + Data("\n".utf8)
        let written = line.withUnsafeBytes { write(fd, $0.baseAddress, line.count) }
        guard written == line.count else { throw HookError("Short write to inbox file: \(url.path)") }
    }
}

public struct HookJob: Codable, Equatable, Sendable {
    public var jobID: String
    public var lastState: NormalizedJobState?
    public var retired = false
}

public struct HookSubscription: Codable, Equatable, Identifiable, Sendable {
    public var id: String
    public var clusterID: ClusterID
    public var jobs: [HookJob]
    public var states: Set<NormalizedJobState>
    public var destination: HookDestination
    public var createdAt: Date
    public var cancelled = false
    public var error: String?
    public var paused = false
    public var completed = false
}

public struct HookEvent: Codable, Equatable, Identifiable, Sendable {
    public var schemaVersion = 1
    public var id: String
    public var subscriptionID: String
    public var clusterID: ClusterID
    public var clusterName: String
    public var jobID: String
    public var jobName: String
    public var previousState: NormalizedJobState?
    public var state: NormalizedJobState
    public var rawState: String
    public var observedAt: Date
    public var submitTime: Date?
    public var startTime: Date?
    public var endTime: Date?
}

public struct HookDelivery: Codable, Equatable, Identifiable, Sendable {
    public var event: HookEvent
    public var status = "pending"
    public var attempts = 0
    public var nextAttemptAt: Date
    public var result: HookExecutionResult?
    public var id: String { event.id }
}

public struct HookState: Codable, Equatable, Sendable {
    public var schemaVersion = 1
    public var subscriptions: [HookSubscription] = []
    public var deliveries: [HookDelivery] = []
    public init() {}
}

public protocol HookStateStoring {
    func load() throws -> HookState
    func save(_ state: HookState) throws
    func writeEvent(_ event: HookEvent) throws -> URL
}

public final class HookFileStore: HookStateStoring {
    public let directory: URL
    public init(directory: URL = HookJSON.directory) throws {
        self.directory = directory
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                               attributes: [.posixPermissions: 0o700])
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path)
    }
    public func load() throws -> HookState {
        let url = directory.appendingPathComponent("state.json")
        guard FileManager.default.fileExists(atPath: url.path) else { return HookState() }
        let state = try HookJSON.decode(HookState.self, from: Data(contentsOf: url))
        guard state.schemaVersion == 1 else { throw HookError("Unsupported hook-state schema version.") }
        return state
    }
    public func save(_ state: HookState) throws {
        try HookJSON.encode(state).write(to: directory.appendingPathComponent("state.json"), options: .atomic)
    }
    public func writeEvent(_ event: HookEvent) throws -> URL {
        let url = directory.appendingPathComponent("event-\(event.id).json")
        try HookJSON.encode(event).write(to: url, options: .atomic)
        return url
    }
}

/// All state transactions are synchronous on the main actor: no registration or
/// polling callback can interleave a read/modify/save operation.
@MainActor
public final class AgentHookService {
    public private(set) var state: HookState
    public private(set) var lastError: String?
    private let storage: any HookStateStoring
    private let executor: any HookExecuting
    private var delivering = false
    private var acknowledgements: [String: HookExecutionResult] = [:]
    public var deliveryEnabled = true

    public init(storage: any HookStateStoring, executor: any HookExecuting = HookProcessExecutor()) throws {
        self.storage = storage
        self.executor = executor
        let original = try storage.load()
        var loaded = original
        // A crash after sending but before saving acknowledgement is ambiguous.
        // Reuse the durable event ID on recovery, respecting the attempt limit.
        for index in loaded.deliveries.indices where loaded.deliveries[index].status == "delivering" {
            loaded.deliveries[index].status = loaded.deliveries[index].attempts >= 4 ? "failed" : "pending"
        }
        if loaded != original { try storage.save(loaded) }
        state = loaded
    }

    private func commit(_ next: HookState) throws {
        do {
            try storage.save(next)
            state = next
            lastError = nil
        } catch {
            lastError = error.localizedDescription
            throw error
        }
    }

    public func subscribe(cluster: ClusterConfig, jobIDs: [String], states: Set<NormalizedJobState>,
                          destination: HookDestination, now: Date = Date()) throws -> HookSubscription {
        guard !jobIDs.isEmpty, jobIDs.count <= 1000, !states.isEmpty,
              jobIDs.allSatisfy({ $0.range(of: "^[0-9]+(_[0-9]+)?$", options: .regularExpression) != nil }) else {
            throw HookError("Specify 1–1000 ordinary job IDs or explicit array-task IDs, and at least one status.")
        }
        try destination.validate()
        let subscription = HookSubscription(id: UUID().uuidString, clusterID: cluster.id,
            jobs: Array(Set(jobIDs)).sorted().map { HookJob(jobID: $0) }, states: states,
            destination: destination, createdAt: now, paused: !cluster.isEnabled)
        var next = state
        next.subscriptions.append(subscription)
        try commit(next)
        return subscription
    }

    public func unsubscribe(id: String) throws {
        guard let index = state.subscriptions.firstIndex(where: { $0.id == id }) else {
            throw HookError("Unknown subscription: \(id)")
        }
        var next = state
        next.subscriptions[index].cancelled = true
        for index in next.deliveries.indices where next.deliveries[index].event.subscriptionID == id &&
            ["pending", "failed"].contains(next.deliveries[index].status) {
            next.deliveries[index].status = "cancelled"
        }
        try commit(next)
    }

    public func retry(eventID: String, now: Date = Date()) throws {
        guard let index = state.deliveries.firstIndex(where: { $0.id == eventID }),
              state.deliveries[index].status == "failed",
              state.subscriptions.contains(where: { $0.id == state.deliveries[index].event.subscriptionID && !$0.cancelled }) else {
            throw HookError("Only failed events on non-cancelled subscriptions can be retried.")
        }
        var next = state
        next.deliveries[index].status = "pending"
        next.deliveries[index].attempts = 0
        next.deliveries[index].nextAttemptAt = now
        try commit(next)
    }

    public func updateClusters(_ clusters: [ClusterConfig], demo: Bool) throws {
        deliveryEnabled = !demo
        var next = state
        for index in next.subscriptions.indices {
            let cluster = clusters.first { $0.id == next.subscriptions[index].clusterID }
            next.subscriptions[index].error = cluster == nil ? "Cluster was removed from configuration." : nil
            next.subscriptions[index].paused = demo || cluster?.isEnabled == false
        }
        if next != state { try commit(next) }
    }

    public func activeIDs(clusterID: ClusterID) -> Set<String> {
        Set(state.subscriptions.filter {
            $0.clusterID == clusterID && !$0.cancelled && !$0.completed && !$0.paused && $0.error == nil
        }.map(\.id))
    }

    public func jobIDs(subscriptionIDs: Set<String>) -> Set<String> {
        Set(state.subscriptions.filter { subscriptionIDs.contains($0.id) && !$0.cancelled }
            .flatMap { $0.jobs.filter { !$0.retired }.map(\.jobID) })
    }

    public func observe(cluster: ClusterConfig, snapshots: [String: JobSnapshot],
                        subscriptionIDs: Set<String>, now: Date = Date()) throws {
        guard deliveryEnabled else { return }
        var next = state
        for si in next.subscriptions.indices {
            guard subscriptionIDs.contains(next.subscriptions[si].id),
                  next.subscriptions[si].clusterID == cluster.id,
                  !next.subscriptions[si].cancelled, !next.subscriptions[si].paused,
                  next.subscriptions[si].error == nil else { continue }
            for ji in next.subscriptions[si].jobs.indices {
                let job = next.subscriptions[si].jobs[ji]
                guard !job.retired, let snapshot = snapshots[job.jobID] else { continue }
                if job.lastState != snapshot.state && next.subscriptions[si].states.contains(snapshot.state) {
                    let event = HookEvent(id: UUID().uuidString, subscriptionID: next.subscriptions[si].id,
                        clusterID: cluster.id, clusterName: cluster.displayName, jobID: job.jobID,
                        jobName: snapshot.jobName, previousState: job.lastState, state: snapshot.state,
                        rawState: snapshot.rawState, observedAt: now, submitTime: snapshot.submitTime,
                        startTime: snapshot.startTime, endTime: snapshot.endTime)
                    next.deliveries.append(HookDelivery(event: event, nextAttemptAt: now))
                }
                next.subscriptions[si].jobs[ji].lastState = snapshot.state
                next.subscriptions[si].jobs[ji].retired = snapshot.state.isTerminal
            }
            next.subscriptions[si].completed = next.subscriptions[si].jobs.allSatisfy(\.retired)
        }
        if next != state { try commit(next) }
    }

    public func deliverDue(now: Date = Date()) async {
        guard deliveryEnabled, !delivering else { return }
        delivering = true
        defer { delivering = false }
        do { try saveAcknowledgements() }
        catch { lastError = error.localizedDescription; return }
        // Snapshot IDs, then recheck before each execution because unsubscribe,
        // settings changes and new observations can run while a process awaits.
        let ids = state.deliveries.filter { $0.status == "pending" && $0.nextAttemptAt <= now }.map(\.id)
        for id in ids {
            guard deliveryEnabled, let index = state.deliveries.firstIndex(where: { $0.id == id }),
                  state.deliveries[index].status == "pending",
                  let subscription = state.subscriptions.first(where: { $0.id == state.deliveries[index].event.subscriptionID }),
                  !subscription.cancelled else { continue }
            do {
                let event = state.deliveries[index].event
                let path = try storage.writeEvent(event)
                var next = state
                next.deliveries[index].status = "delivering"
                next.deliveries[index].attempts += 1
                try commit(next)
                let result = await executor.execute(destination: subscription.destination, event: event, eventFile: path)
                acknowledgements[id] = result
                try saveAcknowledgements()
            } catch {
                lastError = error.localizedDescription
                // Keep the durable ambiguous delivery for recovery. Do not send
                // another event while persistence is failing.
                return
            }
        }
    }

    private func saveAcknowledgements() throws {
        guard !acknowledgements.isEmpty else { return }
        var next = state
        for (id, result) in acknowledgements {
            guard let index = next.deliveries.firstIndex(where: { $0.id == id }),
                  let subscription = next.subscriptions.first(where: { $0.id == next.deliveries[index].event.subscriptionID }) else { continue }
            next.deliveries[index].result = result
            if result.succeeded {
                next.deliveries[index].status = subscription.destination.isAgentQueue ? "queued" : "delivered"
            } else if subscription.cancelled {
                next.deliveries[index].status = "cancelled"
            } else if next.deliveries[index].attempts >= 4 {
                next.deliveries[index].status = "failed"
            } else {
                next.deliveries[index].status = "pending"
                let delays: [TimeInterval] = [5, 30, 120]
                next.deliveries[index].nextAttemptAt = result.finishedAt.addingTimeInterval(delays[next.deliveries[index].attempts - 1])
            }
        }
        try commit(next)
        acknowledgements.removeAll()
    }

    public func handle(_ request: HookRequest, clusters: [ClusterConfig]) throws -> HookResponse {
        switch request.command {
        case "clusters": return HookResponse(clusters: clusters)
        case "subscribe":
            guard let selector = request.cluster, let destination = request.destination else {
                throw HookError("subscribe requires a cluster and a destination.")
            }
            let byID = clusters.filter { $0.id.rawValue == selector }
            let matches = byID.isEmpty ? clusters.filter { $0.sshAlias == selector } : byID
            guard matches.count == 1 else { throw HookError("Unknown or ambiguous cluster; use a stable ID from clusters.") }
            var states: Set<NormalizedJobState> = []
            for token in request.states ?? [] {
                if token == "terminal" {
                    states.formUnion(NormalizedJobState.allCases.filter(\.isTerminal))
                } else if let state = NormalizedJobState(rawValue: token) { states.insert(state) }
                else { throw HookError("Unknown status: \(token). Use \(NormalizedJobState.allCases.map(\.rawValue).joined(separator: ", ")), or terminal.") }
            }
            let subscription = try subscribe(cluster: matches[0], jobIDs: request.jobIDs ?? [],
                                             states: states, destination: destination)
            return HookResponse(subscriptions: [subscription])
        case "subscriptions.list": return HookResponse(subscriptions: state.subscriptions)
        case "subscriptions.show":
            guard let found = state.subscriptions.first(where: { $0.id == request.id }) else {
                throw HookError("Unknown subscription.")
            }
            return HookResponse(subscriptions: [found])
        case "unsubscribe":
            try unsubscribe(id: request.id ?? "")
            return HookResponse(subscriptions: state.subscriptions.filter { $0.id == request.id })
        case "events.list":
            return HookResponse(events: state.deliveries.filter { request.id == nil || $0.event.subscriptionID == request.id })
        case "events.retry":
            try retry(eventID: request.id ?? "")
            return HookResponse(events: state.deliveries.filter { $0.id == request.id })
        default: throw HookError("Unknown hook command: \(request.command)")
        }
    }
}
