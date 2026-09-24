import Foundation
import ClusterWatchCore
import Darwin

let usage = """
Usage:
  cluster-watch clusters
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID [--job ID ...]
    --states running,terminal --codex-thread UUID [--codex-executable /path/to/codex]
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID [--job ID ...]
    --states terminal --claude-session SESSION_ID_OR_URL [--claude-executable /path/to/claude]
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID [--job ID ...]
    --states terminal --opencode-session SESSION_ID [--opencode-executable /path/to/opencode]
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID --states terminal
    --exec /absolute/executable [-- arguments...]
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID --states terminal
    (no destination: events go to an inbox read by `events follow`)
  cluster-watch events follow --subscription SUBSCRIPTION_ID
  cluster-watch subscriptions list
  cluster-watch subscriptions show SUBSCRIPTION_ID
  cluster-watch unsubscribe SUBSCRIPTION_ID
  cluster-watch events list [--subscription SUBSCRIPTION_ID]
  cluster-watch events retry EVENT_ID

All commands return JSON; `events follow` prints one JSON event per line and
exits once the subscription is cancelled or all its jobs finished and were delivered. Requires the updated Cluster Watch app to be running.
Statuses: pending, running, completed, failed, cancelled, timeout, outOfMemory,
          nodeFail, preempted, unknown; terminal expands to all terminal statuses.
"""

func resolve(_ name: String, override: String?, flag: String) throws -> String {
    if let override { return override }
    let paths = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":")
    guard let found = paths.map({ URL(fileURLWithPath: String($0)).appendingPathComponent(name).path })
        .first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
        throw HookError("\(name) is not on PATH; supply \(flag) /absolute/path.")
    }
    return found
}

func parse(_ args: [String]) throws -> HookRequest {
    guard let command = args.first else { throw HookError(usage) }
    if command == "clusters", args.count == 1 { return HookRequest(command: command) }
    if command == "unsubscribe", args.count == 2 { return HookRequest(command: command, id: args[1]) }
    if command == "subscriptions" {
        if args == ["subscriptions", "list"] { return HookRequest(command: "subscriptions.list") }
        if args.count == 3, args[1] == "show" { return HookRequest(command: "subscriptions.show", id: args[2]) }
    }
    if command == "events" {
        if args == ["events", "list"] { return HookRequest(command: "events.list") }
        if args.count == 4, args[1] == "list", args[2] == "--subscription" {
            return HookRequest(command: "events.list", id: args[3])
        }
        if args.count == 3, args[1] == "retry" { return HookRequest(command: "events.retry", id: args[2]) }
        if args.count == 4, args[1] == "follow", args[2] == "--subscription" {
            return HookRequest(command: "events.follow", id: args[3])
        }
    }
    guard command == "subscribe" else { throw HookError(usage) }
    var values: [String: String] = [:], jobs: [String] = [], trailing: [String] = []
    var index = 1
    while index < args.count {
        let key = args[index]
        if key == "--" { trailing = Array(args.dropFirst(index + 1)); break }
        guard ["--cluster", "--job", "--states", "--codex-thread", "--codex-executable",
               "--claude-session", "--claude-executable", "--opencode-session",
               "--opencode-executable", "--exec"].contains(key),
              index + 1 < args.count else { throw HookError("Unknown option or missing value: \(key)") }
        if key == "--job" { jobs.append(args[index + 1]) }
        else {
            guard values[key] == nil else { throw HookError("Repeated option: \(key)") }
            values[key] = args[index + 1]
        }
        index += 2
    }
    guard let cluster = values["--cluster"], let stateText = values["--states"], !jobs.isEmpty else {
        throw HookError("subscribe requires --cluster, --job and --states.")
    }
    let codex = values["--codex-thread"] != nil || values["--codex-executable"] != nil
    let claude = values["--claude-session"] != nil || values["--claude-executable"] != nil
    let opencode = values["--opencode-session"] != nil || values["--opencode-executable"] != nil
    let exec = values["--exec"] != nil || !trailing.isEmpty
    guard [codex, claude, opencode, exec].filter({ $0 }).count <= 1 else {
        throw HookError("Choose at most one destination: --codex-thread, --claude-session, --opencode-session or --exec.")
    }
    let destination: HookDestination
    if codex, let thread = values["--codex-thread"] {
        let executable = try resolve("codex", override: values["--codex-executable"], flag: "--codex-executable")
        destination = HookDestination(executable: executable, codexThread: thread)
    } else if claude, let session = values["--claude-session"] {
        let executable = try resolve("claude", override: values["--claude-executable"], flag: "--claude-executable")
        destination = HookDestination(executable: executable, claudeSession: session)
    } else if opencode, let session = values["--opencode-session"] {
        let executable = try resolve("opencode", override: values["--opencode-executable"], flag: "--opencode-executable")
        destination = HookDestination(executable: executable, opencodeSession: session)
    } else if exec, let executable = values["--exec"] {
        destination = HookDestination(executable: executable, arguments: trailing)
    } else if !codex, !claude, !opencode, !exec {
        destination = .inboxOnly
    } else { throw HookError("Choose at most one destination: --codex-thread, --claude-session, --opencode-session or --exec.") }
    return HookRequest(command: command, cluster: cluster, jobIDs: jobs,
                       states: stateText.components(separatedBy: ","), destination: destination)
}

/// Streams the subscription's inbox file. Starts from the beginning so events
/// written before following began are not missed; consumers deduplicate by `id`.
func follow(subscriptionID: String) throws -> Never {
    let url = HookInbox.url(subscriptionID: subscriptionID)
    var handle: FileHandle?
    var buffer = Data()
    var nextCheck = Date.distantPast
    var finished = false
    setvbuf(stdout, nil, _IOLBF, 0)
    while true {
        if handle == nil { handle = FileHandle(forReadingAtPath: url.path) }
        if let handle {
            buffer.append(handle.readDataToEndOfFile())
            while let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) {
                print(String(decoding: buffer[buffer.startIndex..<newline], as: UTF8.self))
                buffer.removeSubrange(buffer.startIndex...newline)
            }
        }
        if finished { exit(0) }
        if Date() >= nextCheck {
            nextCheck = Date().addingTimeInterval(5)
            // The app may be restarting; keep following until it answers.
            if let subs = try? HookSocket.send(HookRequest(command: "subscriptions.show", id: subscriptionID)),
               let events = try? HookSocket.send(HookRequest(command: "events.list", id: subscriptionID)) {
                guard subs.ok, let sub = subs.subscriptions?.first else {
                    throw HookError(subs.error ?? "Unknown subscription.")
                }
                guard sub.destination.inbox == true else { throw HookError("Subscription does not use the inbox.") }
                let inFlight = (events.events ?? []).contains { ["pending", "delivering"].contains($0.status) }
                // One more read after the final state check picks up the last events.
                finished = sub.cancelled || (sub.completed && !inFlight)
                if finished { continue }
            }
        }
        usleep(500_000)
    }
}

do {
    let args = Array(CommandLine.arguments.dropFirst())
    if args == ["--help"] || args == ["-h"] {
        print(usage)
    } else {
        let request = try parse(args)
        if request.command == "events.follow" { try follow(subscriptionID: request.id ?? "") }
        var response = try HookSocket.send(request)
        if request.destination?.inbox == true, let id = response.subscriptions?.first?.id {
            response.follow = "cluster-watch events follow --subscription \(id)"
        }
        print(String(decoding: try HookJSON.encode(response), as: UTF8.self))
        if !response.ok { exit(1) }
    }
} catch {
    let response = HookResponse(ok: false, error: error.localizedDescription)
    print(String(decoding: try HookJSON.encode(response), as: UTF8.self))
    exit(1)
}
