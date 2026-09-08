import Foundation
import ClusterWatchCore
import Darwin

let usage = """
Usage:
  cluster-watch clusters
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID [--job ID ...]
    --states running,terminal --codex-thread UUID [--codex-executable /path/to/codex]
  cluster-watch subscribe --cluster ID_OR_ALIAS --job ID --states terminal
    --exec /absolute/executable [-- arguments...]
  cluster-watch subscriptions list
  cluster-watch subscriptions show SUBSCRIPTION_ID
  cluster-watch unsubscribe SUBSCRIPTION_ID
  cluster-watch events list [--subscription SUBSCRIPTION_ID]
  cluster-watch events retry EVENT_ID

All commands return JSON. Requires the updated Cluster Watch app to be running.
Statuses: pending, running, completed, failed, cancelled, timeout, outOfMemory,
          nodeFail, preempted, unknown; terminal expands to all terminal statuses.
"""

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
    }
    guard command == "subscribe" else { throw HookError(usage) }
    var values: [String: String] = [:], jobs: [String] = [], trailing: [String] = []
    var index = 1
    while index < args.count {
        let key = args[index]
        if key == "--" { trailing = Array(args.dropFirst(index + 1)); break }
        guard ["--cluster", "--job", "--states", "--codex-thread", "--codex-executable", "--exec"].contains(key),
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
    let destination: HookDestination
    if let thread = values["--codex-thread"], values["--exec"] == nil, trailing.isEmpty {
        let executable: String
        if let override = values["--codex-executable"] { executable = override }
        else {
            let paths = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":")
            guard let found = paths.map({ URL(fileURLWithPath: String($0)).appendingPathComponent("codex").path })
                .first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
                throw HookError("codex is not on PATH; supply --codex-executable /absolute/path.")
            }
            executable = found
        }
        destination = HookDestination(executable: executable, codexThread: thread)
    } else if let executable = values["--exec"], values["--codex-thread"] == nil, values["--codex-executable"] == nil {
        destination = HookDestination(executable: executable, arguments: trailing)
    } else { throw HookError("Choose exactly one destination: --codex-thread or --exec.") }
    return HookRequest(command: command, cluster: cluster, jobIDs: jobs,
                       states: stateText.components(separatedBy: ","), destination: destination)
}

do {
    let args = Array(CommandLine.arguments.dropFirst())
    if args == ["--help"] || args == ["-h"] {
        print(usage)
    } else {
        let response = try HookSocket.send(parse(args))
        print(String(decoding: try HookJSON.encode(response), as: UTF8.self))
        if !response.ok { exit(1) }
    }
} catch {
    let response = HookResponse(ok: false, error: error.localizedDescription)
    print(String(decoding: try HookJSON.encode(response), as: UTF8.self))
    exit(1)
}
