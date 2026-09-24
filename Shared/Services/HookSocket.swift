import Foundation
import Darwin

public struct HookRequest: Codable, Sendable {
    public var command: String
    public var cluster: String?
    public var jobIDs: [String]?
    public var states: [String]?
    public var destination: HookDestination?
    public var id: String?
    public init(command: String, cluster: String? = nil, jobIDs: [String]? = nil,
                states: [String]? = nil, destination: HookDestination? = nil, id: String? = nil) {
        self.command = command
        self.cluster = cluster
        self.jobIDs = jobIDs
        self.states = states
        self.destination = destination
        self.id = id
    }
}

public struct HookResponse: Codable, Sendable {
    public var ok: Bool
    public var error: String?
    public var clusters: [ClusterConfig]?
    public var subscriptions: [HookSubscription]?
    public var events: [HookDelivery]?
    public var serviceError: String?
    /// Set by the CLI for inbox subscriptions: the command that streams their events.
    public var follow: String?
    public init(ok: Bool = true, error: String? = nil, clusters: [ClusterConfig]? = nil,
                subscriptions: [HookSubscription]? = nil, events: [HookDelivery]? = nil, serviceError: String? = nil) {
        self.ok = ok
        self.error = error
        self.clusters = clusters
        self.subscriptions = subscriptions
        self.events = events
        self.serviceError = serviceError
    }
}

/// One length-prefixed JSON request/response per connection. No network listener.
public enum HookSocket {
    public static var path: String { HookJSON.directory.appendingPathComponent("control.sock").path }
    private static let limit = 16 * 1024 * 1024

    static func address(_ path: String) throws -> sockaddr_un {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let bytes = Array(path.utf8CString)
        guard bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else {
            throw HookError("Hook socket path is too long: \(path)")
        }
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        withUnsafeMutableBytes(of: &address.sun_path) { raw in
            raw.copyBytes(from: bytes.map { UInt8(bitPattern: $0) })
        }
        return address
    }

    static func configure(_ fd: Int32) {
        var enabled: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout<Int32>.size))
        var timeout = timeval(tv_sec: 10, tv_usec: 0)
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        _ = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        _ = fcntl(fd, F_SETFD, FD_CLOEXEC)
        _ = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & ~O_NONBLOCK)
    }

    static func sameUser(_ fd: Int32) -> Bool {
        var uid: uid_t = 0, gid: gid_t = 0
        return getpeereid(fd, &uid, &gid) == 0 && uid == getuid()
    }

    static func readExactly(_ fd: Int32, count: Int) throws -> Data {
        var data = Data(count: count)
        try data.withUnsafeMutableBytes { raw in
            var offset = 0
            while offset < count {
                let read = recv(fd, raw.baseAddress!.advanced(by: offset), count - offset, 0)
                if read < 0 && errno == EINTR { continue }
                guard read > 0 else { throw HookError("Hook connection closed or timed out.") }
                offset += read
            }
        }
        return data
    }

    static func read(_ fd: Int32) throws -> Data {
        let header = try readExactly(fd, count: 4)
        let count = header.reduce(0) { ($0 << 8) | Int($1) }
        guard count > 0, count <= limit else { throw HookError("Invalid hook message size.") }
        return try readExactly(fd, count: count)
    }

    static func write(_ fd: Int32, data: Data) throws {
        guard !data.isEmpty, data.count <= limit else { throw HookError("Hook response exceeds 16 MiB; narrow the query.") }
        var size = UInt32(data.count).bigEndian
        var framed = withUnsafeBytes(of: &size) { Data($0) }
        framed.append(data)
        try framed.withUnsafeBytes { raw in
            var offset = 0
            while offset < raw.count {
                let sent = Darwin.send(fd, raw.baseAddress!.advanced(by: offset), raw.count - offset, 0)
                if sent < 0 && errno == EINTR { continue }
                guard sent > 0 else { throw HookError("Unable to write hook response.") }
                offset += sent
            }
        }
    }

    public static func send(_ request: HookRequest, path: String = HookSocket.path) throws -> HookResponse {
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw HookError("Unable to create hook socket.") }
        defer { close(fd) }
        configure(fd)
        var addr = try address(path)
        let result = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard result == 0 else { throw HookError("Cannot connect to Cluster Watch. Open the updated app and retry. Socket: \(path)") }
        guard sameUser(fd) else { throw HookError("Hook server belongs to another user.") }
        try write(fd, data: HookJSON.encode(request))
        return try HookJSON.decode(HookResponse.self, from: read(fd))
    }
}

public final class HookSocketServer: @unchecked Sendable {
    private let source: DispatchSourceRead
    private let lockFD: Int32
    private let path: String

    public init(path: String = HookSocket.path,
                handler: @escaping @Sendable (HookRequest) async -> HookResponse) throws {
        self.path = path
        var addr = try HookSocket.address(path)
        let directory = URL(fileURLWithPath: path).deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                               attributes: [.posixPermissions: 0o700])
        let lock = open(path + ".lock", O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0o600)
        guard lock >= 0 else { throw HookError("Cannot open hook server lock.") }
        guard flock(lock, LOCK_EX | LOCK_NB) == 0 else {
            close(lock)
            throw HookError("Another Cluster Watch app already owns the hook server.")
        }
        let listener = socket(AF_UNIX, SOCK_STREAM, 0)
        guard listener >= 0 else { close(lock); throw HookError("Cannot create hook listener.") }
        HookSocket.configure(listener)
        var noTimeout = timeval(tv_sec: 0, tv_usec: 0)
        _ = setsockopt(listener, SOL_SOCKET, SO_RCVTIMEO, &noTimeout, socklen_t(MemoryLayout<timeval>.size))
        unlink(path)
        let bound = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(listener, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard bound == 0, chmod(path, 0o600) == 0, listen(listener, 16) == 0 else {
            let reason = String(cString: strerror(errno))
            close(listener); close(lock)
            throw HookError("Cannot bind hook socket: \(path): \(reason)")
        }
        lockFD = lock
        _ = fcntl(listener, F_SETFL, O_NONBLOCK)
        source = DispatchSource.makeReadSource(fileDescriptor: listener, queue: DispatchQueue(label: "ClusterWatch.hooks.accept"))
        source.setCancelHandler { close(listener) }
        // The source closes the listener only after its handler stops using it,
        // preventing descriptor reuse races when an app instance shuts down.
        source.setEventHandler {
            while true {
                let client = accept(listener, nil, nil)
                if client < 0 {
                    if errno == EINTR { continue }
                    break
                }
                HookSocket.configure(client)
                guard HookSocket.sameUser(client) else { close(client); continue }
                DispatchQueue.global(qos: .utility).async {
                    do {
                        let request = try HookJSON.decode(HookRequest.self, from: HookSocket.read(client))
                        Task {
                            let response = await handler(request)
                            // Socket writes must not block the app's main actor.
                            do { try HookSocket.write(client, data: HookJSON.encode(response)) }
                            catch {
                                try? HookSocket.write(client, data: HookJSON.encode(HookResponse(ok: false, error: error.localizedDescription)))
                            }
                            close(client)
                        }
                    } catch {
                        try? HookSocket.write(client, data: HookJSON.encode(HookResponse(ok: false, error: error.localizedDescription)))
                        close(client)
                    }
                }
            }
        }
        source.resume()
    }

    deinit {
        source.cancel()
        unlink(path)
        close(lockFD)
    }
}
