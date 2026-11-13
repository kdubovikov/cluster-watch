import Foundation
import Darwin

/// Posts a user message into a live Claude Code session through its inbox socket.
/// The wire format is newline-delimited JSON; the socket is owner-only and lives
/// only while the session runs, so a connect failure means the session has exited.
/// Authentication is optional on macOS, so no token is read or stored.
public enum ClaudeSessionSocket {
    public static func isValid(path: String) -> Bool {
        path.hasPrefix("/") && !path.contains("\0") && path.utf8.count < 104
    }

    private struct Message: Encodable {
        struct Body: Encodable { var role = "user"; var content: String }
        var type = "user"
        var message: Body
    }

    public static func post(_ text: String, to path: String) throws {
        guard isValid(path: path) else { throw HookError("Invalid Claude session socket path.") }
        var info = stat()
        guard lstat(path, &info) == 0, (info.st_mode & S_IFMT) == S_IFSOCK else {
            throw HookError("Claude session is no longer running (no socket at \(path)).")
        }
        guard info.st_uid == getuid() else { throw HookError("Claude session socket belongs to another user.") }

        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw HookError("Unable to create Claude session socket.") }
        defer { close(fd) }
        HookSocket.configure(fd)
        var addr = try HookSocket.address(path)
        let connected = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard connected == 0 else {
            throw HookError("Cannot connect to Claude session socket: \(String(cString: strerror(errno)))")
        }
        let line = try HookJSON.encode(Message(message: .init(content: text))) + Data("\n".utf8)
        try line.withUnsafeBytes { raw in
            var offset = 0
            while offset < raw.count {
                let sent = Darwin.send(fd, raw.baseAddress!.advanced(by: offset), raw.count - offset, 0)
                if sent < 0 && errno == EINTR { continue }
                guard sent > 0 else { throw HookError("Unable to write to Claude session socket.") }
                offset += sent
            }
        }
        // The server stays silent on success and closes the connection when it
        // rejects a message, so a short wait distinguishes the two.
        var poll = pollfd(fd: fd, events: Int16(POLLIN), revents: 0)
        if Darwin.poll(&poll, 1, 200) > 0 {
            var byte: UInt8 = 0
            let count = recv(fd, &byte, 1, 0)
            if count <= 0 { throw HookError("Claude session closed the connection without accepting the message.") }
        }
    }
}
