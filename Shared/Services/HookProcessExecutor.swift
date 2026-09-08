import Foundation
import Darwin

public struct HookExecutionResult: Codable, Equatable, Sendable {
    public var exitCode: Int32?
    public var timedOut: Bool
    public var stdout: String
    public var stderr: String
    public var error: String?
    public var finishedAt: Date
    public var succeeded: Bool { exitCode == 0 && !timedOut && error == nil }
    public init(exitCode: Int32? = nil, timedOut: Bool = false, stdout: String = "",
                stderr: String = "", error: String? = nil, finishedAt: Date = Date()) {
        self.exitCode = exitCode
        self.timedOut = timedOut
        self.stdout = stdout
        self.stderr = stderr
        self.error = error
        self.finishedAt = finishedAt
    }
}

public protocol HookExecuting: Sendable {
    func execute(destination: HookDestination, event: HookEvent, eventFile: URL) async -> HookExecutionResult
}

public struct HookProcessExecutor: HookExecuting {
    public var timeout: TimeInterval
    public init(timeout: TimeInterval = 30) { self.timeout = timeout }

    public func execute(destination: HookDestination, event: HookEvent, eventFile: URL) async -> HookExecutionResult {
        await Task.detached {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: destination.executable)
            process.arguments = destination.arguments
            process.currentDirectoryURL = eventFile.deletingLastPathComponent()
            let out = Pipe(), err = Pipe()
            process.standardOutput = out
            process.standardError = err
            var input: FileHandle?
            defer { try? input?.close() }
            do {
                if let thread = destination.codexThread {
                    let summary = String(decoding: try HookJSON.encode(event), as: UTF8.self)
                    process.arguments = ["queue", "--thread", thread, "--message",
                        "[Cluster Watch job status]\nEvent data: \(summary)\nEvent file: \(eventFile.path)"]
                    process.standardInput = FileHandle.nullDevice
                } else {
                    input = try FileHandle(forReadingFrom: eventFile)
                    process.standardInput = input
                }
                try process.run()
                // Nonblocking reads keep both streams drained without allowing
                // unlimited output or inherited pipe handles to stall delivery.
                let outFD = out.fileHandleForReading.fileDescriptor
                let errFD = err.fileHandleForReading.fileDescriptor
                _ = fcntl(outFD, F_SETFL, O_NONBLOCK)
                _ = fcntl(errFD, F_SETFL, O_NONBLOCK)
                var stdout = Data(), stderr = Data()
                let deadline = ProcessInfo.processInfo.systemUptime + timeout
                var timedOut = false
                while process.isRunning {
                    Self.drain(outFD, into: &stdout)
                    Self.drain(errFD, into: &stderr)
                    if ProcessInfo.processInfo.systemUptime >= deadline {
                        timedOut = true
                        _ = kill(process.processIdentifier, SIGKILL)
                        process.waitUntilExit()
                        break
                    }
                    try? await Task.sleep(for: .milliseconds(10))
                }
                Self.drain(outFD, into: &stdout)
                Self.drain(errFD, into: &stderr)
                try? out.fileHandleForReading.close()
                try? err.fileHandleForReading.close()
                return HookExecutionResult(exitCode: process.terminationStatus, timedOut: timedOut,
                    stdout: String(decoding: stdout, as: UTF8.self), stderr: String(decoding: stderr, as: UTF8.self))
            } catch {
                return HookExecutionResult(error: error.localizedDescription)
            }
        }.value
    }

    private static func drain(_ fd: Int32, into output: inout Data) {
        var buffer = [UInt8](repeating: 0, count: 4096)
        for _ in 0..<16 {
            let count = Darwin.read(fd, &buffer, buffer.count)
            guard count > 0 else { break }
            let remaining = max(0, 16_384 - output.count)
            output.append(contentsOf: buffer.prefix(min(count, remaining)))
        }
    }
}
