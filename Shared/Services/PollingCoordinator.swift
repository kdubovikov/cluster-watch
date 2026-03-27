import Foundation

@MainActor
public final class PollingCoordinator {
    private var task: Task<Void, Never>?

    public init() {}

    public func start(
        intervalProvider: @escaping @MainActor () -> TimeInterval,
        refreshAction: @escaping @MainActor () async -> Void
    ) {
        stop()

        task = Task {
            while !Task.isCancelled {
                let seconds = max(5, intervalProvider())
                let nanoseconds = UInt64(seconds * 1_000_000_000)
                try? await Task.sleep(nanoseconds: nanoseconds)

                guard !Task.isCancelled else { return }
                await refreshAction()
            }
        }
    }

    public func stop() {
        task?.cancel()
        task = nil
    }
}
