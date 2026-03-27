import Foundation

public struct PersistedAppState: Codable, Equatable, Sendable {
    public var clusters: [ClusterConfig]
    public var globalUsernameFilter: String
    public var pollIntervalSeconds: Double
    public var watchedJobs: [WatchedJob]
    public var reachabilityByCluster: [String: ClusterReachabilityState]

    public init(
        clusters: [ClusterConfig],
        globalUsernameFilter: String,
        pollIntervalSeconds: Double,
        watchedJobs: [WatchedJob],
        reachabilityByCluster: [String: ClusterReachabilityState]
    ) {
        self.clusters = clusters
        self.globalUsernameFilter = globalUsernameFilter
        self.pollIntervalSeconds = pollIntervalSeconds
        self.watchedJobs = watchedJobs
        self.reachabilityByCluster = reachabilityByCluster
    }

    public static func defaultState(localUsername: String = NSUserName()) -> PersistedAppState {
        PersistedAppState(
            clusters: ClusterConfig.defaultClusters(),
            globalUsernameFilter: localUsername,
            pollIntervalSeconds: 30,
            watchedJobs: [],
            reachabilityByCluster: [:]
        )
    }
}

public protocol PersistenceStoring: Sendable {
    func load() async -> PersistedAppState?
    func save(_ state: PersistedAppState) async throws
}

public actor PersistenceStore: PersistenceStoring {
    private let fileURL: URL
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(fileURL: URL = PersistenceStore.defaultFileURL()) {
        self.fileURL = fileURL

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        self.decoder = decoder
    }

    public func load() async -> PersistedAppState? {
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            return nil
        }

        do {
            let data = try Data(contentsOf: fileURL)
            return try decoder.decode(PersistedAppState.self, from: data)
        } catch {
            return nil
        }
    }

    public func save(_ state: PersistedAppState) async throws {
        let directoryURL = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true, attributes: nil)
        let data = try encoder.encode(state)
        try data.write(to: fileURL, options: .atomic)
    }

    public static func defaultFileURL(fileManager: FileManager = .default) -> URL {
        let appSupport = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? URL(fileURLWithPath: NSHomeDirectory()).appendingPathComponent("Library/Application Support", isDirectory: true)
        return appSupport
            .appendingPathComponent("ClusterWatch", isDirectory: true)
            .appendingPathComponent("state.json", isDirectory: false)
    }
}
