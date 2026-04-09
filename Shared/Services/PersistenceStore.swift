import Foundation

public struct PersistedAppState: Codable, Equatable, Sendable {
    public var clusters: [ClusterConfig]
    public var globalUsernameFilter: String
    public var pollIntervalSeconds: Double
    public var watchedJobs: [WatchedJob]
    public var reachabilityByCluster: [String: ClusterReachabilityState]
    public var isDemoDataEnabled: Bool

    public init(
        clusters: [ClusterConfig],
        globalUsernameFilter: String,
        pollIntervalSeconds: Double,
        watchedJobs: [WatchedJob],
        reachabilityByCluster: [String: ClusterReachabilityState],
        isDemoDataEnabled: Bool = false
    ) {
        self.clusters = clusters
        self.globalUsernameFilter = globalUsernameFilter
        self.pollIntervalSeconds = pollIntervalSeconds
        self.watchedJobs = watchedJobs
        self.reachabilityByCluster = reachabilityByCluster
        self.isDemoDataEnabled = isDemoDataEnabled
    }

    public static func defaultState(localUsername: String = NSUserName()) -> PersistedAppState {
        PersistedAppState(
            clusters: ClusterConfig.defaultClusters(),
            globalUsernameFilter: localUsername,
            pollIntervalSeconds: 30,
            watchedJobs: [],
            reachabilityByCluster: [:],
            isDemoDataEnabled: false
        )
    }

    private enum CodingKeys: String, CodingKey {
        case clusters
        case globalUsernameFilter
        case pollIntervalSeconds
        case watchedJobs
        case reachabilityByCluster
        case isDemoDataEnabled
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.clusters = try container.decode([ClusterConfig].self, forKey: .clusters)
        self.globalUsernameFilter = try container.decode(String.self, forKey: .globalUsernameFilter)
        self.pollIntervalSeconds = try container.decode(Double.self, forKey: .pollIntervalSeconds)
        self.watchedJobs = try container.decode([WatchedJob].self, forKey: .watchedJobs)
        self.reachabilityByCluster = try container.decode([String: ClusterReachabilityState].self, forKey: .reachabilityByCluster)
        self.isDemoDataEnabled = try container.decodeIfPresent(Bool.self, forKey: .isDemoDataEnabled) ?? false
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
