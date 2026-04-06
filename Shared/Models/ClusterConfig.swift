import Foundation

public struct ClusterID: RawRepresentable, Codable, Hashable, Identifiable, Sendable {
    public var rawValue: String

    public var id: String { rawValue }

    public init(rawValue: String) {
        let trimmed = rawValue.trimmedOrEmpty
        self.rawValue = trimmed.isEmpty ? UUID().uuidString : trimmed
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(rawValue: try container.decode(String.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public static func new() -> ClusterID {
        ClusterID(rawValue: UUID().uuidString)
    }
}

public struct ClusterConfig: Identifiable, Codable, Hashable, Sendable {
    public let id: ClusterID
    public var displayName: String
    public var sshAlias: String
    public var sshUsername: String
    public var isEnabled: Bool
    public var usernameOverride: String
    public var usableGPUCap: Int?
    public var usableNodeCap: Int?

    public init(
        id: ClusterID = .new(),
        displayName: String,
        sshAlias: String,
        sshUsername: String = "",
        isEnabled: Bool = true,
        usernameOverride: String = "",
        usableGPUCap: Int? = nil,
        usableNodeCap: Int? = nil
    ) {
        self.id = id
        self.displayName = displayName
        self.sshAlias = sshAlias
        self.sshUsername = sshUsername
        self.isEnabled = isEnabled
        self.usernameOverride = usernameOverride
        self.usableGPUCap = usableGPUCap
        self.usableNodeCap = usableNodeCap
    }

    public var effectiveSSHDestination: String {
        let alias = sshAlias.trimmedOrEmpty
        guard !alias.isEmpty else { return "" }

        let username = sshUsername.trimmedOrEmpty
        return username.isEmpty ? alias : "\(username)@\(alias)"
    }

    public func effectiveUsername(globalUsername: String) -> String {
        let override = usernameOverride.trimmedOrEmpty
        return override.isEmpty ? globalUsername.trimmedOrEmpty : override
    }

    public func withID(_ id: ClusterID) -> ClusterConfig {
        ClusterConfig(
            id: id,
            displayName: displayName,
            sshAlias: sshAlias,
            sshUsername: sshUsername,
            isEnabled: isEnabled,
            usernameOverride: usernameOverride,
            usableGPUCap: usableGPUCap,
            usableNodeCap: usableNodeCap
        )
    }

    public static func defaultClusters() -> [ClusterConfig] {
        []
    }

    public static func empty(named displayName: String = "") -> ClusterConfig {
        ClusterConfig(
            displayName: displayName,
            sshAlias: "",
            sshUsername: "",
            isEnabled: true,
            usernameOverride: "",
            usableGPUCap: nil,
            usableNodeCap: nil
        )
    }
}

public extension String {
    var trimmedOrEmpty: String {
        trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
