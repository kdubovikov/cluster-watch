import Foundation

public enum ClusterID: String, CaseIterable, Codable, Hashable, Identifiable, Sendable {
    case camd
    case cscc

    public var id: String { rawValue }

    public var defaultDisplayName: String {
        switch self {
        case .camd:
            return "CAMD"
        case .cscc:
            return "CSCC"
        }
    }

    public var defaultAlias: String {
        switch self {
        case .camd:
            return "camd1"
        case .cscc:
            return "cscc"
        }
    }
}

public struct ClusterConfig: Identifiable, Codable, Hashable, Sendable {
    public let id: ClusterID
    public var displayName: String
    public var sshAlias: String
    public var sshUsername: String
    public var isEnabled: Bool
    public var usernameOverride: String

    public init(
        id: ClusterID,
        displayName: String,
        sshAlias: String,
        sshUsername: String = "",
        isEnabled: Bool = true,
        usernameOverride: String = ""
    ) {
        self.id = id
        self.displayName = displayName
        self.sshAlias = sshAlias
        self.sshUsername = sshUsername
        self.isEnabled = isEnabled
        self.usernameOverride = usernameOverride
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

    public static func defaultValue(for id: ClusterID) -> ClusterConfig {
        ClusterConfig(
            id: id,
            displayName: id.defaultDisplayName,
            sshAlias: id.defaultAlias
        )
    }

    public static func defaultClusters() -> [ClusterConfig] {
        ClusterID.allCases.map(ClusterConfig.defaultValue(for:))
    }
}

public extension String {
    var trimmedOrEmpty: String {
        trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
