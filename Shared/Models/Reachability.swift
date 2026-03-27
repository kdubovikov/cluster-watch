import Foundation

public struct ClusterReachabilityState: Codable, Equatable, Sendable {
    public enum Status: String, Codable, CaseIterable, Sendable {
        case checking
        case reachable
        case unreachable
    }

    public var status: Status
    public var lastSuccessfulRefresh: Date?
    public var lastErrorMessage: String?

    public init(
        status: Status = .checking,
        lastSuccessfulRefresh: Date? = nil,
        lastErrorMessage: String? = nil
    ) {
        self.status = status
        self.lastSuccessfulRefresh = lastSuccessfulRefresh
        self.lastErrorMessage = lastErrorMessage
    }
}
