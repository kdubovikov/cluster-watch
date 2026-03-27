import SwiftUI

struct ClusterStatusIndicatorsView: View {
    let store: JobStore

    var body: some View {
        HStack(spacing: 8) {
            ForEach(store.clusters) { cluster in
                ClusterStatusIndicatorView(
                    cluster: cluster,
                    reachability: store.reachability(for: cluster.id)
                )
            }

            Spacer(minLength: 0)
        }
    }
}

private struct ClusterStatusIndicatorView: View {
    let cluster: ClusterConfig
    let reachability: ClusterReachabilityState

    var body: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(statusColor)
                .frame(width: 8, height: 8)

            Text(cluster.displayName)
                .font(.system(size: 11, weight: .semibold, design: .rounded))
                .lineLimit(1)

            if showAlertIcon {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 10, weight: .bold))
                    .foregroundStyle(Color.orange.opacity(0.95))
            }
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(
            Capsule()
                .fill(Color.primary.opacity(0.05))
        )
        .overlay(
            Capsule()
                .stroke(statusColor.opacity(0.22), lineWidth: 1)
        )
        .help(helpText)
    }

    private var showAlertIcon: Bool {
        cluster.isEnabled && !(reachability.lastErrorMessage?.isEmpty ?? true)
    }

    private var statusColor: Color {
        guard cluster.isEnabled else { return .secondary.opacity(0.8) }

        switch reachability.status {
        case .checking:
            return .orange.opacity(0.95)
        case .reachable:
            return .green.opacity(0.95)
        case .unreachable:
            return .red.opacity(0.95)
        }
    }

    private var statusText: String {
        guard cluster.isEnabled else { return "Disabled" }

        switch reachability.status {
        case .checking:
            return "Checking"
        case .reachable:
            return "Reachable"
        case .unreachable:
            return "Unreachable"
        }
    }

    private var helpText: String {
        var parts = [
            cluster.displayName,
            cluster.sshAlias.isEmpty ? "SSH alias not configured" : "Alias: \(cluster.sshAlias)",
            "Status: \(statusText)"
        ]

        if cluster.isEnabled {
            parts.append(JobFormatting.refreshText(reachability.lastSuccessfulRefresh))
            if let lastError = reachability.lastErrorMessage, !lastError.isEmpty {
                parts.append("Last error: \(lastError)")
            }
        } else {
            parts.append("Disabled clusters keep watched jobs visible but stop refreshing.")
        }

        return parts.joined(separator: "\n")
    }
}
