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

struct ClusterLoadSectionView: View {
    let store: JobStore

    private var enabledClusters: [ClusterConfig] {
        store.clusters.filter(\.isEnabled)
    }

    var body: some View {
        PanelSection(title: "Cluster Load", systemImage: "gauge.with.dots.needle.50percent") {
            if enabledClusters.isEmpty {
                Text("Enable at least one cluster to see queue and free-resource summaries.")
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)
            } else {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(enabledClusters) { cluster in
                        ClusterLoadRowView(
                            cluster: cluster,
                            reachability: store.reachability(for: cluster.id),
                            load: store.clusterLoad(for: cluster.id)
                        )
                    }
                }
            }
        }
    }
}

private struct ClusterLoadRowView: View {
    let cluster: ClusterConfig
    let reachability: ClusterReachabilityState
    let load: ClusterLoadSnapshot

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(cluster.displayName)
                    .font(.system(size: 12, weight: .semibold, design: .rounded))
                    .lineLimit(1)

                Spacer(minLength: 8)

                Text(statusTitle)
                    .font(.system(size: 11, weight: .semibold, design: .rounded))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(
                        Capsule()
                            .fill(statusColor.opacity(0.12))
                    )
                    .overlay(
                        Capsule()
                            .stroke(statusColor.opacity(0.24), lineWidth: 1)
                    )
                    .foregroundStyle(statusColor)
            }

            Text(summaryText)
                .font(.system(size: 11.5, weight: .medium, design: .rounded))
                .foregroundStyle(.primary.opacity(0.85))
                .lineLimit(2)

            if let secondaryText, !secondaryText.isEmpty {
                Text(secondaryText)
                    .font(.system(size: 10.5, weight: .medium, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }

            if let qosText, !qosText.isEmpty {
                Text(qosText)
                    .font(.system(size: 10.5, weight: .medium, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 9)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .stroke(statusColor.opacity(0.12), lineWidth: 1)
        )
        .help(helpText)
    }

    private var effectiveLevel: ClusterLoadLevel {
        if reachability.status == .checking {
            return .busy
        }
        if reachability.status == .unreachable {
            return .unknown
        }
        return load.level
    }

    private var statusTitle: String {
        if reachability.status == .checking {
            return "Checking"
        }
        return effectiveLevel.title
    }

    private var summaryText: String {
        if reachability.status == .unreachable {
            return "Load unavailable until the cluster becomes reachable again."
        }
        if reachability.status == .checking {
            return "Refreshing queue depth and free resources…"
        }
        if load.lastUpdatedAt == nil {
            return "Load summary is not available yet."
        }
        return load.summaryText
    }

    private var secondaryText: String? {
        if reachability.status == .unreachable {
            return reachability.lastErrorMessage
        }

        var parts: [String] = []

        if let pendingJobCount = load.pendingJobCount, pendingJobCount > 0 {
            parts.append("Pending \(pendingJobCount)")
        }

        if let partitionsText = partitionsText {
            parts.append(partitionsText)
        }

        if let resourceText = load.detailResourceText, !resourceText.isEmpty {
            parts.append(resourceText)
        }

        if parts.isEmpty, let message = load.message ?? reachability.lastErrorMessage {
            return message
        }

        return parts.isEmpty ? nil : parts.joined(separator: " • ")
    }

    private var qosText: String? {
        guard reachability.status == .reachable else { return nil }
        return load.qosSummaryText
    }

    private var partitionsText: String? {
        guard !load.accessiblePartitions.isEmpty else { return nil }
        return "Scope: \(load.accessiblePartitions.joined(separator: ", "))"
    }

    private var statusColor: Color {
        switch effectiveLevel {
        case .open:
            return .green.opacity(0.95)
        case .busy:
            return .orange.opacity(0.95)
        case .constrained:
            return .orange
        case .full:
            return .red.opacity(0.95)
        case .unknown:
            return .secondary.opacity(0.9)
        }
    }

    private var helpText: String {
        var parts = [
            cluster.displayName,
            "Status: \(statusTitle)",
            summaryText
        ]

        if let secondaryText, !secondaryText.isEmpty {
            parts.append(secondaryText)
        }

        if let updated = load.lastUpdatedAt {
            parts.append("Updated: \(JobFormatting.absoluteDateText(updated))")
        }

        return parts.joined(separator: "\n")
    }
}
