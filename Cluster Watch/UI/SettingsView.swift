import SwiftUI

struct SettingsView: View {
    @State private var draft = SettingsDraft()
    @State private var saveMessage = ""
    @State private var hasLoadedDraft = false

    let store: JobStore

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color.cyan.opacity(0.14),
                    Color.mint.opacity(0.08),
                    Color.clear
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(spacing: 0) {
                ScrollView {
                    VStack(alignment: .leading, spacing: 18) {
                        header
                        monitoringCard

                        if draft.clusters.isEmpty {
                            emptyClustersCard
                        } else {
                            ForEach(draft.clusters.indices, id: \.self) { index in
                                clusterCard($draft.clusters[index], index: index)
                            }
                        }
                    }
                    .padding(24)
                }

                footer
            }
        }
        .frame(minWidth: 680, idealWidth: 720, minHeight: 560, idealHeight: 620)
        .background(Color(nsColor: .windowBackgroundColor))
        .onAppear {
            guard !hasLoadedDraft else { return }
            draft = SettingsDraft(from: store)
            saveMessage = ""
            hasLoadedDraft = true
        }
    }

    private var header: some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 6) {
                Text("Cluster Watch Settings")
                    .font(.system(size: 28, weight: .bold, design: .rounded))
                Text("Configure cluster aliases, usernames, and polling without leaving the menu bar workflow.")
                    .font(.system(.subheadline, design: .rounded))
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Button {
                draft.clusters.append(.empty(index: draft.clusters.count + 1))
            } label: {
                Label("Add Cluster", systemImage: "plus")
            }
            .buttonStyle(.borderedProminent)
        }
    }

    private var monitoringCard: some View {
        SettingsCard(title: "Monitoring", subtitle: "The default username filter is used for all enabled clusters unless a cluster-specific override is set.") {
            SettingsTextRow(
                title: "Default username filter",
                prompt: "username",
                text: $draft.globalUsernameFilter
            )

            SettingsStepperRow(
                title: "Poll interval",
                valueText: "\(Int(draft.pollIntervalSeconds)) seconds",
                value: $draft.pollIntervalSeconds,
                range: 5...300,
                step: 5
            )
        }
    }

    private var emptyClustersCard: some View {
        SettingsCard(
            title: "No Clusters Configured",
            subtitle: "Add one or more Slurm clusters. Each cluster should map to a working SSH alias in your local `~/.ssh/config`."
        ) {
            Button {
                draft.clusters.append(.empty(index: 1))
            } label: {
                Label("Add Your First Cluster", systemImage: "plus")
            }
            .buttonStyle(.borderedProminent)
        }
    }

    private func clusterCard(_ cluster: Binding<ClusterDraft>, index: Int) -> some View {
        let hasWatchedJobs = store.hasWatchedJobs(for: cluster.wrappedValue.id)

        return SettingsCard(
            title: cluster.wrappedValue.displayName.trimmedOrEmpty.isEmpty ? "Cluster \(index + 1)" : cluster.wrappedValue.displayName.trimmedOrEmpty,
            subtitle: "Aliases should match working `ssh` entries in your local `~/.ssh/config`."
        ) {
            HStack {
                Toggle(isOn: cluster.isEnabled) {
                    Text("Enabled")
                        .font(.system(.body, design: .rounded, weight: .medium))
                }
                .toggleStyle(.switch)

                Spacer()

                Button(role: .destructive) {
                    draft.clusters.remove(at: index)
                } label: {
                    Label("Remove Cluster", systemImage: "trash")
                }
                .buttonStyle(.bordered)
                .disabled(hasWatchedJobs)
            }
            .padding(.bottom, 4)

            SettingsTextRow(
                title: "Display name",
                prompt: "My Cluster",
                text: cluster.displayName
            )

            SettingsTextRow(
                title: "SSH alias",
                prompt: "mycluster",
                text: cluster.sshAlias,
                usesMonospacedFont: true
            )

            SettingsTextRow(
                title: "SSH username override",
                prompt: "optional",
                text: cluster.sshUsername,
                usesMonospacedFont: true
            )

            SettingsTextRow(
                title: "Job owner override",
                prompt: "optional",
                text: cluster.usernameOverride,
                usesMonospacedFont: true
            )

            SettingsOptionalIntegerRow(
                title: "Usable GPU cap",
                prompt: "optional",
                text: cluster.usableGPUCap
            )

            SettingsOptionalIntegerRow(
                title: "Usable node cap",
                prompt: "optional",
                text: cluster.usableNodeCap
            )

            Text("Optional caps override cluster-wide free counts when Slurm does not expose your account limit directly.")
                .font(.system(.caption, design: .rounded))
                .foregroundStyle(.secondary)

            if hasWatchedJobs {
                Text("Unwatch this cluster's jobs before removing it.")
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var footer: some View {
        HStack(spacing: 12) {
            if !saveMessage.isEmpty {
                Text(saveMessage)
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }

            Spacer()

            Button("Reload") {
                draft = SettingsDraft(from: store)
                saveMessage = "Reloaded current settings."
                hasLoadedDraft = true
            }
            .buttonStyle(.bordered)

            Button {
                draft.clusters.append(.empty(index: draft.clusters.count + 1))
            } label: {
                Label("Add Cluster", systemImage: "plus")
            }
            .buttonStyle(.bordered)

            Button("Save") {
                store.applySettings(
                    clusters: draft.clusters.map(\.asClusterConfig),
                    globalUsernameFilter: draft.globalUsernameFilter,
                    pollIntervalSeconds: draft.pollIntervalSeconds
                )
                saveMessage = "Saved. Refreshing clusters…"
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
        }
        .padding(.horizontal, 24)
        .padding(.vertical, 16)
        .background(.ultraThinMaterial)
        .overlay(alignment: .top) {
            Divider()
        }
    }
}

private struct SettingsCard<Content: View>: View {
    let title: String
    let subtitle: String?
    @ViewBuilder var content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.system(.title3, design: .rounded, weight: .semibold))

                if let subtitle, !subtitle.isEmpty {
                    Text(subtitle)
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                }
            }

            content
        }
        .padding(18)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(Color.primary.opacity(0.045))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color.primary.opacity(0.08), lineWidth: 1)
        )
    }
}

private struct SettingsTextRow: View {
    let title: String
    let prompt: String
    @Binding var text: String
    var usesMonospacedFont: Bool = false

    var body: some View {
        HStack(alignment: .center, spacing: 18) {
            Text(title)
                .font(.system(.body, design: .rounded, weight: .medium))
                .frame(width: 190, alignment: .leading)

            TextField(prompt, text: $text)
                .textFieldStyle(.roundedBorder)
                .font(usesMonospacedFont ? .system(.body, design: .monospaced) : .system(.body, design: .rounded))
                .frame(maxWidth: .infinity)
        }
    }
}

private struct SettingsStepperRow: View {
    let title: String
    let valueText: String
    @Binding var value: Double
    let range: ClosedRange<Double>
    let step: Double

    var body: some View {
        HStack(alignment: .center, spacing: 18) {
            Text(title)
                .font(.system(.body, design: .rounded, weight: .medium))
                .frame(width: 190, alignment: .leading)

            HStack {
                Text(valueText)
                    .foregroundStyle(.secondary)
                Spacer()
                Stepper("", value: $value, in: range, step: step)
                    .labelsHidden()
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 10)
            .frame(maxWidth: .infinity)
            .background(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(Color(nsColor: .textBackgroundColor))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
        }
    }
}

private struct SettingsOptionalIntegerRow: View {
    let title: String
    let prompt: String
    @Binding var text: String

    var body: some View {
        HStack(alignment: .center, spacing: 18) {
            Text(title)
                .font(.system(.body, design: .rounded, weight: .medium))
                .frame(width: 190, alignment: .leading)

            TextField(prompt, text: $text)
                .textFieldStyle(.roundedBorder)
                .font(.system(.body, design: .monospaced))
                .frame(maxWidth: .infinity)
        }
    }
}

private struct SettingsDraft {
    var globalUsernameFilter: String = NSUserName()
    var pollIntervalSeconds: Double = 30
    var clusters: [ClusterDraft] = []

    init() {}

    @MainActor
    init(from store: JobStore) {
        globalUsernameFilter = store.globalUsernameFilter
        pollIntervalSeconds = store.pollIntervalSeconds
        clusters = store.clusters.map(ClusterDraft.init(cluster:))
    }
}

private struct ClusterDraft: Identifiable {
    let id: ClusterID
    var displayName: String
    var sshAlias: String
    var sshUsername: String
    var isEnabled: Bool
    var usernameOverride: String
    var usableGPUCap: String
    var usableNodeCap: String

    init(cluster: ClusterConfig) {
        id = cluster.id
        displayName = cluster.displayName
        sshAlias = cluster.sshAlias
        sshUsername = cluster.sshUsername
        isEnabled = cluster.isEnabled
        usernameOverride = cluster.usernameOverride
        usableGPUCap = cluster.usableGPUCap.map(String.init) ?? ""
        usableNodeCap = cluster.usableNodeCap.map(String.init) ?? ""
    }

    var asClusterConfig: ClusterConfig {
        ClusterConfig(
            id: id,
            displayName: displayName,
            sshAlias: sshAlias,
            sshUsername: sshUsername,
            isEnabled: isEnabled,
            usernameOverride: usernameOverride,
            usableGPUCap: parsedLimit(from: usableGPUCap),
            usableNodeCap: parsedLimit(from: usableNodeCap)
        )
    }

    static func empty(index: Int) -> ClusterDraft {
        ClusterDraft(
            cluster: ClusterConfig.empty(named: "Cluster \(index)")
        )
    }

    private func parsedLimit(from rawValue: String) -> Int? {
        let value = rawValue.trimmedOrEmpty
        guard let parsed = Int(value), parsed > 0 else { return nil }
        return parsed
    }
}
