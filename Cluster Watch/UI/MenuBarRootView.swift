import SwiftUI
import AppKit

struct MenuBarRootView: View {
    @Bindable var store: JobStore
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color.cyan.opacity(0.18),
                    Color.mint.opacity(0.10),
                    Color.clear
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    header

                    TimelineView(.periodic(from: .now, by: 1)) { context in
                        VStack(alignment: .leading, spacing: 12) {
                            WatchedJobsSectionView(
                                store: store,
                                now: context.date,
                                openLogTailWindow: {
                                    NSApp.activate(ignoringOtherApps: true)
                                    openWindow(id: "log-tail")
                                },
                                openLaunchCommandWindow: {
                                    NSApp.activate(ignoringOtherApps: true)
                                    openWindow(id: "launch-command")
                                }
                            )
                            BrowseJobsSectionView(
                                store: store,
                                now: context.date,
                                openLogTailWindow: {
                                    NSApp.activate(ignoringOtherApps: true)
                                    openWindow(id: "log-tail")
                                },
                                openLaunchCommandWindow: {
                                    NSApp.activate(ignoringOtherApps: true)
                                    openWindow(id: "launch-command")
                                }
                            )
                            ClusterLoadSectionView(store: store)
                        }
                    }
                }
                .padding(12)
            }
        }
        .frame(width: 440, height: 680)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Cluster Watch")
                        .font(.system(.title3, design: .rounded, weight: .semibold))
                    Text("Watched jobs stay pinned until you remove them.")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                }

                Spacer()

                HStack(spacing: 8) {
                    Button {
                        Task {
                            await store.refreshAll()
                        }
                    } label: {
                        Text("Refresh")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)

                    Button {
                        store.clearCompleted()
                    } label: {
                        Text("Clear Completed")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                    .disabled(!store.watchedJobs.contains(where: { $0.isTerminal }))

                    Button {
                        NSApp.activate(ignoringOtherApps: true)
                        openWindow(id: "settings")
                    } label: {
                        Text("Settings")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                }
            }

            ClusterStatusIndicatorsView(store: store)
        }
    }
}

struct PanelSection<Content: View>: View {
    let title: String
    let systemImage: String
    @ViewBuilder var content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Label(title, systemImage: systemImage)
                .font(.system(.headline, design: .rounded, weight: .semibold))
                .foregroundStyle(.primary)

            content
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color.primary.opacity(0.045))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .stroke(Color.primary.opacity(0.08), lineWidth: 1)
        )
    }
}
