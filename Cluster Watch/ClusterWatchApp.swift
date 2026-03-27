import SwiftUI

@main
struct ClusterWatchApp: App {
    private enum WindowID {
        static let settings = "settings"
    }

    @State private var store: JobStore

    init() {
        _store = State(initialValue: JobStore())
    }

    var body: some Scene {
        MenuBarExtra("Cluster Watch", systemImage: "dot.scope.display") {
            MenuBarRootView(store: store)
                .task {
                    await store.bootstrap()
                }
        }
        .menuBarExtraStyle(.window)

        Window("Cluster Watch Settings", id: WindowID.settings) {
            SettingsView(store: store)
                .task {
                    await store.bootstrap()
                }
        }
        .defaultSize(width: 720, height: 620)
    }
}
