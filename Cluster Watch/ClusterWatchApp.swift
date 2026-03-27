import SwiftUI

@main
struct ClusterWatchApp: App {
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

        Settings {
            SettingsView(store: store)
                .task {
                    await store.bootstrap()
                }
        }
    }
}
