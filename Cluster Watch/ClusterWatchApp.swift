import SwiftUI

@main
struct ClusterWatchApp: App {
    private enum WindowID {
        static let settings = "settings"
        static let logTail = "log-tail"
        static let launchCommand = "launch-command"
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

        Window("Job Log Tail", id: WindowID.logTail) {
            JobLogTailWindowView(store: store)
                .task {
                    await store.bootstrap()
                }
        }
        .defaultSize(width: 860, height: 560)

        Window("Launch Command", id: WindowID.launchCommand) {
            JobLaunchCommandWindowView(store: store)
                .task {
                    await store.bootstrap()
                }
        }
        .defaultSize(width: 900, height: 620)
    }
}

private struct JobLogTailWindowView: View {
    @Bindable var store: JobStore
    @State private var selectedStream: JobLogStream = .stdout
    @State private var logText: String = ""
    @State private var errorText: String?
    @State private var lastLoadedAt: Date?
    @State private var isLoading = false
    @State private var autoScrolledTaskIDs: Set<String> = []
    @State private var pendingAutoScrollTaskID: String?

    var body: some View {
        Group {
            if let session = store.activeLogTail {
                content(for: session)
                    .task(id: taskID(for: session)) {
                        await pollLog(for: session)
                    }
            } else {
                ContentUnavailableView(
                    "No Log Selected",
                    systemImage: "doc.text.magnifyingglass",
                    description: Text("Choose Tail on a job row to open its stdout or stderr here.")
                )
                .padding(24)
            }
        }
        .frame(minWidth: 760, minHeight: 460)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private func content(for session: JobLogTailSession) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(session.jobName)
                        .font(.system(.title3, design: .rounded, weight: .semibold))
                    Text("\(session.clusterName) • #\(session.jobID)")
                        .font(.system(.subheadline, design: .rounded))
                        .foregroundStyle(.secondary)
                }

                Spacer(minLength: 12)

                if session.availableStreams.count > 1 {
                    Picker("Log Stream", selection: $selectedStream) {
                        ForEach(session.availableStreams) { stream in
                            Text(stream.title).tag(stream)
                        }
                    }
                    .pickerStyle(.segmented)
                    .frame(width: 180)
                }

                Button("Refresh") {
                    Task {
                        await refreshLog(for: session, stream: selectedStream)
                    }
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            }

            if let path = session.path(for: selectedStream) {
                Text(path)
                    .font(.system(.caption, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            HStack(spacing: 10) {
                if isLoading {
                    ProgressView()
                        .controlSize(.small)
                }

                if let lastLoadedAt {
                    Text("Updated \(JobFormatting.absoluteDateText(lastLoadedAt))")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                } else {
                    Text("Waiting for first log fetch")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                }

                Spacer(minLength: 0)
            }

            if let errorText {
                Text(errorText)
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.red)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 8)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(
                        RoundedRectangle(cornerRadius: 10, style: .continuous)
                            .fill(Color.red.opacity(0.08))
                    )
            }

            ScrollViewReader { proxy in
                ScrollView([.vertical, .horizontal]) {
                    VStack(alignment: .leading, spacing: 0) {
                        Text(displayedLogText)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .topLeading)
                            .padding(12)

                        Color.clear
                            .frame(height: 1)
                            .id(bottomAnchorID(for: session, stream: selectedStream))
                    }
                }
                .onChange(of: pendingAutoScrollTaskID) { _, newValue in
                    guard newValue == taskID(for: session, stream: selectedStream) else { return }

                    DispatchQueue.main.async {
                        withAnimation(.easeOut(duration: 0.15)) {
                            proxy.scrollTo(bottomAnchorID(for: session, stream: selectedStream), anchor: .bottom)
                        }
                    }
                }
            }
            .background(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(Color.primary.opacity(0.05))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
        }
        .padding(16)
    }

    private var displayedLogText: String {
        logText.isEmpty ? "No log output yet." : logText
    }

    private func taskID(for session: JobLogTailSession, stream: JobLogStream? = nil) -> String {
        "\(session.clusterID.rawValue):\(session.jobID):\((stream ?? selectedStream).rawValue)"
    }

    private func bottomAnchorID(for session: JobLogTailSession, stream: JobLogStream) -> String {
        "\(taskID(for: session, stream: stream))-bottom"
    }

    @MainActor
    private func pollLog(for session: JobLogTailSession) async {
        let availableStreams = session.availableStreams
        if !availableStreams.contains(selectedStream) {
            selectedStream = availableStreams.first ?? session.preferredStream
        }

        logText = ""
        errorText = nil
        lastLoadedAt = nil

        while !Task.isCancelled {
            await refreshLog(for: session, stream: selectedStream)

            do {
                try await Task.sleep(nanoseconds: 2_000_000_000)
            } catch {
                break
            }
        }
    }

    @MainActor
    private func refreshLog(for session: JobLogTailSession, stream: JobLogStream) async {
        guard session.path(for: stream) != nil else {
            errorText = "No \(stream.title.lowercased()) path available for this job."
            logText = ""
            return
        }

        isLoading = true
        defer { isLoading = false }

        do {
            let output = try await store.tailLog(session: session, stream: stream, lineCount: 200)
            guard !Task.isCancelled else { return }
            logText = output
            errorText = nil
            lastLoadedAt = Date()

            let currentTaskID = taskID(for: session, stream: stream)
            if !autoScrolledTaskIDs.contains(currentTaskID) {
                autoScrolledTaskIDs.insert(currentTaskID)
                pendingAutoScrollTaskID = currentTaskID
            }
        } catch {
            guard !Task.isCancelled else { return }
            errorText = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
        }
    }
}

private struct JobLaunchCommandWindowView: View {
    @Bindable var store: JobStore
    @State private var selectedMode: JobLaunchMode = .command
    @State private var wrapsLines = true

    var body: some View {
        Group {
            if let session = store.activeLaunchCommand {
                content(for: session)
                    .task(id: session.id) {
                        selectedMode = session.preferredMode
                        wrapsLines = true
                    }
            } else {
                ContentUnavailableView(
                    "No Command Selected",
                    systemImage: "chevron.left.forwardslash.chevron.right",
                    description: Text("Choose the command action on a job row to inspect its launch command or batch script here.")
                )
                .padding(24)
            }
        }
        .frame(minWidth: 820, minHeight: 520)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private func content(for session: JobLaunchSession) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(session.jobName)
                        .font(.system(.title3, design: .rounded, weight: .semibold))
                    Text("\(session.clusterName) • #\(session.jobID)")
                        .font(.system(.subheadline, design: .rounded))
                        .foregroundStyle(.secondary)
                }

                Spacer(minLength: 12)

                if session.details.availableModes.count > 1 {
                    Picker("Content", selection: $selectedMode) {
                        ForEach(session.details.availableModes) { mode in
                            Text(mode.title).tag(mode)
                        }
                    }
                    .pickerStyle(.segmented)
                    .frame(width: 220)
                }

                Toggle("Wrap", isOn: $wrapsLines)
                    .toggleStyle(.switch)
                    .controlSize(.small)

                Button(isDisplayingFormattedContent(for: session) ? "Copy Raw" : "Copy") {
                    copyToPasteboard(text: rawContent(for: session))
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .disabled(rawContent(for: session).isEmpty)
            }

            if let workDirectory = session.details.workDirectory {
                Text("Working directory: \(workDirectory)")
                    .font(.system(.caption, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            if isDisplayingFormattedContent(for: session) {
                Text("Displayed with best-effort shell wrapping for readability.")
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)
            }

            ScrollView(wrapsLines ? .vertical : [.vertical, .horizontal]) {
                Text(verbatim: displayedContent(for: session).isEmpty ? "No launch command details available for this job." : displayedContent(for: session))
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .textSelection(.enabled)
                    .frame(maxWidth: wrapsLines ? .infinity : nil, alignment: .topLeading)
                    .fixedSize(horizontal: !wrapsLines, vertical: true)
                    .padding(12)
            }
            .background(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(Color.primary.opacity(0.05))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
        }
        .padding(16)
    }

    private func displayedContent(for session: JobLaunchSession) -> String {
        let raw = rawContent(for: session)
        guard !raw.isEmpty else { return "" }
        return JobFormatting.formattedLaunchContent(raw, mode: selectedMode)
    }

    private func rawContent(for session: JobLaunchSession) -> String {
        session.details.content(for: selectedMode) ?? ""
    }

    private func isDisplayingFormattedContent(for session: JobLaunchSession) -> Bool {
        let raw = rawContent(for: session)
        guard !raw.isEmpty else { return false }
        return displayedContent(for: session) != raw
    }

    private func copyToPasteboard(text: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(text, forType: .string)
    }
}
