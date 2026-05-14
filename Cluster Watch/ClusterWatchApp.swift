import SwiftUI
import AppKit

@main
struct ClusterWatchApp: App {
    private enum WindowID {
        static let settings = "settings"
        static let jobInspector = "job-inspector"
        static let dashboard = "dashboard"
    }

    @State private var store: JobStore

    init() {
        let store = JobStore()
        _store = State(initialValue: store)

        Task {
            await store.bootstrap()
        }
    }

    var body: some Scene {
        MenuBarExtra("Cluster Watch", systemImage: "dot.scope.display") {
            MenuBarRootView(store: store)
        }
        .menuBarExtraStyle(.window)

        Window("Cluster Watch Settings", id: WindowID.settings) {
            SettingsView(store: store)
        }
        .defaultSize(width: 720, height: 620)

        Window("Job Inspector", id: WindowID.jobInspector) {
            JobInspectorWindowView(store: store)
        }
        .defaultSize(width: 900, height: 620)

        Window("Cluster Watch Dashboard", id: WindowID.dashboard) {
            DashboardWindowView(store: store)
        }
        .defaultSize(width: 1520, height: 920)
        .windowResizability(.contentMinSize)
    }
}

private struct JobInspectorWindowView: View {
    @Bindable var store: JobStore

    var body: some View {
        JobInspectorContentView(store: store)
            .frame(minWidth: 860, minHeight: 560)
            .background(Color(nsColor: .windowBackgroundColor))
    }
}

private struct JobInspectorContentView: View {
    private enum InspectorPrimarySelection: String, CaseIterable, Identifiable {
        case stdout
        case stderr
        case command

        var id: String { rawValue }

        var title: String {
            switch self {
            case .stdout:
                return "Stdout"
            case .stderr:
                return "Stderr"
            case .command:
                return "Command"
            }
        }
    }

    @Bindable var store: JobStore
    @State private var selectedTab: JobInspectorTab = .logs
    @State private var selectedStream: JobLogStream = .stdout
    @State private var selectedMode: JobLaunchMode = .command
    @State private var lineCount: Int = 200
    @State private var grepFilter: String = ""
    @State private var logText: String = ""
    @State private var displayedLogText: String = "No log output yet."
    @State private var errorText: String?
    @State private var filterErrorText: String?
    @State private var lastLoadedAt: Date?
    @State private var isLoadingLogs = false
    @State private var isLoadingCommand = false
    @State private var wrapsLines = true
    @State private var autoScrolledTaskIDs: Set<String> = []
    @State private var pendingAutoScrollTaskID: String?
    @State private var contentTask: Task<Void, Never>?

    var body: some View {
        Group {
            if let session = store.activeInspector {
                content(for: session)
                    .task(id: session.id) {
                        configure(for: session)
                    }
                    .onDisappear {
                        contentTask?.cancel()
                        contentTask = nil
                    }
            } else {
                ContentUnavailableView(
                    "No Job Selected",
                    systemImage: "doc.text.magnifyingglass",
                    description: Text("Choose Tail on a job row to inspect the job here.")
                )
                .padding(24)
            }
        }
    }

    private func content(for session: JobInspectorSession) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            header(for: session)

            if selectedTab == .logs {
                logsContent(for: session)
            } else {
                commandContent(for: session)
            }
        }
        .padding(16)
        .onChange(of: selectedTab) { _, newValue in
            store.updateActiveInspectorPreferredTab(newValue)
            startSelectedContentTask()
        }
        .onChange(of: selectedStream) { _, newValue in
            store.updateActiveInspectorPreferredLogStream(newValue)
            guard selectedTab == .logs else { return }
            startSelectedContentTask()
        }
        .onChange(of: selectedMode) { _, newValue in
            store.updateActiveInspectorPreferredLaunchMode(newValue)
        }
    }

    private func header(for session: JobInspectorSession) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 12) {
            VStack(alignment: .leading, spacing: 4) {
                Text(session.jobName)
                    .font(.system(.title3, design: .rounded, weight: .semibold))
                Text("\(session.clusterName) • #\(session.jobID)")
                    .font(.system(.subheadline, design: .rounded))
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func logsContent(for session: JobInspectorSession) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                primarySelectionPicker
                    .frame(width: 240)

                HStack(spacing: 6) {
                    Text("Lines")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)

                    TextField("200", value: lineCountBinding, format: .number)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 64)
                        .onChange(of: lineCount) { _, _ in
                            refreshLogsIfValid()
                        }
                }

                HStack(spacing: 6) {
                    Text("Grep")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)

                    TextField("Step|loss|lr|epoch", text: $grepFilter)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 180)
                        .help("Case-insensitive regex filter. The app searches a deeper recent tail window remotely, then returns the latest matching lines.")
                        .onChange(of: grepFilter) { _, _ in
                            applyDisplayedLogText()
                            refreshLogsIfValid()
                        }
                }

                Button("Refresh") {
                    startSelectedContentTask()
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
                if isLoadingLogs {
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

            if let bannerText = errorText {
                errorBanner(text: bannerText)
            }

            if let filterErrorText {
                errorBanner(text: filterErrorText)
            }

            ReadOnlyCodeTextView(
                text: displayedLogText,
                wrapLines: false,
                autoScrollToken: pendingAutoScrollTaskID == taskID(for: session, stream: selectedStream)
                    ? pendingAutoScrollTaskID
                    : nil
            )
            .background(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(Color.primary.opacity(0.05))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
        }
    }

    private func commandContent(for session: JobInspectorSession) -> some View {
        let details = session.launchDetails

        return VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                primarySelectionPicker
                    .frame(width: 240)

                if let details, details.availableModes.count > 1 {
                    Picker("Content", selection: $selectedMode) {
                        ForEach(details.availableModes) { mode in
                            Text(mode.title).tag(mode)
                        }
                    }
                    .pickerStyle(.segmented)
                    .frame(width: 180)
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

                Button("Refresh") {
                    startSelectedContentTask()
                }
                .buttonStyle(.bordered)
                .controlSize(.small)

                Spacer(minLength: 0)

                if isLoadingCommand {
                    ProgressView()
                        .controlSize(.small)
                }
            }

            if let workDirectory = details?.workDirectory {
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

            ReadOnlyCodeTextView(
                text: displayedContent(for: session).isEmpty ? "No launch command details available for this job." : displayedContent(for: session),
                wrapLines: wrapsLines
            )
            .background(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(Color.primary.opacity(0.05))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color.primary.opacity(0.08), lineWidth: 1)
            )
        }
    }

    private var lineCountBinding: Binding<Int> {
        Binding(
            get: { lineCount },
            set: { newValue in
                lineCount = max(1, min(10_000, newValue))
            }
        )
    }

    private func configure(for session: JobInspectorSession) {
        contentTask?.cancel()
        selectedTab = session.preferredTab
        selectedStream = session.preferredLogStream
        selectedMode = session.preferredLaunchMode
        wrapsLines = true
        startSelectedContentTask()
    }

    private func startSelectedContentTask() {
        contentTask?.cancel()
        contentTask = Task {
            await loadSelectedContent()
        }
    }

    @MainActor
    private func loadSelectedContent() async {
        switch selectedTab {
        case .logs:
            await pollLogs()
        case .command:
            await loadCommandDetails()
        }
    }

    private func taskID(for session: JobInspectorSession, stream: JobLogStream? = nil) -> String {
        "\(session.clusterID.rawValue):\(session.jobID):\((stream ?? selectedStream).rawValue)"
    }

    @MainActor
    private func pollLogs() async {
        _ = await store.ensureActiveInspectorLogPaths()
        guard let session = store.activeInspector else { return }

        let availableStreams = session.availableStreams
        if !availableStreams.contains(selectedStream) {
            selectedStream = availableStreams.first ?? session.preferredLogStream
        }

        logText = ""
        displayedLogText = "No log output yet."
        filterErrorText = nil
        lastLoadedAt = nil
        applyDisplayedLogText()

        guard session.path(for: selectedStream) != nil else {
            errorText = "No stdout or stderr path available for this job."
            return
        }

        errorText = nil

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
    private func refreshLog(for session: JobInspectorSession, stream: JobLogStream) async {
        guard session.path(for: stream) != nil else {
            errorText = "No \(stream.title.lowercased()) path available for this job."
            logText = ""
            displayedLogText = "No log output yet."
            return
        }

        isLoadingLogs = true
        defer { isLoadingLogs = false }

        do {
            let output = try await store.tailLog(
                session: session,
                stream: stream,
                lineCount: lineCount,
                grepFilter: grepFilter.trimmedOrEmpty
            )
            guard !Task.isCancelled else { return }
            logText = output
            errorText = nil
            lastLoadedAt = Date()
            applyDisplayedLogText()

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

    @MainActor
    private func loadCommandDetails() async {
        isLoadingCommand = true
        defer { isLoadingCommand = false }

        let details = await store.ensureActiveInspectorLaunchDetails()
        guard let session = store.activeInspector else { return }

        if let details, !details.availableModes.contains(selectedMode) {
            selectedMode = details.preferredMode
        } else if details == nil {
            selectedMode = session.preferredLaunchMode
        }
    }

    private func applyDisplayedLogText() {
        let result = Self.buildDisplayedLogText(
            rawLogText: logText,
            grepFilter: grepFilter
        )
        displayedLogText = result.text
        filterErrorText = result.errorText
    }

    private func refreshLogsIfValid() {
        guard selectedTab == .logs, isValidGrepPattern(grepFilter) else { return }
        startSelectedContentTask()
    }

    private func isValidGrepPattern(_ pattern: String) -> Bool {
        let trimmed = pattern.trimmedOrEmpty
        guard !trimmed.isEmpty else { return true }

        do {
            _ = try NSRegularExpression(pattern: trimmed, options: [.caseInsensitive])
            return true
        } catch {
            return false
        }
    }

    private func displayedContent(for session: JobInspectorSession) -> String {
        let raw = rawContent(for: session)
        guard !raw.isEmpty else { return "" }
        return JobFormatting.formattedLaunchContent(raw, mode: selectedMode)
    }

    private func rawContent(for session: JobInspectorSession) -> String {
        session.launchDetails?.content(for: selectedMode) ?? ""
    }

    private func isDisplayingFormattedContent(for session: JobInspectorSession) -> Bool {
        let raw = rawContent(for: session)
        guard !raw.isEmpty else { return false }
        return displayedContent(for: session) != raw
    }

    nonisolated private static func buildDisplayedLogText(
        rawLogText: String,
        grepFilter: String
    ) -> (text: String, errorText: String?) {
        let normalizedLogText = rawLogText
            .replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")

        let pattern = grepFilter.trimmedOrEmpty
        guard !pattern.isEmpty else {
            let text = normalizedLogText.isEmpty ? "No log output yet." : normalizedLogText
            return (text, nil)
        }

        do {
            _ = try NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
        } catch {
            let text = normalizedLogText.isEmpty ? "No log output yet." : normalizedLogText
            return (text, "Invalid grep pattern: \(error.localizedDescription)")
        }

        if normalizedLogText.trimmedOrEmpty.isEmpty {
            return ("No log lines matched the current filter.", nil)
        }

        return (normalizedLogText, nil)
    }

    private func copyToPasteboard(text: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(text, forType: .string)
    }

    private func errorBanner(text: String) -> some View {
        Text(text)
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

    private var primarySelectionPicker: some View {
        Picker("Inspector", selection: primarySelectionBinding) {
            ForEach(availablePrimarySelections, id: \.id) { selection in
                Text(selection.title).tag(selection)
            }
        }
        .pickerStyle(.segmented)
    }

    private var availablePrimarySelections: [InspectorPrimarySelection] {
        if let session = store.activeInspector {
            var selections = session.availableStreams.map { stream in
                switch stream {
                case .stdout:
                    return InspectorPrimarySelection.stdout
                case .stderr:
                    return InspectorPrimarySelection.stderr
                }
            }
            selections.append(.command)
            return selections
        }

        return InspectorPrimarySelection.allCases
    }

    private var primarySelectionBinding: Binding<InspectorPrimarySelection> {
        Binding(
            get: {
                if selectedTab == .command {
                    return .command
                }

                return selectedStream == .stderr ? .stderr : .stdout
            },
            set: { newValue in
                switch newValue {
                case .stdout:
                    selectedStream = .stdout
                    selectedTab = .logs
                case .stderr:
                    selectedStream = .stderr
                    selectedTab = .logs
                case .command:
                    selectedTab = .command
                }
            }
        )
    }
}

private struct DashboardWindowView: View {
    @Bindable var store: JobStore
    @Environment(\.openWindow) private var openWindow

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

            VStack(alignment: .leading, spacing: 12) {
                header

                TimelineView(.periodic(from: .now, by: store.timelineTickIntervalSeconds)) { context in
                    dashboardContent(now: context.date)
                }
            }
            .padding(16)
        }
        .frame(minWidth: 1380, minHeight: 860)
        .background(Color(nsColor: .windowBackgroundColor))
        .background(
            WindowConfigurator { window in
                window.styleMask.formUnion([.titled, .closable, .miniaturizable, .resizable])
                window.collectionBehavior.formUnion([.managed, .fullScreenPrimary, .fullScreenAllowsTiling])
                window.tabbingMode = .disallowed
                window.titleVisibility = .visible
                window.standardWindowButton(.zoomButton)?.isEnabled = true
            }
        )
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Cluster Watch Dashboard")
                        .font(.system(.largeTitle, design: .rounded, weight: .bold))
                    Text("Live watched-job monitoring with an embedded inspector for a dedicated display.")
                        .font(.system(.subheadline, design: .rounded))
                        .foregroundStyle(.secondary)
                }

                Spacer(minLength: 16)

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
                    .disabled(!store.hasTerminalDisplayedWatchedJobs)

                    Button {
                        NSApp.activate(ignoringOtherApps: true)
                        openWindow(id: "settings")
                    } label: {
                        Text("Settings")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)

                    Button {
                        NSApp.keyWindow?.toggleFullScreen(nil)
                    } label: {
                        Text(fullScreenButtonTitle)
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.small)
                }
            }

            ClusterStatusIndicatorsView(store: store)
        }
    }

    private func dashboardContent(now: Date) -> some View {
        HStack(alignment: .top, spacing: 12) {
            VStack(alignment: .leading, spacing: 12) {
                ClusterLoadSectionView(store: store)
                Spacer(minLength: 0)
            }
            .frame(width: 340)

            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    WatchedJobsSectionView(
                        store: store,
                        now: now,
                        openLogTailWindow: {}
                    )
                    BrowseJobsSectionView(
                        store: store,
                        now: now,
                        openLogTailWindow: {}
                    )
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)

            PanelSection(title: "Job Inspector", systemImage: "doc.text.magnifyingglass") {
                JobInspectorContentView(store: store)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            }
            .frame(width: 560)
            .frame(maxHeight: .infinity, alignment: .top)
        }
    }

    private var fullScreenButtonTitle: String {
        if NSApp.keyWindow?.styleMask.contains(.fullScreen) == true {
            return "Exit Full Screen"
        }
        return "Full Screen"
    }
}

private struct WindowConfigurator: NSViewRepresentable {
    let configure: (NSWindow) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> NSView {
        NSView(frame: .zero)
    }

    func updateNSView(_ nsView: NSView, context: Context) {
        if let window = nsView.window, context.coordinator.configuredWindow !== window {
            context.coordinator.configuredWindow = window
            configure(window)
        }
    }

    final class Coordinator {
        weak var configuredWindow: NSWindow?
    }
}

private struct ReadOnlyCodeTextView: NSViewRepresentable {
    let text: String
    let wrapLines: Bool
    var autoScrollToken: String? = nil

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.borderType = .noBorder
        scrollView.drawsBackground = false
        scrollView.autohidesScrollers = true

        let textView = NSTextView()
        textView.isEditable = false
        textView.isRichText = false
        textView.importsGraphics = false
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.isAutomaticTextReplacementEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.allowsUndo = false
        textView.isVerticallyResizable = true
        textView.isHorizontallyResizable = true
        textView.drawsBackground = false
        textView.textContainerInset = NSSize(width: 12, height: 12)
        textView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        textView.string = text

        configure(textView: textView)

        scrollView.documentView = textView
        context.coordinator.textView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = context.coordinator.textView else { return }

        configure(textView: textView)

        if textView.string != text {
            textView.string = text
        }

        if context.coordinator.lastAutoScrollToken != autoScrollToken, autoScrollToken != nil {
            context.coordinator.lastAutoScrollToken = autoScrollToken
            DispatchQueue.main.async {
                textView.scrollToEndOfDocument(nil)
            }
        }
    }

    private func configure(textView: NSTextView) {
        textView.textContainer?.containerSize = NSSize(
            width: wrapLines ? 0 : CGFloat.greatestFiniteMagnitude,
            height: CGFloat.greatestFiniteMagnitude
        )
        textView.textContainer?.widthTracksTextView = wrapLines
        textView.isHorizontallyResizable = !wrapLines
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.minSize = .zero
    }

    final class Coordinator {
        var textView: NSTextView?
        var lastAutoScrollToken: String?
    }
}
