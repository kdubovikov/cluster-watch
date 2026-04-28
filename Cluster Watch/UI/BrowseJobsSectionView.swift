import SwiftUI

struct BrowseJobsSectionView: View {
    @Bindable var store: JobStore
    let now: Date
    let openLogTailWindow: () -> Void

    var body: some View {
        let visibleJobs = store.visibleCurrentJobs
        let groupedJobs = GroupedJobsViewModel.currentGroups(for: visibleJobs, maxDisplayedRows: 10)
        let displayedRowCount = groupedJobs.reduce(into: 0) { $0 += $1.rows.count }

        PanelSection(
            title: "Browse Unwatched Jobs",
            systemImage: "magnifyingglass",
            headerAccessory: {
                if !visibleJobs.isEmpty {
                    Button {
                        store.watch(jobs: visibleJobs)
                    } label: {
                        Label("Watch All", systemImage: "plus")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                    .help("Watch all currently visible unwatched jobs")
                }
            }
        ) {
            VStack(alignment: .leading, spacing: 8) {
                TextField("Search by job id, job name, or cluster", text: $store.browseSearchText)
                    .textFieldStyle(.roundedBorder)

                Text("Filtered to your configured username on each enabled cluster.")
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)

                if groupedJobs.isEmpty {
                    Text("No unwatched jobs matched the current filter.")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                        .padding(.vertical, 6)
                } else {
                    VStack(spacing: 6) {
                        ForEach(groupedJobs) { group in
                            if group.isDependencyLinked {
                                CurrentDependencyLinkedJobGroupView(
                                    group: group,
                                    store: store,
                                    allVisibleJobs: visibleJobs,
                                    now: now,
                                    openLogTailWindow: openLogTailWindow
                                )
                            } else if let job = group.jobs.first {
                                BrowseJobRowView(
                                    job: job,
                                    clusterName: store.clusterName(for: job.clusterID),
                                    upstreamJobs: dependencies(for: job, within: visibleJobs),
                                    downstreamJobs: dependents(for: job, within: visibleJobs),
                                    hasDetectedLogPaths: job.state != .pending && store.logPaths(for: job)?.hasAnyPath == true,
                                    now: now,
                                    tailAction: {
                                        Task {
                                            if await store.prepareLogTail(for: job) {
                                                openLogTailWindow()
                                            }
                                        }
                                    },
                                    cancelAction: {
                                        await store.cancel(job: job)
                                    },
                                    watchAction: {
                                        store.watch(job: job)
                                    }
                                )
                                .task(id: job.id) {
                                    await store.prefetchLogPaths(for: job)
                                }
                            }
                        }

                        if visibleJobs.count > displayedRowCount {
                            Text("Showing first \(displayedRowCount) of \(visibleJobs.count) unwatched jobs.")
                                .font(.system(.caption2, design: .rounded))
                                .foregroundStyle(.secondary)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }
                }
            }
        }
    }

    private func dependencies(for job: CurrentJob, within jobs: [CurrentJob]) -> [CurrentJob] {
        jobs
            .filter { candidate in
                candidate.clusterID == job.clusterID && candidate.id != job.id && job.depends(on: candidate.jobID)
            }
            .sorted(by: compareCurrentJobs)
    }

    private func dependents(for job: CurrentJob, within jobs: [CurrentJob]) -> [CurrentJob] {
        jobs
            .filter { candidate in
                candidate.clusterID == job.clusterID && candidate.id != job.id && candidate.depends(on: job.jobID)
            }
            .sorted(by: compareCurrentJobs)
    }

    private func compareCurrentJobs(lhs: CurrentJob, rhs: CurrentJob) -> Bool {
        if lhs.state.sortPriority != rhs.state.sortPriority {
            return lhs.state.sortPriority < rhs.state.sortPriority
        }

        let lhsDate = lhs.startTime ?? lhs.submitTime ?? .distantPast
        let rhsDate = rhs.startTime ?? rhs.submitTime ?? .distantPast

        if lhsDate != rhsDate {
            return lhsDate > rhsDate
        }

        return lhs.jobID > rhs.jobID
    }
}

private struct CurrentDependencyLinkedJobGroupView: View {
    @State private var isConfirmingCancel = false
    @State private var isCancelling = false

    let group: GroupedJobsViewModel.CurrentGroup
    let store: JobStore
    let allVisibleJobs: [CurrentJob]
    let now: Date
    let openLogTailWindow: () -> Void

    var body: some View {
        let coordinateSpaceName = "current-dependency-group-\(group.id)"

        VStack(alignment: .leading, spacing: 0) {
            ForEach(group.rows) { row in
                BrowseJobRowView(
                    job: row.job,
                    clusterName: store.clusterName(for: row.job.clusterID),
                    upstreamJobs: upstreamJobs(for: row.job),
                    downstreamJobs: downstreamJobs(for: row.job),
                    hasDetectedLogPaths: row.job.state != .pending && store.logPaths(for: row.job)?.hasAnyPath == true,
                    now: now,
                    displayStyle: .chain(depth: row.depth),
                    showsPrimaryAction: false,
                    reservedTrailingInset: groupHasCancellableJobs ? 48 : 22,
                    tailAction: {
                        Task {
                            if await store.prepareLogTail(for: row.job) {
                                openLogTailWindow()
                            }
                        }
                    },
                    cancelAction: {
                        await store.cancel(job: row.job)
                    },
                    watchAction: {
                        store.watch(job: row.job)
                    }
                )
                .task(id: row.id) {
                    await store.prefetchLogPaths(for: row.job)
                }
                .background {
                    GeometryReader { proxy in
                        Color.clear.preference(
                            key: CurrentDependencyRowFramePreferenceKey.self,
                            value: [
                                CurrentDependencyRowFrame(
                                    id: row.id,
                                    depth: row.depth,
                                    parentJobID: row.parentJobID,
                                    frame: proxy.frame(in: .named(coordinateSpaceName))
                                )
                            ]
                        )
                    }
                }
            }
        }
        .padding(10)
        .coordinateSpace(name: coordinateSpaceName)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .fill(Color.cyan.opacity(0.05))
        )
        .overlay(alignment: .topTrailing) {
            HStack(spacing: 8) {
                if groupHasCancellableJobs {
                    InlineCancelActionView(
                        isCompact: true,
                        isConfirming: $isConfirmingCancel,
                        isCancelling: $isCancelling,
                        confirmLabel: "Cancel Group",
                        action: {
                            await store.cancel(jobs: cancellableJobs)
                        }
                    )
                }

                Button {
                    store.watch(jobs: group.jobs)
                } label: {
                    Image(systemName: "plus.circle")
                        .accessibilityLabel("Watch Group")
                }
                .buttonStyle(.borderless)
                .controlSize(.small)
                .help("Watch whole group")
            }
            .padding(.top, 10)
            .padding(.trailing, 14)
        }
        .overlayPreferenceValue(CurrentDependencyRowFramePreferenceKey.self) { rows in
            CurrentDependencyChainOverlay(rows: rows)
        }
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .stroke(Color.cyan.opacity(0.16), lineWidth: 1)
        )
    }

    private func upstreamJobs(for job: CurrentJob) -> [CurrentJob] {
        allVisibleJobs
            .filter { candidate in
                candidate.clusterID == job.clusterID && candidate.id != job.id && job.depends(on: candidate.jobID)
            }
            .sorted(by: compareCurrentJobs)
    }

    private func downstreamJobs(for job: CurrentJob) -> [CurrentJob] {
        allVisibleJobs
            .filter { candidate in
                candidate.clusterID == job.clusterID && candidate.id != job.id && candidate.depends(on: job.jobID)
            }
            .sorted(by: compareCurrentJobs)
    }

    private func compareCurrentJobs(lhs: CurrentJob, rhs: CurrentJob) -> Bool {
        if lhs.state.sortPriority != rhs.state.sortPriority {
            return lhs.state.sortPriority < rhs.state.sortPriority
        }

        let lhsDate = lhs.startTime ?? lhs.submitTime ?? .distantPast
        let rhsDate = rhs.startTime ?? rhs.submitTime ?? .distantPast

        if lhsDate != rhsDate {
            return lhsDate > rhsDate
        }

        return lhs.jobID > rhs.jobID
    }

    private var cancellableJobs: [CurrentJob] {
        group.jobs.filter { !$0.state.isTerminal }
    }

    private var groupHasCancellableJobs: Bool {
        !cancellableJobs.isEmpty
    }
}

private struct CurrentDependencyRowFrame: Equatable {
    let id: String
    let depth: Int
    let parentJobID: String?
    let frame: CGRect
}

private struct CurrentDependencyRowFramePreferenceKey: PreferenceKey {
    static var defaultValue: [CurrentDependencyRowFrame] = []

    static func reduce(value: inout [CurrentDependencyRowFrame], nextValue: () -> [CurrentDependencyRowFrame]) {
        value.append(contentsOf: nextValue())
    }
}

private struct CurrentDependencyChainOverlay: View {
    let rows: [CurrentDependencyRowFrame]

    var body: some View {
        GeometryReader { _ in
            ZStack(alignment: .topLeading) {
                Path { path in
                    let rowsByID = Dictionary(uniqueKeysWithValues: rows.map { ($0.id, $0) })
                    let childrenByParent = Dictionary(grouping: rows.compactMap { row in
                        row.parentJobID.map { (parentID: $0, row: row) }
                    }) { $0.parentID }

                    for (parentID, children) in childrenByParent {
                        guard let parent = rowsByID[parentID] else { continue }

                        let parentPoint = nodePoint(for: parent)
                        let childPoints = children
                            .map(\.row)
                            .sorted { $0.frame.minY < $1.frame.minY }
                            .map(nodePoint(for:))

                        guard let lastChild = childPoints.last else { continue }

                        path.move(to: parentPoint)
                        path.addLine(to: CGPoint(x: parentPoint.x, y: lastChild.y))

                        for childPoint in childPoints {
                            path.move(to: CGPoint(x: parentPoint.x, y: childPoint.y))
                            path.addLine(to: childPoint)
                        }
                    }
                }
                .stroke(Color.cyan.opacity(0.42), style: StrokeStyle(lineWidth: 2, lineCap: .round, lineJoin: .round))

                ForEach(rows, id: \.id) { row in
                    Circle()
                        .fill(Color.cyan.opacity(0.92))
                        .frame(width: 10, height: 10)
                        .overlay(
                            Circle()
                                .stroke(Color.white.opacity(0.9), lineWidth: 1)
                        )
                        .position(nodePoint(for: row))
                }
            }
        }
        .allowsHitTesting(false)
    }

    private func nodePoint(for row: CurrentDependencyRowFrame) -> CGPoint {
        CGPoint(x: 16 + CGFloat(row.depth) * 18, y: row.frame.minY + 18)
    }
}

private struct BrowseJobRowView: View {
    @State private var isConfirmingCancel = false
    @State private var isCancelling = false

    enum DisplayStyle {
        case standalone
        case chain(depth: Int)

        var depth: Int {
            switch self {
            case .standalone:
                return 0
            case .chain(let depth):
                return depth
            }
        }

        var isChain: Bool {
            switch self {
            case .standalone:
                return false
            case .chain:
                return true
            }
        }
    }

    let job: CurrentJob
    let clusterName: String
    let upstreamJobs: [CurrentJob]
    let downstreamJobs: [CurrentJob]
    let hasDetectedLogPaths: Bool
    let now: Date
    var displayStyle: DisplayStyle = .standalone
    var showsPrimaryAction: Bool = true
    var reservedTrailingInset: CGFloat = 0
    let tailAction: () -> Void
    let cancelAction: () async -> Bool
    let watchAction: () -> Void

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            VStack(alignment: .leading, spacing: 3) {
                HStack(spacing: 5) {
                    Button {
                        copyToPasteboard(job.jobName)
                    } label: {
                        Text(job.jobName)
                            .font(.system(size: 13, weight: .semibold, design: .rounded))
                            .foregroundStyle(.primary)
                            .lineLimit(1)
                    }
                    .buttonStyle(.plain)
                    .help("Copy job name")
                    StateBadgeView(title: job.state.badgeTitle, style: job.state.badgeStyle)
                    if job.dependencyStatus == .waiting {
                        StateBadgeView(title: "Blocked", style: .pending)
                    }
                    if job.dependencyStatus == .neverSatisfied {
                        StateBadgeView(title: "Deps Broken", style: .failed)
                    }
                }

                HStack(spacing: 0) {
                    Text("\(clusterName) • ")
                    Button {
                        copyToPasteboard(job.jobID)
                    } label: {
                        Text("#\(job.jobID)")
                            .foregroundStyle(.secondary)
                    }
                    .buttonStyle(.plain)
                    .help("Copy job ID")
                    Text(" • \(job.owner)")
                }
                .font(.system(size: 12, weight: .medium, design: .rounded))
                .foregroundStyle(.secondary)
                Text("\(JobFormatting.startedText(for: job)) • \(JobFormatting.timingSummary(for: job, now: now))")
                    .font(.system(size: 11, weight: .regular, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.9)
                if let detailLine = primaryDetailLine {
                    Text(detailLine)
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(job.dependencyStatus == .neverSatisfied ? .red : .secondary)
                }
                if let secondaryDetailLine {
                    Text(secondaryDetailLine)
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(.secondary)
                }
            }

            Spacer(minLength: 8)

            actionButton
        }
        .padding(.leading, leadingInset)
        .padding(.vertical, displayStyle.isChain ? 9 : 8)
        .padding(.leading, displayStyle.isChain ? 6 : 8)
        .padding(.trailing, trailingInset)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(backgroundShape)
    }

    private var primaryDetailLine: String? {
        if displayStyle.isChain {
            return job.dependencyStatus == .none
                ? JobFormatting.pendingReasonSummary(job.pendingReason, dependencyStatus: job.dependencyStatus)
                : nil
        }

        if let dependencySummary = JobFormatting.dependencySummary(
            state: job.state,
            dependencyStatus: job.dependencyStatus,
            dependencyExpression: job.dependencyExpression,
            dependencyJobIDs: job.dependencyJobIDs,
            upstreamJobs: upstreamJobs
        ) {
            return dependencySummary
        }

        return JobFormatting.pendingReasonSummary(job.pendingReason, dependencyStatus: job.dependencyStatus)
    }

    private var secondaryDetailLine: String? {
        guard !displayStyle.isChain else { return nil }
        return JobFormatting.downstreamSummary(downstreamJobs)
    }

    private var leadingInset: CGFloat {
        guard displayStyle.isChain else { return 0 }
        return 30 + CGFloat(displayStyle.depth) * 18
    }

    private var trailingInset: CGFloat {
        (displayStyle.isChain ? 6 : 8) + reservedTrailingInset
    }

    private func copyToPasteboard(_ text: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(text, forType: .string)
    }

    @ViewBuilder
    private var actionButton: some View {
        if displayStyle.isChain {
            HStack(spacing: 8) {
                if hasDetectedLogPaths {
                    Button {
                        tailAction()
                    } label: {
                        Image(systemName: "doc.text.magnifyingglass")
                            .accessibilityLabel("Tail log")
                    }
                    .buttonStyle(.borderless)
                    .controlSize(.small)
                }

                if !job.state.isTerminal {
                    InlineCancelActionView(
                        isCompact: true,
                        isConfirming: $isConfirmingCancel,
                        isCancelling: $isCancelling,
                        confirmLabel: "Cancel Job",
                        action: cancelAction
                    )
                }

                if showsPrimaryAction {
                    Button {
                        watchAction()
                    } label: {
                        Image(systemName: "plus.circle")
                            .accessibilityLabel("Watch")
                    }
                    .buttonStyle(.borderless)
                    .controlSize(.small)
                }
            }
        } else {
            HStack(spacing: 8) {
                if hasDetectedLogPaths {
                    Button {
                        tailAction()
                    } label: {
                        Image(systemName: "doc.text.magnifyingglass")
                            .accessibilityLabel("Tail log")
                    }
                    .buttonStyle(.borderless)
                    .controlSize(.small)
                }

                if !job.state.isTerminal {
                    InlineCancelActionView(
                        isCompact: false,
                        isConfirming: $isConfirmingCancel,
                        isCancelling: $isCancelling,
                        confirmLabel: "Cancel Job",
                        action: cancelAction
                    )
                }

                if showsPrimaryAction {
                    Button {
                        watchAction()
                    } label: {
                        Label("Watch", systemImage: "plus.circle")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                }
            }
        }
    }

    @ViewBuilder
    private var backgroundShape: some View {
        if displayStyle.isChain {
            Color.clear
        } else {
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        }
    }
}
