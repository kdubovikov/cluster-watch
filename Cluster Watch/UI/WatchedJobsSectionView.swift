import SwiftUI

struct WatchedJobsSectionView: View {
    let store: JobStore
    let now: Date
    let openLogTailWindow: () -> Void
    let openLaunchCommandWindow: () -> Void

    var body: some View {
        PanelSection(title: "Watched Jobs", systemImage: "eye") {
            if store.groupedWatchedJobs.isEmpty {
                VStack(alignment: .leading, spacing: 6) {
                    Text("No watched jobs yet")
                        .font(.system(.headline, design: .rounded))
                    Text("Use the browser below to pin a job. Watched jobs stay visible after completion until you unwatch them.")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                }
            } else {
                VStack(alignment: .leading, spacing: 10) {
                    ForEach(store.groupedWatchedJobs) { section in
                        VStack(alignment: .leading, spacing: 6) {
                            Text(section.bucket.title)
                                .font(.system(.subheadline, design: .rounded, weight: .semibold))
                                .foregroundStyle(.secondary)

                            VStack(spacing: 6) {
                                ForEach(section.groups) { group in
                                    if group.isDependencyLinked {
                                        DependencyLinkedJobGroupView(
                                            group: group,
                                            store: store,
                                            now: now,
                                            openLogTailWindow: openLogTailWindow,
                                            openLaunchCommandWindow: openLaunchCommandWindow
                                        )
                                    } else if let job = group.jobs.first {
                                        WatchedJobRowView(
                                            job: job,
                                            clusterName: store.clusterName(for: job.clusterID),
                                            upstreamJobs: store.watchedDependencies(for: job),
                                            downstreamJobs: store.watchedDependents(for: job),
                                            hasDetectedLogPaths: job.state != .pending && store.logPaths(for: job)?.hasAnyPath == true,
                                            now: now,
                                            commandAction: {
                                                Task {
                                                    if await store.prepareLaunchCommand(for: job) {
                                                        openLaunchCommandWindow()
                                                    }
                                                }
                                            },
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
                                            unwatchAction: {
                                                store.unwatch(job: job)
                                            }
                                        )
                                        .task(id: "\(job.id):\(job.state.rawValue)") {
                                            await store.prefetchLogPaths(for: job)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        .transaction { transaction in
            transaction.animation = nil
        }
    }
}

private struct DependencyLinkedJobGroupView: View {
    @State private var isConfirmingCancel = false
    @State private var isCancelling = false

    let group: GroupedJobsViewModel.Group
    let store: JobStore
    let now: Date
    let openLogTailWindow: () -> Void
    let openLaunchCommandWindow: () -> Void

    var body: some View {
        let coordinateSpaceName = "dependency-group-\(group.id)"

        VStack(alignment: .leading, spacing: 0) {
            ForEach(group.rows) { row in
                WatchedJobRowView(
                    job: row.job,
                    clusterName: store.clusterName(for: row.job.clusterID),
                    upstreamJobs: store.watchedDependencies(for: row.job),
                    downstreamJobs: store.watchedDependents(for: row.job),
                    hasDetectedLogPaths: row.job.state != .pending && store.logPaths(for: row.job)?.hasAnyPath == true,
                    now: now,
                    displayStyle: .chain(depth: row.depth),
                    showsPrimaryAction: false,
                    reservedTrailingInset: groupHasCancellableJobs ? 48 : 22,
                    commandAction: {
                        Task {
                            if await store.prepareLaunchCommand(for: row.job) {
                                openLaunchCommandWindow()
                            }
                        }
                    },
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
                    unwatchAction: {
                        store.unwatch(job: row.job)
                    }
                )
                .task(id: "\(row.id):\(row.job.state.rawValue)") {
                    await store.prefetchLogPaths(for: row.job)
                }
                .background {
                    GeometryReader { proxy in
                        Color.clear.preference(
                            key: DependencyRowFramePreferenceKey.self,
                            value: [
                                DependencyRowFrame(
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
                    store.unwatch(jobs: group.jobs)
                } label: {
                    Image(systemName: "minus.circle")
                        .accessibilityLabel("Unwatch Group")
                }
                .buttonStyle(.borderless)
                .controlSize(.small)
                .help("Unwatch whole group")
            }
            .padding(.top, 10)
            .padding(.trailing, 14)
        }
        .overlayPreferenceValue(DependencyRowFramePreferenceKey.self) { rows in
            DependencyChainOverlay(rows: rows)
        }
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .stroke(Color.cyan.opacity(0.16), lineWidth: 1)
        )
    }

    private var cancellableJobs: [WatchedJob] {
        group.jobs.filter { !$0.isTerminal && !$0.isStale }
    }

    private var groupHasCancellableJobs: Bool {
        !cancellableJobs.isEmpty
    }
}

private struct DependencyRowFrame: Equatable {
    let id: String
    let depth: Int
    let parentJobID: String?
    let frame: CGRect
}

private struct DependencyRowFramePreferenceKey: PreferenceKey {
    static var defaultValue: [DependencyRowFrame] = []

    static func reduce(value: inout [DependencyRowFrame], nextValue: () -> [DependencyRowFrame]) {
        value.append(contentsOf: nextValue())
    }
}

private struct DependencyChainOverlay: View {
    let rows: [DependencyRowFrame]

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

                        guard let firstChild = childPoints.first, let lastChild = childPoints.last else {
                            continue
                        }

                        path.move(to: parentPoint)
                        path.addLine(to: CGPoint(x: parentPoint.x, y: lastChild.y))

                        for childPoint in childPoints {
                            path.move(to: CGPoint(x: parentPoint.x, y: childPoint.y))
                            path.addLine(to: childPoint)
                        }

                        if childPoints.count == 1, firstChild.y < parentPoint.y {
                            path.move(to: firstChild)
                            path.addLine(to: parentPoint)
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

    private func nodePoint(for row: DependencyRowFrame) -> CGPoint {
        CGPoint(x: 16 + CGFloat(row.depth) * 18, y: row.frame.minY + 18)
    }
}
