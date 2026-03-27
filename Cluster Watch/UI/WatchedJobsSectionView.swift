import SwiftUI

struct WatchedJobsSectionView: View {
    let store: JobStore
    let now: Date

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
                                            now: now
                                        )
                                    } else if let job = group.jobs.first {
                                        WatchedJobRowView(
                                            job: job,
                                            clusterName: store.clusterName(for: job.clusterID),
                                            upstreamJobs: store.watchedDependencies(for: job),
                                            downstreamJobs: store.watchedDependents(for: job),
                                            now: now,
                                            unwatchAction: {
                                                store.unwatch(job: job)
                                            }
                                        )
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
    let group: GroupedJobsViewModel.Group
    let store: JobStore
    let now: Date

    var body: some View {
        let coordinateSpaceName = "dependency-group-\(group.id)"

        VStack(alignment: .leading, spacing: 0) {
            ForEach(group.rows) { row in
                WatchedJobRowView(
                    job: row.job,
                    clusterName: store.clusterName(for: row.job.clusterID),
                    upstreamJobs: store.watchedDependencies(for: row.job),
                    downstreamJobs: store.watchedDependents(for: row.job),
                    now: now,
                    displayStyle: .chain(depth: row.depth),
                    unwatchAction: {
                        store.unwatch(job: row.job)
                    }
                )
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
        .overlayPreferenceValue(DependencyRowFramePreferenceKey.self) { rows in
            DependencyChainOverlay(rows: rows)
        }
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .stroke(Color.cyan.opacity(0.16), lineWidth: 1)
        )
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
