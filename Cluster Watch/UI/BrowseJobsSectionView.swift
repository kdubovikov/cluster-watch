import SwiftUI

struct BrowseJobsSectionView: View {
    @Bindable var store: JobStore
    let now: Date

    var body: some View {
        PanelSection(title: "Browse Unwatched Jobs", systemImage: "magnifyingglass") {
            VStack(alignment: .leading, spacing: 8) {
                TextField("Search by job id, job name, or cluster", text: $store.browseSearchText)
                    .textFieldStyle(.roundedBorder)

                Text("Filtered to your configured username on each enabled cluster.")
                    .font(.system(.caption, design: .rounded))
                    .foregroundStyle(.secondary)

                if store.visibleCurrentJobs.isEmpty {
                    Text("No unwatched jobs matched the current filter.")
                        .font(.system(.caption, design: .rounded))
                        .foregroundStyle(.secondary)
                        .padding(.vertical, 6)
                } else {
                    VStack(spacing: 6) {
                        ForEach(store.visibleCurrentJobs.prefix(10)) { job in
                            BrowseJobRowView(
                                job: job,
                                clusterName: store.clusterName(for: job.clusterID),
                                upstreamJobs: store.watchedDependencies(for: job),
                                downstreamJobs: store.watchedDependents(for: job),
                                now: now,
                                watchAction: {
                                    store.watch(job: job)
                                }
                            )
                        }
                    }
                }
            }
        }
    }
}

private struct BrowseJobRowView: View {
    let job: CurrentJob
    let clusterName: String
    let upstreamJobs: [WatchedJob]
    let downstreamJobs: [WatchedJob]
    let now: Date
    let watchAction: () -> Void

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            VStack(alignment: .leading, spacing: 3) {
                HStack(spacing: 5) {
                    Text(job.jobName)
                        .font(.system(size: 13, weight: .semibold, design: .rounded))
                        .lineLimit(1)
                    StateBadgeView(title: job.state.badgeTitle, style: job.state.badgeStyle)
                    if job.dependencyStatus == .waiting {
                        StateBadgeView(title: "Blocked", style: .pending)
                    }
                    if job.dependencyStatus == .neverSatisfied {
                        StateBadgeView(title: "Deps Broken", style: .failed)
                    }
                }

                Text("\(clusterName) • #\(job.jobID) • \(job.owner)")
                    .font(.system(size: 12, weight: .medium, design: .rounded))
                    .foregroundStyle(.secondary)
                Text("\(JobFormatting.startedText(for: job)) • \(JobFormatting.timingSummary(for: job, now: now))")
                    .font(.system(size: 11, weight: .regular, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.9)
                if let dependencySummary = JobFormatting.dependencySummary(
                    state: job.state,
                    dependencyStatus: job.dependencyStatus,
                    dependencyExpression: job.dependencyExpression,
                    dependencyJobIDs: job.dependencyJobIDs,
                    upstreamJobs: upstreamJobs
                ) {
                    Text(dependencySummary)
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(job.dependencyStatus == .neverSatisfied ? .red : .secondary)
                }
                if let pendingReason = JobFormatting.pendingReasonSummary(job.pendingReason, dependencyStatus: job.dependencyStatus) {
                    Text(pendingReason)
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(.secondary)
                }
                if let downstreamSummary = JobFormatting.downstreamSummary(downstreamJobs) {
                    Text(downstreamSummary)
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(.secondary)
                }
            }

            Spacer(minLength: 8)

            Button {
                watchAction()
            } label: {
                Label("Watch", systemImage: "plus.circle")
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
        }
        .padding(8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        )
    }
}
