import SwiftUI

struct WatchedJobRowView: View {
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

    let job: WatchedJob
    let clusterName: String
    let upstreamJobs: [WatchedJob]
    let downstreamJobs: [WatchedJob]
    let hasDetectedLogPaths: Bool
    let now: Date
    var displayStyle: DisplayStyle = .standalone
    var showsPrimaryAction: Bool = true
    var reservedTrailingInset: CGFloat = 0
    let commandAction: () -> Void
    let tailAction: () -> Void
    let unwatchAction: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(job.jobName)
                    .font(.system(.subheadline, design: .rounded, weight: .semibold))
                    .lineLimit(1)

                Spacer(minLength: 8)

                HStack(spacing: 4) {
                    StateBadgeView(title: job.state.badgeTitle, style: job.state.badgeStyle)
                    if job.dependencyStatus == .waiting {
                        StateBadgeView(title: "Blocked", style: .pending)
                    }
                    if job.dependencyStatus == .neverSatisfied {
                        StateBadgeView(title: "Deps Broken", style: .failed)
                    }
                    if job.isStale {
                        StateBadgeView(title: "Stale", style: .stale)
                    }
                }
            }

            Text("\(clusterName) • #\(job.jobID)")
                .font(.system(size: 12, weight: .medium, design: .rounded))
                .foregroundStyle(.secondary)

            VStack(alignment: .leading, spacing: 3) {
                Text(compactTimingLine)
                    .font(.system(size: 12, weight: .regular, design: .rounded))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.9)
                if let detailLine = primaryDetailLine {
                    Text(detailLine)
                        .font(.system(size: 12, weight: .regular, design: .rounded))
                        .foregroundStyle(detailLineColor)
                        .lineLimit(1)
                        .truncationMode(.tail)
                }
                if let secondaryDetailLine {
                    Text(secondaryDetailLine)
                        .font(.system(size: 12, weight: .regular, design: .rounded))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.tail)
                }
            }

            HStack {
                if let lastRefresh = job.lastSuccessfulRefreshAt {
                    Text("Updated \(JobFormatting.absoluteDateText(lastRefresh))")
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(.tertiary)
                        .lineLimit(1)
                } else {
                    Text("No successful refresh yet")
                        .font(.system(size: 11, weight: .regular, design: .rounded))
                        .foregroundStyle(.tertiary)
                        .lineLimit(1)
                }

                Spacer()

                Button {
                    commandAction()
                } label: {
                    Image(systemName: "chevron.left.forwardslash.chevron.right")
                        .accessibilityLabel("View launch command")
                }
                .buttonStyle(.borderless)
                .controlSize(.small)

                if hasDetectedLogPaths {
                    Button {
                        tailAction()
                    } label: {
                        if displayStyle.isChain {
                            Image(systemName: "doc.text.magnifyingglass")
                                .accessibilityLabel("Tail log")
                        } else {
                            Label("Tail", systemImage: "doc.text.magnifyingglass")
                        }
                    }
                    .buttonStyle(.borderless)
                    .controlSize(.small)
                }

                if showsPrimaryAction {
                    Button {
                        unwatchAction()
                    } label: {
                        if displayStyle.isChain {
                            Image(systemName: "minus.circle")
                                .accessibilityLabel("Unwatch")
                        } else {
                            Label("Unwatch", systemImage: "minus.circle")
                        }
                    }
                    .buttonStyle(.borderless)
                    .controlSize(.small)
                }
            }
        }
        .padding(.leading, leadingInset)
        .padding(.vertical, displayStyle.isChain ? 9 : 10)
        .padding(.leading, displayStyle.isChain ? 6 : 10)
        .padding(.trailing, trailingInset)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(backgroundShape)
    }

    private var compactTimingLine: String {
        "\(JobFormatting.startedText(for: job)) • \(JobFormatting.timingSummary(for: job, now: now))"
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

    private var detailLineColor: Color {
        job.dependencyStatus == .neverSatisfied ? .red : .secondary
    }

    private var leadingInset: CGFloat {
        guard displayStyle.isChain else { return 0 }
        return 30 + CGFloat(displayStyle.depth) * 18
    }

    private var trailingInset: CGFloat {
        (displayStyle.isChain ? 6 : 10) + reservedTrailingInset
    }

    @ViewBuilder
    private var backgroundShape: some View {
        if displayStyle.isChain {
            Color.clear
        } else {
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .fill(Color.primary.opacity(0.04))
        }
    }
}
