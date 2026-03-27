import SwiftUI

struct StateBadgeView: View {
    enum Style {
        case running
        case pending
        case completed
        case failed
        case stale
        case neutral

        var foregroundColor: Color {
            switch self {
            case .running:
                return .green.opacity(0.95)
            case .pending:
                return .orange.opacity(0.95)
            case .completed:
                return .blue.opacity(0.95)
            case .failed:
                return .red.opacity(0.95)
            case .stale:
                return .purple.opacity(0.95)
            case .neutral:
                return .secondary
            }
        }

        var backgroundColor: Color {
            foregroundColor.opacity(0.12)
        }
    }

    let title: String
    let style: Style

    var body: some View {
        Text(title)
            .font(.system(size: 11, weight: .semibold, design: .rounded))
            .foregroundStyle(style.foregroundColor)
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(
                Capsule()
                    .fill(style.backgroundColor)
            )
            .overlay(
                Capsule()
                    .stroke(style.foregroundColor.opacity(0.18), lineWidth: 1)
            )
    }
}

extension NormalizedJobState {
    var badgeStyle: StateBadgeView.Style {
        switch self {
        case .running:
            return .running
        case .pending:
            return .pending
        case .completed:
            return .completed
        case .failed, .cancelled, .timeout, .outOfMemory, .nodeFail, .preempted:
            return .failed
        case .unknown:
            return .neutral
        }
    }
}
