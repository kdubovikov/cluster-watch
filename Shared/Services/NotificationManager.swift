import Foundation
import AppKit
import UserNotifications

@MainActor
public protocol NotificationManaging: AnyObject {
    func requestAuthorizationIfNeeded() async
    func sendTerminalNotification(for job: WatchedJob, clusterName: String) async
}

@MainActor
public final class NotificationManager: NSObject, NotificationManaging, UNUserNotificationCenterDelegate {
    private var requestedAuthorization = false

    public init(center: UNUserNotificationCenter = .current()) {
        super.init()
        center.delegate = self
    }

    public func requestAuthorizationIfNeeded() async {
        guard !requestedAuthorization else { return }
        requestedAuthorization = true
        let center = UNUserNotificationCenter.current()
        _ = try? await center.requestAuthorization(options: [.alert, .sound, .badge])
    }

    public func sendTerminalNotification(for job: WatchedJob, clusterName: String) async {
        await requestAuthorizationIfNeeded()

        let content = UNMutableNotificationContent()
        content.title = "Slurm job finished"
        content.body = "\(job.jobName) (\(job.jobID)) reached \(job.rawState) on \(clusterName)."
        content.sound = .default

        let request = UNNotificationRequest(
            identifier: "cluster-watch.\(job.clusterID.rawValue).\(job.jobID)",
            content: content,
            trigger: nil
        )

        let center = UNUserNotificationCenter.current()
        try? await center.add(request)
    }

    nonisolated public func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification
    ) async -> UNNotificationPresentationOptions {
        [.banner, .list, .sound]
    }

    nonisolated public func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse
    ) async {
        guard response.actionIdentifier == UNNotificationDefaultActionIdentifier else {
            return
        }

        let text = Self.notificationClipboardText(from: response.notification.request.content)
        guard !text.isEmpty else { return }

        await MainActor.run {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(text, forType: .string)
        }
    }

    nonisolated private static func notificationClipboardText(from content: UNNotificationContent) -> String {
        let title = content.title.trimmingCharacters(in: .whitespacesAndNewlines)
        let body = content.body.trimmingCharacters(in: .whitespacesAndNewlines)

        switch (title.isEmpty, body.isEmpty) {
        case (false, false):
            return "\(title)\n\(body)"
        case (false, true):
            return title
        case (true, false):
            return body
        case (true, true):
            return ""
        }
    }
}
