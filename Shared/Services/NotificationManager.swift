import Foundation
import UserNotifications

@MainActor
public protocol NotificationManaging: AnyObject {
    func requestAuthorizationIfNeeded() async
    func sendTerminalNotification(for job: WatchedJob, clusterName: String) async
}

@MainActor
public final class NotificationManager: NotificationManaging {
    private let center: UNUserNotificationCenter
    private var requestedAuthorization = false

    public init(center: UNUserNotificationCenter = .current()) {
        self.center = center
    }

    public func requestAuthorizationIfNeeded() async {
        guard !requestedAuthorization else { return }
        requestedAuthorization = true
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

        try? await center.add(request)
    }
}
