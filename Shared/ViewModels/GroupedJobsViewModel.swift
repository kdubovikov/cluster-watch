import Foundation

public struct GroupedJobsViewModel {
    public struct GroupRow: Identifiable, Sendable {
        public let job: WatchedJob
        public let depth: Int
        public let parentJobID: String?

        public var id: String { job.id }
    }

    public struct Group: Identifiable, Sendable {
        public let rows: [GroupRow]
        public let isDependencyLinked: Bool

        public var id: String { rows.map(\.job.id).joined(separator: "|") }
        public var jobs: [WatchedJob] { rows.map(\.job) }
    }

    public enum Bucket: String, CaseIterable, Identifiable, Sendable {
        case today
        case yesterday
        case earlierThisWeek
        case lastWeek
        case older

        public var id: String { rawValue }

        public var title: String {
            switch self {
            case .today:
                return "Today"
            case .yesterday:
                return "Yesterday"
            case .earlierThisWeek:
                return "Earlier This Week"
            case .lastWeek:
                return "Last Week"
            case .older:
                return "Older"
            }
        }
    }

    public struct Section: Identifiable, Sendable {
        public let bucket: Bucket
        public let jobs: [WatchedJob]
        public let groups: [Group]

        public var id: String { bucket.id }
    }

    public static func sections(
        for jobs: [WatchedJob],
        referenceDate: Date = .now,
        calendar: Calendar = .current
    ) -> [Section] {
        let grouped = Dictionary(grouping: jobs) { bucket(for: $0, referenceDate: referenceDate, calendar: calendar) }

        return Bucket.allCases.compactMap { bucket in
            let bucketJobs = (grouped[bucket] ?? []).sorted(by: compare)
            guard !bucketJobs.isEmpty else { return nil }
            return Section(
                bucket: bucket,
                jobs: bucketJobs,
                groups: dependencyGroups(for: bucketJobs)
            )
        }
    }

    public static func bucket(
        for job: WatchedJob,
        referenceDate: Date = .now,
        calendar: Calendar = .current
    ) -> Bucket {
        let anchorDate = anchorDate(for: job)

        if calendar.isDateInToday(anchorDate) {
            return .today
        }
        if calendar.isDateInYesterday(anchorDate) {
            return .yesterday
        }

        let startOfToday = calendar.startOfDay(for: referenceDate)
        guard let startOfCurrentWeek = calendar.dateInterval(of: .weekOfYear, for: startOfToday)?.start else {
            return .older
        }

        if anchorDate >= startOfCurrentWeek {
            return .earlierThisWeek
        }

        if let startOfLastWeek = calendar.date(byAdding: .day, value: -7, to: startOfCurrentWeek),
           anchorDate >= startOfLastWeek {
            return .lastWeek
        }

        return .older
    }

    public static func anchorDate(for job: WatchedJob) -> Date {
        if job.isTerminal {
            return job.endTime ?? job.lastUpdatedAt
        }
        return job.startTime ?? job.firstSeenAt
    }

    private static func compare(_ lhs: WatchedJob, _ rhs: WatchedJob) -> Bool {
        if lhs.state.sortPriority != rhs.state.sortPriority {
            return lhs.state.sortPriority < rhs.state.sortPriority
        }

        if lhs.lastUpdatedAt != rhs.lastUpdatedAt {
            return lhs.lastUpdatedAt > rhs.lastUpdatedAt
        }

        return lhs.jobID > rhs.jobID
    }

    private static func dependencyGroups(for jobs: [WatchedJob]) -> [Group] {
        let components = connectedComponents(in: jobs)

        return components
            .map(makeGroup(from:))
            .sorted { lhs, rhs in
                guard let lhsAnchor = lhs.jobs.first, let rhsAnchor = rhs.jobs.first else {
                    return lhs.id < rhs.id
                }
                return compare(lhsAnchor, rhsAnchor)
            }
    }

    private static func connectedComponents(in jobs: [WatchedJob]) -> [[WatchedJob]] {
        let jobsByID = Dictionary(uniqueKeysWithValues: jobs.map { ($0.id, $0) })
        var adjacency: [String: Set<String>] = Dictionary(uniqueKeysWithValues: jobs.map { ($0.id, []) })

        for lhs in jobs {
            for rhs in jobs where lhs.id != rhs.id && lhs.clusterID == rhs.clusterID {
                if lhs.depends(on: rhs.jobID) || rhs.depends(on: lhs.jobID) {
                    adjacency[lhs.id, default: []].insert(rhs.id)
                    adjacency[rhs.id, default: []].insert(lhs.id)
                }
            }
        }

        var visited = Set<String>()
        var components: [[WatchedJob]] = []

        for job in jobs.sorted(by: compare) {
            guard !visited.contains(job.id) else { continue }

            var stack = [job.id]
            var component: [WatchedJob] = []

            while let currentID = stack.popLast() {
                guard visited.insert(currentID).inserted else { continue }
                guard let currentJob = jobsByID[currentID] else { continue }

                component.append(currentJob)

                for neighbor in adjacency[currentID, default: []] where !visited.contains(neighbor) {
                    stack.append(neighbor)
                }
            }

            components.append(component.sorted(by: compare))
        }

        return components
    }

    private static func makeGroup(from jobs: [WatchedJob]) -> Group {
        guard jobs.count > 1 else {
            return Group(rows: jobs.map { GroupRow(job: $0, depth: 0, parentJobID: nil) }, isDependencyLinked: false)
        }

        let componentJobIDs = Set(jobs.map(\.id))
        let sortedRoots = jobs
            .filter { job in
                !jobs.contains { other in
                    other.id != job.id && componentJobIDs.contains(other.id) && job.depends(on: other.jobID)
                }
            }
            .sorted(by: compare)

        let roots = sortedRoots.isEmpty ? jobs.sorted(by: compare) : sortedRoots
        let childrenByParent = Dictionary(grouping: jobs) { child in
            jobs
                .filter { parent in
                    parent.id != child.id && child.depends(on: parent.jobID)
                }
                .sorted(by: compare)
                .first?.id
        }

        var rows: [GroupRow] = []
        var visited = Set<String>()

        func append(job: WatchedJob, depth: Int, parentJobID: String?) {
            guard visited.insert(job.id).inserted else { return }
            rows.append(GroupRow(job: job, depth: min(depth, 3), parentJobID: parentJobID))

            let children = (childrenByParent[job.id] ?? []).sorted(by: compare)
            for child in children {
                append(job: child, depth: depth + 1, parentJobID: job.id)
            }
        }

        for root in roots {
            append(job: root, depth: 0, parentJobID: nil)
        }

        for remaining in jobs.sorted(by: compare) where !visited.contains(remaining.id) {
            append(job: remaining, depth: 0, parentJobID: nil)
        }

        return Group(rows: rows, isDependencyLinked: true)
    }
}
