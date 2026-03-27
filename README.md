# Cluster Watch

Cluster Watch is a native macOS menu bar app for monitoring Slurm jobs across any number of configured clusters. It keeps watched jobs pinned until you explicitly unwatch them, preserves last-known job state during outages, and sends one local notification when a watched job reaches a terminal state.

## Features

- Native SwiftUI menu bar app built with `MenuBarExtra`
- Any number of user-defined clusters, each with:
  - display name
  - SSH alias
  - optional SSH username override
  - optional Slurm owner override
  - enabled/disabled state
- Global username filter seeded from your macOS username, plus optional per-cluster overrides
- Watched jobs grouped into history-style buckets:
  - Today
  - Yesterday
  - Earlier This Week
  - Last Week
  - Older
- Split job timing into:
  - waiting time from submit to start
  - running time from start to end or now
- Dependency-aware rendering for:
  - jobs waiting on other jobs
  - watched jobs that unblock downstream jobs
  - unsatisfied dependency failures such as `DependencyNeverSatisfied`
- One-shot local notifications for terminal transitions
- Persistent local state for watched jobs, cluster settings, and reachability snapshots

## Project Layout

- `Cluster Watch.xcodeproj`
- `Cluster Watch`
  - SwiftUI app entry point, menu bar UI, settings window, `Info.plist`
- `Shared`
  - shared models, parsing, SSH client, persistence, polling, formatting, and store logic
- `Tests/ClusterWatchCoreTests`
  - unit tests intended for the Xcode test target
- `Tools/generate_xcodeproj.rb`
  - reproducible generator for the checked-in Xcode project

## Requirements

- macOS 14 or newer
- Full Xcode for building and running the app target
- Working non-interactive SSH aliases in `~/.ssh/config`
- `squeue` and `sacct` available on the remote cluster login nodes

## Setup

1. Verify the SSH aliases you plan to use work in Terminal:
   - `ssh mycluster`
   - `ssh othercluster`
2. Open `Cluster Watch.xcodeproj` in Xcode.
3. Select the `Cluster Watch` scheme.
4. Build and run the app.
5. Open Settings from the menu bar popup and add one or more clusters.
6. Configure:
   - display names
   - SSH aliases
   - SSH username overrides
   - Slurm owner filters
   - polling interval

## Usage

1. Open the menu bar app.
2. Review watched jobs at the top of the window.
3. Browse current jobs in the lower search area.
4. Click `Watch` next to any visible job.
5. Leave watched jobs pinned after completion, or remove them manually with:
   - `Unwatch` on a single row
   - `Clear Completed` for all watched jobs in terminal states

## Slurm Command Assumptions

The app shells out through `/usr/bin/ssh` and expects key or agent based access only. Password prompts are not supported.

Current jobs are queried with `squeue`, using a machine-readable format similar to:

```sh
ssh mycluster "squeue -h -u <username> -o '%i|%u|%T|%j|%V|%S|%M|%E|%r'"
```

Watched jobs that disappear from `squeue` are checked with `sacct`:

```sh
ssh mycluster "sacct -n -P -j <jobid> --format=JobIDRaw,User,State,JobName,Submit,Start,End,Elapsed,Reason"
```

The parser prefers the primary row matching the raw/base job ID and ignores step rows such as `.batch` and `.extern`. Dependency data comes from:

- `%E` in `squeue` for remaining dependency expressions
- `%r` in `squeue` for the pending reason
- `Reason` in `sacct` for historical dependency-related failures such as `DependencyNeverSatisfied`

## Persistence And Outages

- State is saved to:
  - `~/Library/Application Support/ClusterWatch/state.json`
- If a cluster becomes unreachable:
  - watched jobs for that cluster remain visible
  - their last-known state is preserved
  - they are marked stale
- When the cluster becomes reachable again, the app refreshes those jobs in place

## Verification Notes

- The shared core compiles locally with:

```sh
swift build
```

- Full app builds and Xcode test execution were not run in this environment because the active developer directory is Command Line Tools, not a full Xcode install.
