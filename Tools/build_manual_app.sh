#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="$ROOT_DIR/build-manual/Cluster Watch.app"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
FRAMEWORKS_DIR="$CONTENTS_DIR/Frameworks"
EXECUTABLE_PATH="$MACOS_DIR/Cluster Watch"
PLIST_PATH="$CONTENTS_DIR/Info.plist"
SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"
SWIFT_STDLIB_TOOL="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/swift-stdlib-tool"

pkill -f "$EXECUTABLE_PATH" || true
rm -rf "$ROOT_DIR/build-manual"

mkdir -p "$MACOS_DIR" "$FRAMEWORKS_DIR"
cp "$ROOT_DIR/Cluster Watch/Info.plist" "$PLIST_PATH"

set_plist_string() {
  local key="$1"
  local value="$2"
  /usr/libexec/PlistBuddy -c "Set :$key $value" "$PLIST_PATH" 2>/dev/null \
    || /usr/libexec/PlistBuddy -c "Add :$key string $value" "$PLIST_PATH"
}

set_plist_string "CFBundleExecutable" "Cluster Watch"
set_plist_string "CFBundleIdentifier" "com.kirilldubovikov.ClusterWatch.manual"
set_plist_string "CFBundleName" "Cluster Watch"
set_plist_string "CFBundlePackageType" "APPL"
set_plist_string "CFBundleShortVersionString" "0.5.0"
set_plist_string "CFBundleVersion" "5"

swiftc \
  -sdk "$SDKROOT" \
  -target arm64-apple-macosx14.0 \
  -module-name ClusterWatch \
  -D DEBUG \
  -emit-executable \
  -o "$EXECUTABLE_PATH" \
  "$ROOT_DIR/Cluster Watch/ClusterWatchApp.swift" \
  "$ROOT_DIR/Cluster Watch/UI/MenuBarRootView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/StateBadgeView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/WatchedJobsSectionView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/WatchedJobRowView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/ClusterStatusSectionView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/BrowseJobsSectionView.swift" \
  "$ROOT_DIR/Cluster Watch/UI/SettingsView.swift" \
  "$ROOT_DIR/Shared/Models/ClusterConfig.swift" \
  "$ROOT_DIR/Shared/Models/JobModels.swift" \
  "$ROOT_DIR/Shared/Models/Reachability.swift" \
  "$ROOT_DIR/Shared/Services/SlurmParsing.swift" \
  "$ROOT_DIR/Shared/Services/SlurmClient.swift" \
  "$ROOT_DIR/Shared/Services/NotificationManager.swift" \
  "$ROOT_DIR/Shared/Services/PersistenceStore.swift" \
  "$ROOT_DIR/Shared/Services/JobFormatting.swift" \
  "$ROOT_DIR/Shared/Services/PollingCoordinator.swift" \
  "$ROOT_DIR/Shared/Services/JobStore.swift" \
  "$ROOT_DIR/Shared/ViewModels/GroupedJobsViewModel.swift"

"$SWIFT_STDLIB_TOOL" \
  --copy \
  --platform macosx \
  --destination "$FRAMEWORKS_DIR" \
  --scan-executable "$EXECUTABLE_PATH" \
  --scan-folder "$FRAMEWORKS_DIR"

codesign --force --deep --sign - "$APP_DIR"
open -n "$APP_DIR"

sleep 2
pgrep -fl "$EXECUTABLE_PATH"
