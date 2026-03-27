// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "ClusterWatchCore",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(
            name: "ClusterWatchCore",
            targets: ["ClusterWatchCore"]
        )
    ],
    targets: [
        .target(
            name: "ClusterWatchCore",
            path: "Shared"
        ),
        .testTarget(
            name: "ClusterWatchCoreTests",
            dependencies: ["ClusterWatchCore"],
            path: "Tests/ClusterWatchCoreTests"
        )
    ]
)
