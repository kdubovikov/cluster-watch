// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "ClusterWatchCore",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "cluster-watch", targets: ["ClusterWatchCLI"]),
        .library(
            name: "ClusterWatchCore",
            targets: ["ClusterWatchCore"]
        )
    ],
    targets: [
        .executableTarget(name: "ClusterWatchCLI", dependencies: ["ClusterWatchCore"], path: "CLI"),
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
