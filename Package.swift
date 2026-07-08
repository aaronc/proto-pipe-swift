// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "proto-pipe-swift",
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(name: "ProtoServicePipe", targets: ["ProtoServicePipe"])
    ],
    dependencies: [
        .package(url: "https://github.com/connectrpc/connect-swift.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.31.0"),
    ],
    targets: [
        .target(
            name: "ProtoServicePipe",
            dependencies: [
                .product(name: "Connect", package: "connect-swift"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
            ]
        ),
        .testTarget(
            name: "ProtoServicePipeTests",
            dependencies: ["ProtoServicePipe"]
        ),
    ]
)
