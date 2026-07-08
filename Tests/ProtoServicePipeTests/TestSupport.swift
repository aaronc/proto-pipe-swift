import Connect
import Foundation
import SwiftProtobuf
import Testing

@testable import ProtoServicePipe

/// Decodes what the client sends and lets tests script server responses back. All
/// interaction is synchronous, so tests assert on exact frame sequences.
final class ScriptedServer: @unchecked Sendable {
    private(set) var client: PipeClient!
    private let recorded = Locked([ProtoPipe_V1_RequestPacket]())

    init() {
        self.client = PipeClient { [recorded] data in
            let packet = try! ProtoPipe_V1_RequestPacket(serializedBytes: data)
            recorded.withLock { $0.append(packet) }
        }
    }

    var packets: [ProtoPipe_V1_RequestPacket] { self.recorded.withLock { $0 } }

    func respond(_ reqID: UInt64, _ frame: ProtoPipe_V1_ResponsePacket.OneOf_Frame) {
        var packet = ProtoPipe_V1_ResponsePacket()
        packet.reqID = reqID
        packet.frame = frame
        self.client.receive(try! packet.serializedData())
    }

    func respondHeaders(_ reqID: UInt64, _ headers: Headers) {
        var frame = ProtoPipe_V1_ResponsePacket.Headers()
        frame.metadata = packetMetadata(from: headers)
        self.respond(reqID, .headers(frame))
    }

    func respondMessage(_ reqID: UInt64, _ message: any SwiftProtobuf.Message) {
        self.respondMessage(reqID, body: try! message.serializedData())
    }

    func respondMessage(_ reqID: UInt64, body: Data) {
        var frame = ProtoPipe_V1_ResponsePacket.Message()
        frame.body = body
        self.respond(reqID, .message(frame))
    }

    func respondClose(
        _ reqID: UInt64,
        code: Int32 = 0,
        message: String = "",
        singleResponse: (any SwiftProtobuf.Message)? = nil,
        headers: Headers = [:],
        trailers: Headers = [:]
    ) {
        var close = ProtoPipe_V1_ResponsePacket.Close()
        close.status.code = code
        close.status.message = message
        if let singleResponse {
            close.singleResponse = try! singleResponse.serializedData()
        }
        if !headers.isEmpty {
            close.headers = packetMetadata(from: headers)
        }
        close.trailers = packetMetadata(from: trailers)
        self.respond(reqID, .close(close))
    }
}

func eventually(
    _ condition: @Sendable () -> Bool,
    sourceLocation: SourceLocation = #_sourceLocation
) async {
    for _ in 0..<10_000 {
        if condition() { return }
        await Task.yield()
    }
    Issue.record("condition never became true", sourceLocation: sourceLocation)
}
