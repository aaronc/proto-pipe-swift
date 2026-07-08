import Connect
import Foundation
import SwiftProtobuf
import Testing

@testable import ProtoServicePipe

/// packet.proto's own Status doubles as the test request/response message type.
private typealias Msg = ProtoPipe_V1_Status

private func msg(_ text: String) -> Msg {
    var message = Msg()
    message.message = text
    return message
}

private final class Box<T>: @unchecked Sendable {
    let value = Locked([T]())
    func append(_ element: T) { self.value.withLock { $0.append(element) } }
    var all: [T] { self.value.withLock { $0 } }
}

// MARK: - Unary

@Test private func unarySendsBeginWithSingleRequest() {
    let server = ScriptedServer()
    server.client.unary(
        path: "pkg.Svc/Echo",
        idempotencyLevel: .unknown,
        request: msg("hi"),
        headers: ["User-Key": ["v1", "v2"]]
    ) { (_: ResponseMessage<Msg>) in }

    #expect(server.packets.count == 1)
    let packet = server.packets[0]
    #expect(packet.reqID == 1)
    guard case .begin(let begin) = packet.frame else {
        Issue.record("expected Begin, got \(String(describing: packet.frame))")
        return
    }
    #expect(begin.method == "/pkg.Svc/Echo")
    #expect(begin.hasSingleRequest)
    #expect(try! Msg(serializedBytes: begin.singleRequest) == msg("hi"))
    // Keys are lowercased on the wire.
    #expect(connectHeaders(from: begin.headers) == ["user-key": ["v1", "v2"]])
}

@Test private func unarySuccess() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    server.respondClose(
        1,
        singleResponse: msg("out"),
        headers: ["h": ["1"]],
        trailers: ["t": ["2"]]
    )

    #expect(responses.all.count == 1)
    let response = responses.all[0]
    #expect(response.code == .ok)
    #expect(response.message == msg("out"))
    #expect(response.headers == ["h": ["1"]])
    #expect(response.trailers == ["t": ["2"]])
}

@Test private func unaryErrorClose() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    var close = ProtoPipe_V1_ResponsePacket.Close()
    close.status.code = 5 // not_found
    close.status.message = "nope"
    var detail = SwiftProtobuf.Google_Protobuf_Any()
    detail.typeURL = "type.googleapis.com/pkg.MyDetail"
    detail.value = Data([1, 2, 3])
    close.status.details = [detail]
    close.headers = packetMetadata(from: ["h": ["1"]])
    close.trailers = packetMetadata(from: ["t": ["2"]])
    server.respond(1, .close(close))

    let response = responses.all[0]
    #expect(response.code == .notFound)
    let error = response.error
    #expect(error?.code == .notFound)
    #expect(error?.message == "nope")
    #expect(error?.details.map(\.type) == ["pkg.MyDetail"])
    #expect(error?.details.map(\.payload) == [Data([1, 2, 3])])
    // Error metadata merges leading and trailing metadata, as connect does.
    #expect(error?.metadata == ["h": ["1"], "t": ["2"]])
}

@Test private func unaryCancelSendsCancelAndCompletesOnce() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    let cancelable = server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    cancelable.cancel()
    #expect(responses.all.map(\.code) == [.canceled])
    guard case .cancel = server.packets[1].frame else {
        Issue.record("expected Cancel frame")
        return
    }

    // A Close racing the Cancel is dropped: no second completion.
    server.respondClose(1, singleResponse: msg("late"))
    #expect(responses.all.count == 1)
}

@Test private func unaryRejectsStreamedMessageFrame() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    server.respondMessage(1, msg("bogus"))
    #expect(responses.all.map(\.code) == [.internalError])
    guard case .cancel = server.packets[1].frame else {
        Issue.record("expected Cancel frame")
        return
    }
}

@Test private func unaryDropsUnexpectedHeadersFrame() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    // Single-response calls carry headers in Close; a stray Headers frame is ignored
    // and the call still completes normally.
    server.respondHeaders(1, ["h": ["1"]])
    #expect(responses.all.isEmpty)
    server.respondClose(1, singleResponse: msg("out"))
    #expect(responses.all.map(\.code) == [.ok])
}

@Test private func asyncUnaryDeliversResponse() async {
    let server = ScriptedServer()
    let task = Task { () -> ResponseMessage<Msg> in
        await server.client.unary(
            path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
        )
    }
    await eventually { server.packets.count == 1 }
    server.respondClose(1, singleResponse: msg("out"))
    let response = await task.value
    #expect(response.message == msg("out"))
}

@Test private func asyncUnaryTaskCancellationCancelsCall() async {
    let server = ScriptedServer()
    let task = Task { () -> ResponseMessage<Msg> in
        await server.client.unary(
            path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
        )
    }
    await eventually { server.packets.count == 1 }
    task.cancel()
    let response = await task.value
    #expect(response.code == .canceled)
    await eventually { server.packets.count == 2 }
    guard case .cancel = server.packets[1].frame else {
        Issue.record("expected Cancel frame")
        return
    }
}

// MARK: - Bidirectional streams

@Test private func bidiFullLifecycle() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    let stream: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/Chat", headers: ["k": ["v"]]
    ) { results.append($0) }

    // Begin goes out at stream creation, with no payload, so subscribe-only
    // (unused up-channel) views start the server call immediately.
    #expect(server.packets.count == 1)
    guard case .begin(let begin) = server.packets[0].frame else {
        Issue.record("expected Begin")
        return
    }
    #expect(begin.method == "/pkg.Svc/Chat")
    #expect(!begin.hasSingleRequest)

    stream.send(msg("a")).send(msg("b"))
    #expect(server.packets.count == 3)
    guard case .message(let first) = server.packets[1].frame,
        case .message(let second) = server.packets[2].frame
    else {
        Issue.record("expected two Message frames")
        return
    }
    #expect(try! Msg(serializedBytes: first.body) == msg("a"))
    #expect(try! Msg(serializedBytes: second.body) == msg("b"))

    server.respondHeaders(1, ["h": ["1"]])
    server.respondMessage(1, msg("r1"))
    stream.close()
    guard case .doneSending = server.packets[3].frame else {
        Issue.record("expected DoneSending")
        return
    }
    server.respondClose(1, trailers: ["t": ["2"]])

    #expect(results.all == [
        .headers(["h": ["1"]]),
        .message(msg("r1")),
        .complete(code: .ok, error: nil, trailers: ["t": ["2"]]),
    ])
}

@Test private func bidiCancelAbortsAndDropsLateFrames() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    let stream: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/Chat", headers: [:]
    ) { results.append($0) }

    stream.cancel()
    guard case .cancel = server.packets[1].frame else {
        Issue.record("expected Cancel frame")
        return
    }
    #expect(results.all == [.complete(code: .canceled, error: nil, trailers: nil)])

    // In-flight frames arriving after Cancel are dropped, and sends are inert.
    server.respondMessage(1, msg("late"))
    stream.send(msg("после"))
    #expect(results.all.count == 1)
    #expect(server.packets.count == 2)
}

@Test private func bidiCloseAfterCancelSendsNoDoneSending() {
    let server = ScriptedServer()
    let stream: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/Chat", headers: [:]
    ) { (_: StreamResult<Msg>) in }

    stream.cancel()
    stream.close()
    #expect(server.packets.count == 2) // Begin + Cancel only
}

@Test private func bidiSendAfterCloseIsDropped() {
    let server = ScriptedServer()
    let stream: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/Chat", headers: [:]
    ) { (_: StreamResult<Msg>) in }

    stream.close()
    stream.send(msg("x"))
    #expect(server.packets.count == 2) // Begin + DoneSending only
}

@Test private func bidiUndecodableStreamedMessageFailsCall() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    _ = server.client.bidirectionalStream(path: "pkg.Svc/Chat", headers: [:]) {
        (result: StreamResult<Msg>) in results.append(result)
    } as any BidirectionalStreamInterface<Msg>

    server.respondMessage(1, body: Data([0xFF])) // truncated varint: cannot decode
    guard case .complete(let code, let error, _) = results.all.first else {
        Issue.record("expected complete")
        return
    }
    #expect(code == .internalError)
    #expect(error != nil)
    guard case .cancel = server.packets[1].frame else {
        Issue.record("expected Cancel frame")
        return
    }
}

@Test private func asyncBidiResultsStreamFinishes() async {
    let server = ScriptedServer()
    let stream: any BidirectionalAsyncStreamInterface<Msg, Msg> =
        server.client.bidirectionalStream(path: "pkg.Svc/Chat", headers: [:])
    try? stream.send(msg("a"))
    server.respondMessage(1, msg("r1"))
    server.respondClose(1)

    var collected = [StreamResult<Msg>]()
    for await result in stream.results() {
        collected.append(result)
    }
    // The loop terminating at all proves the continuation was finished on .complete.
    #expect(collected == [
        .message(msg("r1")),
        .complete(code: .ok, error: nil, trailers: [:]),
    ])
}

@Test private func shutdownFinishesAsyncResultsStream() async {
    let server = ScriptedServer()
    let stream: any BidirectionalAsyncStreamInterface<Msg, Msg> =
        server.client.bidirectionalStream(path: "pkg.Svc/Chat", headers: [:])
    server.client.shutdown()

    var collected = [StreamResult<Msg>]()
    for await result in stream.results() {
        collected.append(result)
    }
    guard case .complete(let code, let error, _) = collected.first, collected.count == 1 else {
        Issue.record("expected a single complete, got \(collected)")
        return
    }
    #expect(code == .unavailable)
    #expect(error != nil)
}

// MARK: - Client-only streams

@Test private func clientOnlyLifecycle() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    let stream: any ClientOnlyStreamInterface<Msg> = server.client.clientOnlyStream(
        path: "pkg.Svc/Upload", headers: [:]
    ) { results.append($0) }

    try? stream.send(msg("a"))
    stream.closeAndReceive()
    guard case .doneSending = server.packets[2].frame else {
        Issue.record("expected DoneSending")
        return
    }

    server.respondClose(
        1, singleResponse: msg("summary"), headers: ["h": ["1"]], trailers: ["t": ["2"]]
    )
    #expect(results.all == [
        .headers(["h": ["1"]]),
        .message(msg("summary")),
        .complete(code: .ok, error: nil, trailers: ["t": ["2"]]),
    ])
}

@Test private func clientOnlyErrorCloseSkipsMessage() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    _ = server.client.clientOnlyStream(path: "pkg.Svc/Upload", headers: [:]) {
        (result: StreamResult<Msg>) in results.append(result)
    } as any ClientOnlyStreamInterface<Msg>

    server.respondClose(1, code: 7, message: "denied")
    guard case .complete(let code, let error, _) = results.all[0] else {
        Issue.record("expected complete")
        return
    }
    #expect(code == .permissionDenied)
    #expect((error as? ConnectError)?.message == "denied")
}

// MARK: - Server-only streams

@Test private func serverOnlyDefersBeginUntilSend() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    let stream: any ServerOnlyStreamInterface<Msg> = server.client.serverOnlyStream(
        path: "pkg.Svc/Watch", headers: ["k": ["v"]]
    ) { results.append($0) }

    #expect(server.packets.isEmpty)
    stream.send(msg("query"))
    #expect(server.packets.count == 1)
    guard case .begin(let begin) = server.packets[0].frame else {
        Issue.record("expected Begin")
        return
    }
    #expect(begin.method == "/pkg.Svc/Watch")
    #expect(try! Msg(serializedBytes: begin.singleRequest) == msg("query"))

    // The single request is exactly-once; extra sends are dropped.
    stream.send(msg("again"))
    #expect(server.packets.count == 1)

    server.respondMessage(1, msg("r1"))
    server.respondMessage(1, msg("r2"))
    server.respondClose(1)
    #expect(results.all == [
        .message(msg("r1")),
        .message(msg("r2")),
        .complete(code: .ok, error: nil, trailers: [:]),
    ])
}

// MARK: - Client-wide behavior

@Test private func interleavedCallsRouteByReqID() {
    let server = ScriptedServer()
    let first = Box<StreamResult<Msg>>()
    let second = Box<StreamResult<Msg>>()
    let stream1: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/A", headers: [:]
    ) { first.append($0) }
    _ = stream1
    let stream2: any BidirectionalStreamInterface<Msg> = server.client.bidirectionalStream(
        path: "pkg.Svc/B", headers: [:]
    ) { second.append($0) }
    _ = stream2

    #expect(server.packets.map(\.reqID) == [1, 2])
    server.respondMessage(2, msg("to-b"))
    server.respondMessage(1, msg("to-a"))
    server.respondClose(2)

    #expect(first.all == [.message(msg("to-a"))])
    #expect(second.all == [
        .message(msg("to-b")),
        .complete(code: .ok, error: nil, trailers: [:]),
    ])
}

@Test private func framesForUnknownReqIDAreDropped() {
    let server = ScriptedServer()
    server.respondClose(42, singleResponse: msg("ghost"))
    server.respondMessage(7, msg("ghost"))
    #expect(server.packets.isEmpty)
}

@Test private func shutdownFailsInFlightAndNewCalls() {
    let server = ScriptedServer()
    let results = Box<StreamResult<Msg>>()
    let responses = Box<ResponseMessage<Msg>>()
    _ = server.client.bidirectionalStream(path: "pkg.Svc/Chat", headers: [:]) {
        (result: StreamResult<Msg>) in results.append(result)
    } as any BidirectionalStreamInterface<Msg>

    server.client.shutdown()
    guard case .complete(let code, _, _) = results.all[0] else {
        Issue.record("expected complete")
        return
    }
    #expect(code == .unavailable)

    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }
    #expect(responses.all.map(\.code) == [.unavailable])
}

@Test private func corruptPacketShutsDown() {
    let server = ScriptedServer()
    let responses = Box<ResponseMessage<Msg>>()
    server.client.unary(
        path: "pkg.Svc/Echo", idempotencyLevel: .unknown, request: msg("in"), headers: [:]
    ) { responses.append($0) }

    server.client.receive(Data([0xFF, 0xFF, 0xFF, 0xFF, 0xFF]))
    #expect(responses.all.map(\.code) == [.unavailable])
}
