import Connect
import Foundation
import Testing

@testable import ProtoServicePipe

/// End-to-end through connect-swift generated code for the repo's example Greeter
/// service, over the scripted packet server.
private func ping(_ msg: String) -> Example_V1_Ping {
    var ping = Example_V1_Ping()
    ping.msg = msg
    return ping
}

private func pong(_ msg: String) -> Example_V1_Pong {
    var pong = Example_V1_Pong()
    pong.msg = msg
    return pong
}

@Test private func generatedUnary() async {
    let server = ScriptedServer()
    let client = Example_V1_GreeterClient(client: server.client)
    let task = Task { await client.unary(request: ping("hello")) }

    await eventually { server.packets.count == 1 }
    guard case .begin(let begin) = server.packets[0].frame else {
        Issue.record("expected Begin")
        return
    }
    #expect(begin.method == "/example.v1.Greeter/Unary")
    #expect(try! Example_V1_Ping(serializedBytes: begin.singleRequest) == ping("hello"))

    server.respondClose(1, singleResponse: pong("HELLO"))
    let response = await task.value
    #expect(response.message == pong("HELLO"))
}

@Test private func generatedServerStream() async {
    let server = ScriptedServer()
    let client = Example_V1_GreeterClient(client: server.client)
    let stream = client.serverStream()
    try? stream.send(ping("topic"))

    guard case .begin(let begin) = server.packets[0].frame else {
        Issue.record("expected Begin")
        return
    }
    #expect(begin.method == "/example.v1.Greeter/ServerStream")
    #expect(begin.hasSingleRequest)

    server.respondMessage(1, pong("a"))
    server.respondMessage(1, pong("b"))
    server.respondClose(1)

    var messages = [String]()
    for await result in stream.results() {
        if case .message(let message) = result {
            messages.append(message.msg)
        }
    }
    #expect(messages == ["a", "b"])
}

@Test private func generatedClientStream() async {
    let server = ScriptedServer()
    let client = Example_V1_GreeterClient(client: server.client)
    let stream = client.clientStream()
    try? stream.send(ping("x"))
    try? stream.send(ping("y"))
    stream.closeAndReceive()

    #expect(server.packets.count == 4) // Begin, Message, Message, DoneSending
    guard case .doneSending = server.packets[3].frame else {
        Issue.record("expected DoneSending")
        return
    }

    server.respondClose(1, singleResponse: pong("xy"))
    var received = [Example_V1_Pong]()
    for await result in stream.results() {
        if case .message(let message) = result {
            received.append(message)
        }
    }
    #expect(received == [pong("xy")])
}

@Test private func generatedBidi() async {
    let server = ScriptedServer()
    let client = Example_V1_GreeterClient(client: server.client)
    let stream = client.bidi()

    // Begin at creation: an MVU view can subscribe without ever dispatching a Msg.
    guard case .begin(let begin) = server.packets[0].frame else {
        Issue.record("expected Begin")
        return
    }
    #expect(begin.method == "/example.v1.Greeter/Bidi")
    #expect(!begin.hasSingleRequest)

    try? stream.send(ping("msg1"))
    server.respondMessage(1, pong("reply1"))
    stream.cancel()
    guard case .cancel = server.packets.last?.frame else {
        Issue.record("expected Cancel")
        return
    }

    var results = [StreamResult<Example_V1_Pong>]()
    for await result in stream.results() {
        results.append(result)
    }
    #expect(results == [
        .message(pong("reply1")),
        .complete(code: .canceled, error: nil, trailers: nil),
    ])
}
