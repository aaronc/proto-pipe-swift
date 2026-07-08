import Connect
import Foundation
import SwiftProtobuf

/// Send-half state machine shared by every stream handle for one call.
///
/// A racing completion (server Close, cancel, shutdown) can remove the session between
/// the `hasSession` check and the send; the frame then goes out for a dead req_id, which
/// the wire protocol requires the server to ignore.
final class PipeStreamCore: @unchecked Sendable {
    private let client: PipeClient
    private let reqID: UInt64
    private let sendHalf: Locked<SendHalf>

    private enum SendHalf {
        /// Server-only call whose Begin (with the single request) has not been sent yet.
        case pendingBegin(method: String, headers: Headers)
        case open
        case closed
    }

    init(client: PipeClient, reqID: UInt64, pendingBegin: (String, Headers)? = nil) {
        self.client = client
        self.reqID = reqID
        self.sendHalf = Locked(
            pendingBegin.map { .pendingBegin(method: $0.0, headers: $0.1) } ?? .open
        )
    }

    /// Send one streamed request message. Sending after close is a caller precondition
    /// violation and is dropped; only encoding failures throw.
    func sendMessage(_ input: some ProtobufMessage) throws {
        let body = try input.serializedData()
        let isOpen = self.sendHalf.withLock { half in
            if case .open = half { return true }
            return false
        }
        guard isOpen, self.client.hasSession(self.reqID) else { return }
        var message = ProtoPipe_V1_RequestPacket.Message()
        message.body = body
        self.client.sendFrame(.message(message), reqID: self.reqID)
    }

    /// Send the single request of a server-only call as its Begin frame. At most once;
    /// later calls are dropped. Only encoding failures throw.
    func sendSingleRequest(_ input: some ProtobufMessage) throws {
        let body = try input.serializedData()
        let pending: (String, Headers)? = self.sendHalf.withLock { half in
            guard case .pendingBegin(let method, let headers) = half else { return nil }
            half = .closed
            return (method, headers)
        }
        guard let (method, headers) = pending, self.client.hasSession(self.reqID) else { return }
        var begin = ProtoPipe_V1_RequestPacket.Begin()
        begin.method = method
        begin.headers = packetMetadata(from: headers)
        begin.singleRequest = body
        self.client.sendFrame(.begin(begin), reqID: self.reqID)
    }

    /// Close the send half (DoneSending). The response direction stays open.
    func closeSend() {
        let wasOpen = self.sendHalf.withLock { half in
            if case .open = half {
                half = .closed
                return true
            }
            return false
        }
        guard wasOpen, self.client.hasSession(self.reqID) else { return }
        self.client.sendFrame(.doneSending(.init()), reqID: self.reqID)
    }

    /// Abort the call in both directions; the caller sees `.complete(code: .canceled)`.
    func cancel() {
        self.sendHalf.withLock { $0 = .closed }
        self.client.cancelCall(self.reqID)
    }
}

/// Callback handle for bidirectional and client-only streams. The non-throwing `send`
/// witnesses both protocols; encoding failures (practically impossible for a valid
/// proto3 message) are dropped.
final class PipeRequestStream<Input: ProtobufMessage>: @unchecked Sendable,
    BidirectionalStreamInterface, ClientOnlyStreamInterface
{
    private let core: PipeStreamCore

    init(core: PipeStreamCore) {
        self.core = core
    }

    @discardableResult
    func send(_ input: Input) -> Self {
        try? self.core.sendMessage(input)
        return self
    }

    func close() {
        self.core.closeSend()
    }

    func closeAndReceive() {
        self.core.closeSend()
    }

    func cancel() {
        self.core.cancel()
    }
}

/// Callback handle for server-only streams.
final class PipeServerOnlyStream<Input: ProtobufMessage>: @unchecked Sendable,
    ServerOnlyStreamInterface
{
    private let core: PipeStreamCore

    init(core: PipeStreamCore) {
        self.core = core
    }

    func send(_ input: Input) {
        try? self.core.sendSingleRequest(input)
    }

    func cancel() {
        self.core.cancel()
    }
}

/// Async handle for bidirectional and client-only streams. `results()` is
/// single-consumer: it returns the same stream every call.
final class PipeAsyncStream<Input: ProtobufMessage, Output: ProtobufMessage>:
    @unchecked Sendable, BidirectionalAsyncStreamInterface, ClientOnlyAsyncStreamInterface
{
    private let core: PipeStreamCore
    private let stream: AsyncStream<StreamResult<Output>>

    init(core: PipeStreamCore, stream: AsyncStream<StreamResult<Output>>) {
        self.core = core
        self.stream = stream
    }

    @discardableResult
    func send(_ input: Input) throws -> Self {
        try self.core.sendMessage(input)
        return self
    }

    func results() -> AsyncStream<StreamResult<Output>> {
        return self.stream
    }

    func close() {
        self.core.closeSend()
    }

    func closeAndReceive() {
        self.core.closeSend()
    }

    func cancel() {
        self.core.cancel()
    }
}

/// Async handle for server-only streams.
final class PipeAsyncServerOnlyStream<Input: ProtobufMessage, Output: ProtobufMessage>:
    @unchecked Sendable, ServerOnlyAsyncStreamInterface
{
    private let core: PipeStreamCore
    private let stream: AsyncStream<StreamResult<Output>>

    init(core: PipeStreamCore, stream: AsyncStream<StreamResult<Output>>) {
        self.core = core
        self.stream = stream
    }

    func send(_ input: Input) throws {
        try self.core.sendSingleRequest(input)
    }

    func results() -> AsyncStream<StreamResult<Output>> {
        return self.stream
    }

    func cancel() {
        self.core.cancel()
    }
}
