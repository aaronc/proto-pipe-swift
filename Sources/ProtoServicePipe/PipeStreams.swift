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
    private let sendHalf: Locked<SendHalf>

    /// A server-only call before its Begin: what to send, and how to start or fail it.
    struct PendingBegin {
        let method: String
        let headers: Headers
        /// Atomically allocates the req_id, registers the session and queues the Begin
        /// (`PipeClient.queueCall`); nil if the client is shut down. Does not flush.
        let start: @Sendable (ProtoPipe_V1_RequestPacket.Begin) -> UInt64?
        /// Completes the call locally (cancelled before Begin, or shut down).
        let fail: @Sendable (Code, String?) -> Void
    }

    private enum SendHalf {
        /// Server-only call whose Begin (with the single request) has not been sent yet;
        /// it has no req_id until then.
        case pendingBegin(PendingBegin)
        case open(reqID: UInt64)
        /// nil req_id: the call never started (shut down, or cancelled before Begin).
        case closed(reqID: UInt64?)
    }

    /// A call whose Begin is already queued; nil `reqID` when the client was shut down.
    init(client: PipeClient, reqID: UInt64?) {
        self.client = client
        self.sendHalf = Locked(reqID.map { .open(reqID: $0) } ?? .closed(reqID: nil))
    }

    init(client: PipeClient, pendingBegin: PendingBegin) {
        self.client = client
        self.sendHalf = Locked(.pendingBegin(pendingBegin))
    }

    /// Send one streamed request message. Sending after close is a caller precondition
    /// violation and is dropped; only encoding failures throw.
    func sendMessage(_ input: some ProtobufMessage) throws {
        let body = try input.serializedData()
        let reqID = self.sendHalf.withLock { half -> UInt64? in
            if case .open(let reqID) = half { return reqID }
            return nil
        }
        guard let reqID, self.client.hasSession(reqID) else { return }
        var message = ProtoPipe_V1_RequestPacket.Message()
        message.body = body
        self.client.sendFrame(.message(message), reqID: reqID)
    }

    /// Send the single request of a server-only call as its Begin frame. At most once;
    /// later calls are dropped. Only encoding failures throw.
    func sendSingleRequest(_ input: some ProtobufMessage) throws {
        let body = try input.serializedData()
        // Start under the send-half lock so a racing cancel sees either the pending call
        // or its req_id, never neither. `start` only queues; flushing (which may re-enter
        // via `send`) happens after the lock is released.
        let started = self.sendHalf.withLock { half -> (PendingBegin, UInt64?)? in
            guard case .pendingBegin(let pending) = half else { return nil }
            var begin = ProtoPipe_V1_RequestPacket.Begin()
            begin.method = pending.method
            begin.headers = packetMetadata(from: pending.headers)
            begin.singleRequest = body
            let reqID = pending.start(begin)
            half = .closed(reqID: reqID)
            return (pending, reqID)
        }
        guard let (pending, reqID) = started else { return }
        if reqID == nil {
            pending.fail(.unavailable, "pipe is shut down")
        } else {
            self.client.flush()
        }
    }

    /// Close the send half (DoneSending). The response direction stays open.
    func closeSend() {
        let reqID = self.sendHalf.withLock { half -> UInt64? in
            guard case .open(let reqID) = half else { return nil }
            half = .closed(reqID: reqID)
            return reqID
        }
        guard let reqID, self.client.hasSession(reqID) else { return }
        self.client.sendFrame(.doneSending(.init()), reqID: reqID)
    }

    /// Abort the call in both directions; the caller sees `.complete(code: .canceled)`.
    func cancel() {
        let previous = self.sendHalf.withLock { half -> SendHalf in
            let previous = half
            switch half {
            case .pendingBegin: half = .closed(reqID: nil)
            case .open(let reqID): half = .closed(reqID: reqID)
            case .closed: break
            }
            return previous
        }
        switch previous {
        case .pendingBegin(let pending):
            // Never reached the server: nothing to Cancel on the wire.
            pending.fail(.canceled, nil)
        case .open(let reqID), .closed(reqID: let reqID?):
            self.client.cancelCall(reqID)
        case .closed(reqID: nil):
            break
        }
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
