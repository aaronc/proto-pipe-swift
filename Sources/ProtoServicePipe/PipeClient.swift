import Connect
import Foundation
import SwiftProtobuf

/// A connect-swift `ProtocolClientInterface` that speaks the proto-service-rs packet
/// protocol over an injected bidirectional byte pipe.
///
/// The client owns no I/O: `send` receives encoded `RequestPacket` bytes, and the
/// application feeds encoded `ResponsePacket` bytes to `receive(_:)`. Wire it to
/// UniFFI, a Tauri channel, a WebSocket, or an in-process loopback.
public final class PipeClient: @unchecked Sendable {
    private let sendData: @Sendable (Data) -> Void
    private let state = Locked(State())

    private struct State {
        var nextReqID: UInt64 = 1
        var sessions = [UInt64: Session]()
        var isShutdown = false
    }

    /// One in-flight call, type-erased. Closures are invoked outside the client's lock,
    /// in `receive(_:)` order.
    private struct Session {
        /// Whether streamed Message frames are valid for this call (response-streaming
        /// shapes only); a Message frame for a single-response call aborts it.
        let acceptsMessages: Bool
        let onHeaders: @Sendable (Headers) -> Void
        let onMessage: @Sendable (Data) -> Void
        let onClose: @Sendable (SessionClose) -> Void
    }

    enum SessionClose {
        case remote(ProtoPipe_V1_ResponsePacket.Close)
        case local(Code, String?)
    }

    /// - parameter send: Called with each encoded `RequestPacket`. Must preserve call
    ///   order on the pipe. May be called from any thread, including synchronously from
    ///   within `receive(_:)` handling.
    public init(send: @escaping @Sendable (Data) -> Void) {
        self.sendData = send
    }

    deinit {
        self.shutdown()
    }

    /// Feed one encoded `ResponsePacket` from the pipe.
    ///
    /// Must be called serially and in pipe order — concurrent calls lose frame ordering.
    /// Undecodable bytes mean the pipe framing is corrupt: the client shuts down.
    public func receive(_ data: Data) {
        guard let packet = try? ProtoPipe_V1_ResponsePacket(serializedBytes: data) else {
            self.shutdown()
            return
        }
        switch packet.frame {
        case .headers(let headers):
            // Frames for unknown req_ids may trail a Cancel; drop them.
            guard let session = self.session(packet.reqID) else { return }
            session.onHeaders(connectHeaders(from: headers.metadata))
        case .message(let message):
            guard let session = self.session(packet.reqID) else { return }
            if session.acceptsMessages {
                session.onMessage(message.body)
            } else {
                self.terminate(
                    reqID: packet.reqID,
                    code: .internalError,
                    message: "received streamed message for a single-response call",
                    sendCancel: true
                )
            }
        case .close(let close):
            guard let session = self.removeSession(packet.reqID) else { return }
            session.onClose(.remote(close))
        case .none:
            return
        }
    }

    /// Fail every in-flight call with `.unavailable` and refuse new ones. Call when the
    /// underlying pipe is gone.
    public func shutdown() {
        let sessions = self.state.withLock { state -> [Session] in
            state.isShutdown = true
            let open = Array(state.sessions.values)
            state.sessions.removeAll()
            return open
        }
        for session in sessions {
            session.onClose(.local(.unavailable, "pipe is shut down"))
        }
    }

    // MARK: - Internal call plumbing

    private func session(_ reqID: UInt64) -> Session? {
        return self.state.withLock { $0.sessions[reqID] }
    }

    private func removeSession(_ reqID: UInt64) -> Session? {
        return self.state.withLock { $0.sessions.removeValue(forKey: reqID) }
    }

    func hasSession(_ reqID: UInt64) -> Bool {
        return self.state.withLock { $0.sessions[reqID] != nil }
    }

    private func allocateReqID() -> UInt64 {
        return self.state.withLock { state in
            let reqID = state.nextReqID
            state.nextReqID += 1
            return reqID
        }
    }

    /// Returns false (without inserting) if the client is shut down.
    private func insert(_ session: Session, reqID: UInt64) -> Bool {
        return self.state.withLock { state in
            guard !state.isShutdown else { return false }
            state.sessions[reqID] = session
            return true
        }
    }

    func sendFrame(_ frame: ProtoPipe_V1_RequestPacket.OneOf_Frame, reqID: UInt64) {
        var packet = ProtoPipe_V1_RequestPacket()
        packet.reqID = reqID
        packet.frame = frame
        self.sendData(try! packet.serializedData())
    }

    /// Locally end a call: remove its session, optionally send Cancel, deliver the close.
    /// No-op if the call already completed.
    private func terminate(reqID: UInt64, code: Code, message: String?, sendCancel: Bool) {
        guard let session = self.removeSession(reqID) else { return }
        if sendCancel {
            self.sendFrame(.cancel(.init()), reqID: reqID)
        }
        session.onClose(.local(code, message))
    }

    func cancelCall(_ reqID: UInt64) {
        self.terminate(reqID: reqID, code: .canceled, message: nil, sendCancel: true)
    }

    // MARK: - Shape-independent stream opening

    /// Session for a response-streaming call (bidi / server-only): Headers and Message
    /// frames pass through; Close maps to `.complete`.
    private func streamingResponseSession<Output: ProtobufMessage>(
        reqID: UInt64,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> Session {
        return Session(
            acceptsMessages: true,
            onHeaders: { onResult(.headers($0)) },
            onMessage: { [weak self] body in
                if let message = try? Output(serializedBytes: body) {
                    onResult(.message(message))
                } else {
                    self?.terminate(
                        reqID: reqID,
                        code: .internalError,
                        message: "undecodable streamed message",
                        sendCancel: true
                    )
                }
            },
            onClose: { onResult(Self.streamComplete(from: $0)) }
        )
    }

    /// Session for a single-response streaming call (client-only): the entire response
    /// rides the Close frame and unfolds into headers/message/complete results.
    private func singleResponseSession<Output: ProtobufMessage>(
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> Session {
        return Session(
            acceptsMessages: false,
            onHeaders: { _ in },
            onMessage: { _ in },
            onClose: { close in
                switch close {
                case .remote(let close) where close.status.code == 0:
                    let trailers = connectHeaders(from: close.trailers)
                    if close.hasHeaders {
                        onResult(.headers(connectHeaders(from: close.headers)))
                    }
                    guard close.hasSingleResponse,
                        let message = try? Output(serializedBytes: close.singleResponse)
                    else {
                        onResult(.complete(
                            code: .internalError,
                            error: ConnectError(
                                code: .internalError,
                                message: "missing or undecodable single response"
                            ),
                            trailers: trailers
                        ))
                        return
                    }
                    onResult(.message(message))
                    onResult(.complete(code: .ok, error: nil, trailers: trailers))
                default:
                    onResult(Self.streamComplete(from: close))
                }
            }
        )
    }

    private static func streamComplete<Output>(from close: SessionClose) -> StreamResult<Output> {
        switch close {
        case .remote(let close):
            let trailers = connectHeaders(from: close.trailers)
            guard close.status.code == 0 else {
                return .complete(
                    code: code(from: close.status),
                    error: connectError(from: close),
                    trailers: trailers
                )
            }
            return .complete(code: .ok, error: nil, trailers: trailers)
        case .local(let code, let message):
            return .complete(
                code: code,
                error: message.map { ConnectError(code: code, message: $0) },
                trailers: nil
            )
        }
    }

    /// Open a streaming-request call (bidi / client-only): registers the session and
    /// sends Begin (no payload) immediately, so the server call starts even if the
    /// caller never sends a request message.
    func openStream<Output: ProtobufMessage>(
        path: String,
        headers: Headers,
        responseStreaming: Bool,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> PipeStreamCore {
        let reqID = self.allocateReqID()
        let session: Session = responseStreaming
            ? self.streamingResponseSession(reqID: reqID, onResult: onResult)
            : self.singleResponseSession(onResult: onResult)
        let core = PipeStreamCore(client: self, reqID: reqID)
        guard self.insert(session, reqID: reqID) else {
            onResult(Self.streamComplete(from: .local(.unavailable, "pipe is shut down")))
            return core
        }
        var begin = ProtoPipe_V1_RequestPacket.Begin()
        begin.method = wireMethod(from: path)
        begin.headers = packetMetadata(from: headers)
        self.sendFrame(.begin(begin), reqID: reqID)
        return core
    }

    /// Open a server-only call. Begin (carrying the single request) is deferred until
    /// the handle's one `send`, matching connect-swift's server-only semantics.
    func openServerOnlyStream<Output: ProtobufMessage>(
        path: String,
        headers: Headers,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> PipeStreamCore {
        let reqID = self.allocateReqID()
        let session: Session = self.streamingResponseSession(reqID: reqID, onResult: onResult)
        let core = PipeStreamCore(
            client: self,
            reqID: reqID,
            pendingBegin: (wireMethod(from: path), headers)
        )
        guard self.insert(session, reqID: reqID) else {
            onResult(Self.streamComplete(from: .local(.unavailable, "pipe is shut down")))
            return core
        }
        return core
    }

    private static func unaryResponse<Output: ProtobufMessage>(
        from close: SessionClose
    ) -> ResponseMessage<Output> {
        switch close {
        case .remote(let close):
            let trailers = connectHeaders(from: close.trailers)
            let responseHeaders = connectHeaders(from: close.headers)
            guard close.status.code == 0 else {
                return ResponseMessage(
                    code: code(from: close.status),
                    headers: responseHeaders,
                    result: .failure(connectError(from: close)),
                    trailers: trailers
                )
            }
            guard close.hasSingleResponse,
                let message = try? Output(serializedBytes: close.singleResponse)
            else {
                return ResponseMessage(
                    code: .internalError,
                    headers: responseHeaders,
                    result: .failure(ConnectError(
                        code: .internalError,
                        message: "missing or undecodable single response"
                    )),
                    trailers: trailers
                )
            }
            return ResponseMessage(
                headers: responseHeaders,
                result: .success(message),
                trailers: trailers
            )
        case .local(let code, let message):
            return ResponseMessage(
                code: code,
                result: .failure(ConnectError(code: code, message: message))
            )
        }
    }
}

// MARK: - ProtocolClientInterface

extension PipeClient: ProtocolClientInterface {
    @discardableResult
    public func unary<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        idempotencyLevel: IdempotencyLevel,
        request: Input,
        headers: Headers,
        completion: @escaping @Sendable (ResponseMessage<Output>) -> Void
    ) -> Cancelable {
        let requestBytes: Data
        do {
            requestBytes = try request.serializedData()
        } catch {
            completion(ResponseMessage(
                code: .internalError,
                result: .failure(ConnectError(
                    code: .internalError,
                    message: "failed to encode request: \(error)"
                ))
            ))
            return Cancelable {}
        }

        let reqID = self.allocateReqID()
        let session = Session(
            acceptsMessages: false,
            onHeaders: { _ in },
            onMessage: { _ in },
            onClose: { completion(Self.unaryResponse(from: $0)) }
        )
        guard self.insert(session, reqID: reqID) else {
            completion(Self.unaryResponse(from: .local(.unavailable, "pipe is shut down")))
            return Cancelable {}
        }

        var begin = ProtoPipe_V1_RequestPacket.Begin()
        begin.method = wireMethod(from: path)
        begin.headers = packetMetadata(from: headers)
        begin.singleRequest = requestBytes
        self.sendFrame(.begin(begin), reqID: reqID)
        return Cancelable { [weak self] in self?.cancelCall(reqID) }
    }

    public func bidirectionalStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> any BidirectionalStreamInterface<Input> {
        return PipeRequestStream<Input>(
            core: self.openStream(
                path: path, headers: headers, responseStreaming: true, onResult: onResult
            )
        )
    }

    public func clientOnlyStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> any ClientOnlyStreamInterface<Input> {
        return PipeRequestStream<Input>(
            core: self.openStream(
                path: path, headers: headers, responseStreaming: false, onResult: onResult
            )
        )
    }

    public func serverOnlyStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers,
        onResult: @escaping @Sendable (StreamResult<Output>) -> Void
    ) -> any ServerOnlyStreamInterface<Input> {
        return PipeServerOnlyStream<Input>(
            core: self.openServerOnlyStream(path: path, headers: headers, onResult: onResult)
        )
    }

    public func unary<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        idempotencyLevel: IdempotencyLevel,
        request: Input,
        headers: Headers
    ) async -> ResponseMessage<Output> {
        let cancelable = Locked<Cancelable?>(nil)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                let handle: Cancelable = self.unary(
                    path: path,
                    idempotencyLevel: idempotencyLevel,
                    request: request,
                    headers: headers
                ) { (response: ResponseMessage<Output>) in
                    continuation.resume(returning: response)
                }
                cancelable.withLock { $0 = handle }
                // The task may have been cancelled before the handle was stored, making
                // onCancel a no-op; cancelCall is idempotent, so cancel again here.
                if Task.isCancelled {
                    handle.cancel()
                }
            }
        } onCancel: {
            cancelable.withLock { $0 }?.cancel()
        }
    }

    public func bidirectionalStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers
    ) -> any BidirectionalAsyncStreamInterface<Input, Output> {
        let (stream, onResult) = Self.bufferedResults(of: Output.self)
        return PipeAsyncStream<Input, Output>(
            core: self.openStream(
                path: path, headers: headers, responseStreaming: true, onResult: onResult
            ),
            stream: stream
        )
    }

    public func clientOnlyStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers
    ) -> any ClientOnlyAsyncStreamInterface<Input, Output> {
        let (stream, onResult) = Self.bufferedResults(of: Output.self)
        return PipeAsyncStream<Input, Output>(
            core: self.openStream(
                path: path, headers: headers, responseStreaming: false, onResult: onResult
            ),
            stream: stream
        )
    }

    public func serverOnlyStream<Input: ProtobufMessage, Output: ProtobufMessage>(
        path: String,
        headers: Headers
    ) -> any ServerOnlyAsyncStreamInterface<Input, Output> {
        let (stream, onResult) = Self.bufferedResults(of: Output.self)
        return PipeAsyncServerOnlyStream<Input, Output>(
            core: self.openServerOnlyStream(path: path, headers: headers, onResult: onResult),
            stream: stream
        )
    }

    /// An AsyncStream fed by an onResult callback; finishes after `.complete` so
    /// consumers' `for await` loops end (SE-0406: ending the source alone would not).
    private static func bufferedResults<Output: Sendable>(
        of _: Output.Type
    ) -> (AsyncStream<StreamResult<Output>>, @Sendable (StreamResult<Output>) -> Void) {
        var streamContinuation: AsyncStream<StreamResult<Output>>.Continuation!
        let stream = AsyncStream<StreamResult<Output>> { streamContinuation = $0 }
        let continuation = streamContinuation!
        return (
            stream,
            { result in
                continuation.yield(result)
                if case .complete = result {
                    continuation.finish()
                }
            }
        )
    }
}
