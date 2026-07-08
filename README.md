# proto-pipe-swift

A [connect-swift](https://github.com/connectrpc/connect-swift) `ProtocolClientInterface`
that speaks the proto-service-rs packet protocol over an injected bidirectional byte
pipe. Generated connect-swift clients work unchanged on top; the pipe underneath can be
UniFFI, a Tauri channel, a WebSocket, or an in-process loopback — the library does no
I/O of its own.

```swift
let pipe = PipeClient { requestPacketBytes in
    // hand encoded RequestPacket bytes to your transport
}
// feed encoded ResponsePacket bytes from your transport:
pipe.receive(responsePacketBytes)

let client = Myapp_V1_FooServiceClient(client: pipe)
let response = await client.getThing(request: request)
```

## Seam contract

- `receive(_:)` must be called serially, in pipe order. From a transport callback that
  fires on arbitrary threads, funnel through one ordered consumer first.
- The `send` closure must preserve call order onto the pipe. It may be invoked from any
  thread, including synchronously from within `receive(_:)` handling.
- Call `shutdown()` when the pipe dies; in-flight calls complete with `.unavailable`.
- The server must ignore frames for unknown req_ids: a send racing a call's completion
  can put a Message frame on the wire after Close/Cancel.

## Codegen

Messages come from `buf.build/apple/swift`, clients from `buf.build/connectrpc/swift`:

```yaml
version: v2
plugins:
  - remote: buf.build/apple/swift
    out: Sources/MyApp/Generated
  - remote: buf.build/connectrpc/swift
    out: Sources/MyApp/Generated
```

## Platforms

iOS 13 / macOS 10.15, matching connect-swift. Linux builds (dev/test only) currently
require a locally patched connect-swift checkout (`swift package edit connect-swift` +
`#if canImport(os)` guards around its Apple-only corners) until those patches are
upstreamed; Apple-platform consumers need nothing.
