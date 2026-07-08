import Connect
import Foundation

func packetMetadata(from headers: Headers) -> ProtoPipe_V1_Metadata {
    var metadata = ProtoPipe_V1_Metadata()
    for (key, values) in headers {
        for value in values {
            var entry = ProtoPipe_V1_Metadata.Entry()
            entry.key = key.lowercased()
            entry.value = value
            metadata.entries.append(entry)
        }
    }
    return metadata
}

func connectHeaders(from metadata: ProtoPipe_V1_Metadata) -> Headers {
    var headers = Headers()
    for entry in metadata.entries {
        headers[entry.key.lowercased(), default: []].append(entry.value)
    }
    return headers
}

func code(from status: ProtoPipe_V1_Status) -> Code {
    return Code(rawValue: Int(status.code)) ?? .unknown
}

func connectError(from close: ProtoPipe_V1_ResponsePacket.Close) -> ConnectError {
    // Error metadata carries leading and trailing metadata merged, as connect does.
    var metadata = connectHeaders(from: close.headers)
    metadata.merge(connectHeaders(from: close.trailers)) { $0 + $1 }
    return ConnectError(
        code: code(from: close.status),
        message: close.status.message.isEmpty ? nil : close.status.message,
        details: close.status.details.map { any in
            ConnectError.Detail(
                type: any.typeURL.split(separator: "/").last.map(String.init) ?? any.typeURL,
                payload: any.value
            )
        },
        metadata: metadata
    )
}

/// Generated connect-swift clients pass paths as "package.Service/Method"; the wire's
/// Begin.method wants "/package.Service/Method".
func wireMethod(from path: String) -> String {
    return path.hasPrefix("/") ? path : "/" + path
}
