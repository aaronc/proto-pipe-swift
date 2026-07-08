import Foundation

/// A value guarded by a lock. `body` must not call back into the owning client.
final class Locked<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: T

    init(_ value: T) {
        self.value = value
    }

    func withLock<R>(_ body: (inout T) -> R) -> R {
        self.lock.lock()
        defer { self.lock.unlock() }
        return body(&self.value)
    }
}
