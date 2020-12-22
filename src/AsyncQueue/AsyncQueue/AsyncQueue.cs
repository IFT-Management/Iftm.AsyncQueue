using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Iftm.AsyncQueue {


    public abstract class AsyncQueueBase {
        private protected struct State {
            private const int _low30Bits = (1 << 30) - 1;
            private const long _top4bits = 15L << 60;

            public const long WriteCompletedFlag = 1L << 63;
            public const long ReadCompletedFlag = 1L << 62;
            public const long HasReadAwaiterFlag = 1L << 61;
            public const long HasWriteAwaiterFlag = 1L << 60;

            private long _state;

            public bool IsWriteCompleted => (_state & WriteCompletedFlag) != 0;
            public bool IsReadCompleted => (_state & ReadCompletedFlag) != 0;
            public bool HasReadAwaiter => (_state & HasReadAwaiterFlag) != 0;
            public bool HasWriteAwaiter => (_state & HasWriteAwaiterFlag) != 0;

            public int Start => (int)_state & _low30Bits;
            public int Count => (int)(_state >> 30) & _low30Bits;
            public long Flags => _state & _top4bits;

            public State(long flags, int start, int count) {
                Debug.Assert(((flags & HasReadAwaiterFlag) == 0 || count == 0) && start >= 0 && count >= 0);

                _state =
                    flags |
                    (uint)start |
                    ((long)(uint)count << 30)
                    ;
            }

            private State(long state) =>
                _state = state;

            public bool TryWrite(ref State lastKnown, State writeIfSame) {
                var found = Interlocked.CompareExchange(ref _state, writeIfSame._state, lastKnown._state);
                bool success = found == lastKnown._state;
                lastKnown = new State(success ? writeIfSame._state : found);
                return success;
            }

            public static bool operator ==(State x, State y) => x._state == y._state;
            public static bool operator !=(State x, State y) => x._state != y._state;

            public override int GetHashCode() => _state.GetHashCode();

            public override bool Equals(object? obj) => obj is State other && this == other;

            public State ReadAtomic() => new State(Interlocked.Read(ref _state));
        }

        private protected State _state;
        private protected Exception? _exception;

        private protected readonly ReusableTaskCompletionSource<bool> _readWaiter = new ReusableTaskCompletionSource<bool>();
        private protected readonly ReusableTaskCompletionSource<bool> _writeWaiter = new ReusableTaskCompletionSource<bool>();

        private protected AsyncQueueBase() {
        }

        private protected static void ThrowInvalidOperation() => throw new InvalidOperationException();

        public ValueTask DisposeAsync() {
            var state = _state.ReadAtomic();

            for (; ; ) {
                if (state.HasReadAwaiter) ThrowInvalidOperation();
                if (state.IsReadCompleted) break;

                bool hasWriteAwaiters = state.HasWriteAwaiter;
                var newState = new State((state.Flags & State.WriteCompletedFlag) | State.ReadCompletedFlag, state.Start, state.Count);
                if (_state.TryWrite(ref state, newState)) {
                    if (hasWriteAwaiters) _writeWaiter.SetResult(false);
                    break;
                }
            }

            return new ValueTask();
        }

        public void Complete(Exception? ex = null) {
            var writeState = _state.ReadAtomic();

            _exception = ex;

            for (; ; ) {
                if (writeState.IsReadCompleted) break;
                if (writeState.IsWriteCompleted || writeState.HasWriteAwaiter) ThrowInvalidOperation();

                var hasReadAwaiter = writeState.HasReadAwaiter;
                var newState = new State(State.WriteCompletedFlag, writeState.Start, writeState.Count);
                if (_state.TryWrite(ref writeState, newState)) {
                    if (hasReadAwaiter) {
                        if (ex != null) {
                            _readWaiter.SetException(ex);
                        }
                        else {
                            _readWaiter.SetResult(false);
                        }
                    }
                    break;
                }
            }
        }
    }

    public class AsyncQueue<T> : AsyncQueueBase, IAsyncEnumerator<T> {
        private readonly T[] _buffer;
        private readonly int _bufferMask;
        private readonly int _readHisteresis;

        private T _current = default!;
        private T _pendingWriteValue = default!;

        public AsyncQueue(int capacity, int readHisteresis = 1) {
            if (capacity <= 0 || capacity > (1 << 30)) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (readHisteresis <= 0 || readHisteresis > capacity) throw new ArgumentOutOfRangeException(nameof(readHisteresis));

            // if capacity is not a power of two round it up to the first higher power of two
            if (BitOperations.PopCount((uint)capacity) != 1) {
                capacity = 1 << (32 - (int)BitOperations.LeadingZeroCount((uint)capacity));
            }

            _buffer = new T[capacity];
            _bufferMask = capacity - 1;
            _readHisteresis = readHisteresis;
        }

        public ValueTask<bool> MoveNextAsync() {
            var state = _state.ReadAtomic();

            for (; ; ) {
                if (state.IsReadCompleted || state.HasReadAwaiter) ThrowInvalidOperation();
                if (state.IsWriteCompleted && _exception != null) {
                    ExceptionDispatchInfo.Throw(_exception);
                }

                if (state.Count == 0) {
                    if (state.IsWriteCompleted) {
                        return new ValueTask<bool>(false);
                    }
                    else {
                        var newState = new State(State.HasReadAwaiterFlag, state.Start, 0);

                        if (_state.TryWrite(ref state, newState)) {
                            return _readWaiter.GetResultAsync();
                        }
                    }
                }
                else { // if there is data in the buffer, simply return it
                    Debug.Assert(!state.HasWriteAwaiter || state.Count == _buffer.Length);

                    if (state.HasWriteAwaiter) { // the buffer is full and a writer is waiting
                        _current = _buffer[state.Start];
                        _buffer[state.Start] = _pendingWriteValue;

                        var start = (state.Start + 1) & _bufferMask;
                        _state = new State(state.Flags & State.WriteCompletedFlag, start, state.Count);

                        _writeWaiter.SetResult(true);

                        return new ValueTask<bool>(true);
                    }
                    else {
                        _current = _buffer[state.Start];
                        var start = (state.Start + 1) & _bufferMask;
                        var count = state.Count - 1;

                        var newState = new State(state.Flags & State.WriteCompletedFlag, start, count);
                        if (_state.TryWrite(ref state, newState)) {
                            return new ValueTask<bool>(true);
                        }
                    }
                }
            }
        }

        public T Current => _current;

        public ValueTask<bool> WriteAsync(T val) {
            var state = _state.ReadAtomic();

            for (; ; ) {
                if (state.IsReadCompleted) return new ValueTask<bool>(false);
                if (state.IsWriteCompleted || state.HasWriteAwaiter) ThrowInvalidOperation();

                if (state.HasReadAwaiter) {
                    _state = new State(0, state.Start, 0);
                    _current = val;
                    _readWaiter.SetResult(true);
                    return new ValueTask<bool>(true);
                }
                else if (state.Count < _buffer.Length) {
                    var writePos = (state.Start + state.Count) & _bufferMask;
                    _buffer[writePos] = val;

                    var newState = new State(0, state.Start, state.Count + 1);
                    if (_state.TryWrite(ref state, newState)) {
                        return new ValueTask<bool>(true);
                    }
                }
                else {
                    var newState = new State(State.HasWriteAwaiterFlag, state.Start, state.Count);
                    _pendingWriteValue = val;
                    if (_state.TryWrite(ref state, newState)) {
                        return _writeWaiter.GetResultAsync();
                    }
                }
            }
        }
    }
}
