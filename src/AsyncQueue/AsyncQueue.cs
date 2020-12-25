using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace Iftm.AsyncQueue {
    public struct ReadOnlySegment<T> : IEnumerable<T> {
        public readonly T[] Array;
        public readonly int Start, Length;

        public ReadOnlySegment(T[] array, int start, int length) {
            Array = array;
            Start = start;
            Length = length;
        }

        public readonly ref T this[int index] {
            get {
                if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
                return ref Array[Start + index];
            }
        }

        public IEnumerator<T> GetEnumerator() {
            int end = Start + Length;
            for (int x = Start; x < end; ++x) yield return Array[x];
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public ReadOnlySpan<T> Span => Array.AsSpan(Start, Length);
    }

    public struct Segment<T> {
        public readonly T[] Array;
        public readonly int Start, Length;

        public Segment(T[] array, int start, int length) {
            Array = array;
            Start = start;
            Length = length;
        }

        public ref T this[int index] {
            get {
                if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
                return ref Array[Start + index];
            }
        }

        public Span<T> Span => Array.AsSpan(Start, Length);
    }

    public interface IAsyncQueueReader<T> {
        ValueTask<ReadOnlySegment<T>> GetReadBufferAsync(int markRead = 0);
        void MarkRead(int read);

        void CompleteReader();
        void CompleteReader(Exception ex);
    }

    public interface IAsyncQueueWriter<T> {
        ValueTask<Segment<T>> GetWriteBufferAsync(int commit = 0);
        void Commit(int written);

        void CompleteWrite();
        void CompleteWrite(Exception ex);
    }

    public class AsyncQueue<T> : IAsyncQueueReader<T>, IAsyncQueueWriter<T> {
        private T[]? _buffer;
        private readonly int _capacity;
        private readonly int _bufferMask, _maxReadBlock, _maxWriteBlock;
        private protected readonly ReusableTaskCompletionSource<ReadOnlySegment<T>> _readAwaiter = new ReusableTaskCompletionSource<ReadOnlySegment<T>>();
        private protected readonly ReusableTaskCompletionSource<Segment<T>> _writeAwaiter = new ReusableTaskCompletionSource<Segment<T>>();

        private enum AwaiterStatus {
            NoAwaiter,
            HasReadAwaiter,
            HasWriteAwaiter
        }

        private AwaiterStatus _awaiterStatus;

        private readonly object _lock = new object();
        private int _start, _count;
        private bool _readerCompleted, _writerCompleted;
        private ExceptionDispatchInfo? _readerException, _writerException;

        public AsyncQueue(int capacity, int maxReadBlock = 1, int maxWriteBlock = 1) {
            if (capacity <= 0 || capacity > (1 << 30)) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxReadBlock < 1 || maxReadBlock > capacity) throw new ArgumentOutOfRangeException(nameof(maxReadBlock));
            if (maxWriteBlock < 1 || maxWriteBlock > capacity) throw new ArgumentOutOfRangeException(nameof(maxReadBlock));

            // if capacity is not a power of two round it up to the first higher power of two
            if (BitOperations.PopCount((uint)capacity) != 1) {
                capacity = 1 << (32 - (int)BitOperations.LeadingZeroCount((uint)capacity));
            }

            _capacity = capacity;
            _buffer = ArrayPool<T>.Shared.Rent(capacity);
            _bufferMask = capacity - 1;
            _maxReadBlock = maxReadBlock;
            _maxWriteBlock = maxWriteBlock;
        }

        ValueTask<Segment<T>> IAsyncQueueWriter<T>.GetWriteBufferAsync(int commitSize) {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                if (commitSize > 0) CommitInternal(commitSize);

                if (_readerException != null) {
                    return new ValueTask<Segment<T>>(Task.FromException<Segment<T>>(_readerException.SourceException));
                }
                else if (_readerCompleted) {
                    return default;
                }
                else {
                    var freeAmount = _capacity - _count;

                    if (freeAmount == 0) {
                        _awaiterStatus = AwaiterStatus.HasWriteAwaiter;
                        return _writeAwaiter.GetResultAsync();
                    }
                    else {
                        var endOfData = (_start + _count) & _bufferMask;
                        var toEndOfBuffer = _capacity - endOfData;
                        var blockSize = Math.Min(freeAmount, Math.Min(toEndOfBuffer, _maxWriteBlock));

                        return new ValueTask<Segment<T>>(new Segment<T>(_buffer, endOfData, blockSize));
                    }
                }
            }
        }

        private void ReturnBufferToPool() {
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) {
                Array.Clear(_buffer!, 0, _capacity);
            }

            ArrayPool<T>.Shared.Return(_buffer!);
            _buffer = default!;
        }

        void IAsyncQueueWriter<T>.CompleteWrite() {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                _writerCompleted = true;

                if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _readAwaiter.SetResult(default);
                }
                else if (_readerCompleted) {
                    ReturnBufferToPool();
                }
            }
        }

        void IAsyncQueueWriter<T>.CompleteWrite(Exception ex) {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                _writerCompleted = true;
                _writerException = ExceptionDispatchInfo.Capture(ex);

                if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _readAwaiter.SetException(ex);
                }
                else if (_readerCompleted) {
                    ReturnBufferToPool();
                }
            }
        }

        void IAsyncQueueWriter<T>.Commit(int written) {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted || _buffer == null) throw new InvalidOperationException();

            if (written == 0) return;

            lock (_lock) {
                CommitInternal(written);
            }
        }

        private void CommitInternal(int written) {
            if (_readerCompleted) return;

            var endOfData = (_start + _count) & _bufferMask;

            var toEndOfBuffer = _capacity - endOfData;
            var freeAmount = _capacity - _count;
            if (written > freeAmount || written > _maxWriteBlock || written > toEndOfBuffer) throw new ArgumentOutOfRangeException(nameof(written));

            _count += written;

            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                _awaiterStatus = AwaiterStatus.NoAwaiter;

                var availableToRead = Math.Min(_capacity - _start, Math.Min(_count, _maxReadBlock));

                _readAwaiter.SetResult(new ReadOnlySegment<T>(_buffer!, _start, availableToRead));
            }
        }

        ValueTask<ReadOnlySegment<T>> IAsyncQueueReader<T>.GetReadBufferAsync(int markRead) {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                if (markRead > 0) MarkReadInternal(markRead);

                if (_writerException != null) {
                    return new ValueTask<ReadOnlySegment<T>>(Task.FromException<ReadOnlySegment<T>>(_writerException.SourceException));
                }
                else if (_count > 0) {
                    var availableToRead = Math.Min(_capacity - _start, Math.Min(_count, _maxReadBlock));
                    return new ValueTask<ReadOnlySegment<T>>(new ReadOnlySegment<T> (_buffer, _start, availableToRead));
                }
                else if (_writerCompleted) {
                    return default;
                }
                else {
                    _awaiterStatus = AwaiterStatus.HasReadAwaiter;
                    return _readAwaiter.GetResultAsync();
                }
            }
        }

        void IAsyncQueueReader<T>.MarkRead(int read) {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted || _buffer == null) throw new InvalidOperationException();

            if (read == 0) return;

            lock (_lock) {
                MarkReadInternal(read);
            }
        }

        private void MarkReadInternal(int read) {
            if (read > _count || read > _maxReadBlock || read > _capacity - _start) throw new ArgumentOutOfRangeException(nameof(read));

            _start = (_start + read) & _bufferMask;
            _count -= read;

            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                _awaiterStatus = AwaiterStatus.NoAwaiter;

                var freeAmount = _capacity - _count;
                var endOfData = (_start + _count) & _bufferMask;
                var toEndOfBuffer = _capacity - endOfData;
                var blockSize = Math.Min(freeAmount, Math.Min(toEndOfBuffer, _maxWriteBlock));

                _writeAwaiter.SetResult(new Segment<T>(_buffer!, endOfData, blockSize));
            }
        }

        void IAsyncQueueReader<T>.CompleteReader() {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                _readerCompleted = true;

                if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _writeAwaiter.SetResult(default);
                }
                else if (_writerCompleted) {
                    ReturnBufferToPool();
                }
            }
        }

        void IAsyncQueueReader<T>.CompleteReader(Exception ex) {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted || _buffer == null) throw new InvalidOperationException();

            lock (_lock) {
                _readerCompleted = true;
                _readerException = ExceptionDispatchInfo.Capture(ex);

                if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _writeAwaiter.SetException(ex);
                }
                else if (_writerCompleted) {
                    ReturnBufferToPool();
                }
            }
        }

        public IAsyncQueueReader<T> AsReader => this;
        public IAsyncQueueWriter<T> AsWriter => this;
    }
}
