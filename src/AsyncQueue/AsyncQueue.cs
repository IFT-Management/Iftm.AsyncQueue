using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace Iftm.AsyncQueue {
    public interface IAsyncQueueReader<T> {
        ValueTask<ReadOnlyMemory<T>> GetReadBufferAsync();
        void MarkRead(int read);

        void CompleteReader();
        void CompleteReader(Exception ex);
    }

    public interface IAsyncQueueWriter<T> {
        ValueTask<Memory<T>> GetWriteBufferAsync();
        void Commit(int written);

        void CompleteWrite();
        void CompleteWrite(Exception ex);
    }

    public class AsyncQueue<T> : IAsyncQueueReader<T>, IAsyncQueueWriter<T> {
        private readonly T[] _buffer;
        private readonly int _bufferMask, _maxReadBlock, _maxWriteBlock;
        private protected readonly ReusableTaskCompletionSource<ReadOnlyMemory<T>> _readAwaiter = new ReusableTaskCompletionSource<ReadOnlyMemory<T>>();
        private protected readonly ReusableTaskCompletionSource<Memory<T>> _writeAwaiter = new ReusableTaskCompletionSource<Memory<T>>();

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

            _buffer = new T[capacity];
            _bufferMask = capacity - 1;
            _maxReadBlock = maxReadBlock;
            _maxWriteBlock = maxWriteBlock;
        }

        ValueTask<Memory<T>> IAsyncQueueWriter<T>.GetWriteBufferAsync() {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                if (_readerException != null) {
                    return new ValueTask<Memory<T>>(Task.FromException<Memory<T>>(_readerException.SourceException));
                }
                else if (_readerCompleted) {
                    return default;
                }
                else {
                    var freeAmount = _buffer.Length - _count;

                    if (freeAmount == 0) {
                        _awaiterStatus = AwaiterStatus.HasWriteAwaiter;
                        return _writeAwaiter.GetResultAsync();
                    }
                    else {
                        var endOfData = (_start + _count) & _bufferMask;
                        var toEndOfBuffer = _buffer.Length - endOfData;
                        var blockSize = Math.Min(freeAmount, Math.Min(toEndOfBuffer, _maxWriteBlock));

                        var buffer = _buffer.AsMemory(endOfData, blockSize);
                        return new ValueTask<Memory<T>>(buffer);
                    }
                }
            }
        }

        void IAsyncQueueWriter<T>.CompleteWrite() {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                _writerCompleted = true;

                if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _readAwaiter.SetResult(default);
                }
            }
        }

        void IAsyncQueueWriter<T>.CompleteWrite(Exception ex) {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                _writerCompleted = true;
                _writerException = ExceptionDispatchInfo.Capture(ex);

                if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _readAwaiter.SetException(ex);
                }
            }
        }

        void IAsyncQueueWriter<T>.Commit(int written) {
            if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter || _writerCompleted) throw new InvalidOperationException();

            if (written == 0) return;

            lock (_lock) {
                if (_readerCompleted) return;

                var endOfData = (_start + _count) & _bufferMask;

                var toEndOfBuffer = _buffer.Length - endOfData;
                var freeAmount = _buffer.Length - _count;
                if (written > freeAmount || written > _maxWriteBlock || written > toEndOfBuffer) throw new ArgumentOutOfRangeException(nameof(written));

                _count += written;

                if (_awaiterStatus == AwaiterStatus.HasReadAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;

                    var availableToRead = Math.Min(_buffer.Length - _start, Math.Min(_count, _maxReadBlock));
                    var buffer = _buffer.AsMemory(_start, availableToRead);

                    _readAwaiter.SetResult(buffer);
                }
            }
        }

        ValueTask<ReadOnlyMemory<T>> IAsyncQueueReader<T>.GetReadBufferAsync() {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                if (_writerException != null) {
                    return new ValueTask<ReadOnlyMemory<T>>(Task.FromException<ReadOnlyMemory<T>>(_writerException.SourceException));
                }
                else if (_count > 0) {
                    var availableToRead = Math.Min(_buffer.Length - _start, Math.Min(_count, _maxReadBlock));
                    var buffer = _buffer.AsMemory(_start, availableToRead);

                    return new ValueTask<ReadOnlyMemory<T>>(buffer);
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
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted) throw new InvalidOperationException();

            if (read == 0) return;

            lock (_lock) {
                if (read > _count || read > _maxReadBlock || read > _buffer.Length - _start) throw new ArgumentOutOfRangeException(nameof(read));

                _start = (_start + read) & _bufferMask;
                _count -= read;

                if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;

                    var freeAmount = _buffer.Length - _count;
                    var endOfData = (_start + _count) & _bufferMask;
                    var toEndOfBuffer = _buffer.Length - endOfData;
                    var blockSize = Math.Min(freeAmount, Math.Min(toEndOfBuffer, _maxWriteBlock));

                    var buffer = _buffer.AsMemory(endOfData, blockSize);
                    _writeAwaiter.SetResult(buffer);
                }
            }
        }

        void IAsyncQueueReader<T>.CompleteReader() {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                _readerCompleted = true;

                if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _writeAwaiter.SetResult(default);
                }
            }
        }

        void IAsyncQueueReader<T>.CompleteReader(Exception ex) {
            if (_awaiterStatus == AwaiterStatus.HasReadAwaiter || _readerCompleted) throw new InvalidOperationException();

            lock (_lock) {
                _readerCompleted = true;
                _readerException = ExceptionDispatchInfo.Capture(ex);

                if (_awaiterStatus == AwaiterStatus.HasWriteAwaiter) {
                    _awaiterStatus = AwaiterStatus.NoAwaiter;
                    _writeAwaiter.SetException(ex);
                }
            }
        }

        public IAsyncQueueReader<T> AsReader => this;
        public IAsyncQueueWriter<T> AsWriter => this;
    }
}
