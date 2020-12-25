using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Iftm.AsyncQueue {
    public struct AsyncProcessingEnumerable<T> : IAsyncEnumerable<T> {
        private readonly IAsyncEnumerable<T> _enumerable;
        private readonly int _bufferSize, _readChunkSize, _writeChunkSize;

        public AsyncProcessingEnumerable(IAsyncEnumerable<T> enumerable, int bufferSize, int readChunkSize, int writeChunkSize) =>
            (_enumerable, _bufferSize, _readChunkSize, _writeChunkSize) = (enumerable, bufferSize, readChunkSize, writeChunkSize);

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            _enumerable.GetAsyncEnumerator(cancellationToken).ProcessAsynchronously(_bufferSize, _readChunkSize, _writeChunkSize);
    }


    public static class AsyncQueueExtensions {
        private static (int Count, ValueTask<bool> MoveNextTask, bool EnumeratorConsumed) WriteToSpan<T>(Segment<T> span, in T val, IAsyncEnumerator<T> enumerator) {
            int count = 0;

            span[count++] = val;
            var task = enumerator.MoveNextAsync();

            if (span.Length == 1) {
                return (1, task, false);
            }
            else {
                for (; ; ) {
                    if (!task.IsCompleted) return (count, task, false);
                    var hasData = task.GetAwaiter().GetResult();
                    if (!hasData) return (count, default, true);
                    span[count++] = enumerator.Current;
                    task = enumerator.MoveNextAsync();

                    if (count == span.Length) return (count, task, false);
                }
            }
        }

        public static async IAsyncEnumerator<T> ProcessAsynchronously<T>(this IAsyncEnumerator<T> enumerator, int bufferSize, int readChunkSize, int writeChunkSize) {
            static async void WriteToQueue(IAsyncEnumerator<T> enumerator, IAsyncQueueWriter<T> queue) {
                try {
                    var writeBuffer = await queue.GetWriteBufferAsync().ConfigureAwait(false);
                    if (writeBuffer.Length == 0) return;

                    var hasMore = await enumerator.MoveNextAsync().ConfigureAwait(false);

                    while (hasMore) {
                        var (count, moveNextTask, enumeratorConsumed) = WriteToSpan(writeBuffer, enumerator.Current, enumerator);
                        if (enumeratorConsumed) {
                            queue.Commit(count);
                            break;
                        }
                        else {
                            writeBuffer = await queue.GetWriteBufferAsync(count).ConfigureAwait(false);
                            if (writeBuffer.Length == 0) break;

                            hasMore = await moveNextTask.ConfigureAwait(false);
                        }
                    }

                    queue.CompleteWrite();
                }
                catch (Exception e) {
                    queue.CompleteWrite(e);
                }
                finally {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }
            }

            var queue = new AsyncQueue<T>(bufferSize, readChunkSize, writeChunkSize);

            ThreadPool.QueueUserWorkItem(_ => WriteToQueue(enumerator, queue), null);

            var reader = queue.AsReader;

            using var readCompleter = reader.CompleteReadOnDispose();

            int markAsRead = 0;
            for (; ; ) {
                var readBuffer = await reader.GetReadBufferAsync(markAsRead).ConfigureAwait(false);
                if (readBuffer.Length == 0) break;

                foreach (var x in readBuffer) {
                    yield return x;
                }

                markAsRead = readBuffer.Length;
            }
        }

        public static AsyncProcessingEnumerable<T> ProcessAsynchronously<T>(this IAsyncEnumerable<T> enumerable, int bufferSize, int readChunkSize, int writeChunkSize) =>
            new AsyncProcessingEnumerable<T>(enumerable, bufferSize, readChunkSize, writeChunkSize);

        public static CompleteReadOnDispose<T> CompleteReadOnDispose<T>(this IAsyncQueueReader<T> reader) => new CompleteReadOnDispose<T>(reader);
    }

    public readonly struct CompleteReadOnDispose<T> : IDisposable {
        private readonly IAsyncQueueReader<T> _queue;

        public CompleteReadOnDispose(IAsyncQueueReader<T> queue) {
            _queue = queue;
        }

        public void Dispose() {
            _queue.CompleteReader();
        }
    }

}
