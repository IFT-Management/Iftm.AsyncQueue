using System;
using System.Collections.Generic;
using System.Threading;

namespace Iftm.AsyncQueue {
    public struct AsyncProcessingEnumerable<T> : IAsyncEnumerable<T> {
        private readonly IAsyncEnumerable<T> _enumerable;
        private readonly int _bufferSize;

        public AsyncProcessingEnumerable(IAsyncEnumerable<T> enumerable, int bufferSize) =>
            (_enumerable, _bufferSize) = (enumerable, bufferSize);

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            _enumerable.GetAsyncEnumerator(cancellationToken).ProcessAsynchronously(_bufferSize);
    }


    public static class AsyncQueueExtensions {
        public static IAsyncEnumerator<T> ProcessAsynchronously<T>(this IAsyncEnumerator<T> enumerator, int bufferSize) {
            static async void WriteToQueue(IAsyncEnumerator<T> enumerator, AsyncQueue<T> queue) {
                try {
                    while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
                        if (!await queue.WriteAsync(enumerator.Current).ConfigureAwait(false)) {
                            break;
                        }
                    }
                    queue.Complete();
                }
                catch (Exception e) {
                    queue.Complete(e);
                }
                finally {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }
            }

            var queue = new AsyncQueue<T>(bufferSize);
            WriteToQueue(enumerator, queue);
            return queue;
        }

        public static AsyncProcessingEnumerable<T> ProcessAsynchronously<T>(this IAsyncEnumerable<T> enumerable, int bufferSize) =>
            new AsyncProcessingEnumerable<T>(enumerable, bufferSize);
    }
}
