using BenchmarkDotNet.Attributes;
using System.Threading.Tasks;
using Iftm.AsyncQueue;
using BenchmarkDotNet.Running;
using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Channels;

[MemoryDiagnoser]
[WarmupCount(20)]
public class ReusableTaskCompletionSourceBenchmarks {
    [Benchmark]
    public async Task PingPong() {
        const int iterations = 1_000_000;

        var left = new ReusableTaskCompletionSource<int>();
        var right = new ReusableTaskCompletionSource<int>();

        async Task TestLeft() {
            for (int x = 0; x < iterations; ++x) {
                left.SetResult(x);
                (await right.GetResultAsync().ConfigureAwait(false)).Expect(x);
            }
        }

        async Task TestRight() {
            for (int x = 0; x < iterations; ++x) {
                (await left.GetResultAsync().ConfigureAwait(false)).Expect(x);
                right.SetResult(x);
            }
        }

        var t1 = TestLeft();
        var t2 = TestRight();

        await t1;
        await t2;
    }
}

[MemoryDiagnoser]
[WarmupCount(20)]
public class AsyncQueueBenchmarks {
    [Benchmark]
    public async Task ReadThenWrite() {
        var pipeline = new AsyncQueue<int>(4);

        for (int i = 0; i < 10_000_000; i++) {
            ValueTask<bool> moveNextTask = pipeline.MoveNextAsync();
            (await pipeline.WriteAsync(i).ConfigureAwait(false)).Expect(true);
            (await moveNextTask.ConfigureAwait(false)).Expect(true);
            pipeline.Current.Expect(i);
        }
    }

    [Benchmark]
    public async Task WriteThenRead() {
        var pipeline = new AsyncQueue<int>(4);

        for (int i = 0; i < 10_000_000; i++) {
            (await pipeline.WriteAsync(i).ConfigureAwait(false)).Expect(true);
            (await pipeline.MoveNextAsync().ConfigureAwait(false)).Expect(true);
            pipeline.Current.Expect(i);
        }
    }

    [Benchmark]
    public async Task AsyncReaderAndWriter() {
        const int pipelineSize = 1024;
        const int iterations = 10_000_000;

        var queue = new AsyncQueue<int>(pipelineSize);

        async Task WriteToPipeline() {
            for (int x = 1; x <= iterations; ++x) {
                (await queue.WriteAsync(x).ConfigureAwait(false)).Expect(true);
            }
            queue.Complete();
        }

        var writeTask = WriteToPipeline();

        int lastRead = 0;
        while (await queue.MoveNextAsync().ConfigureAwait(false)) {
            queue.Current.Expect(lastRead + 1);
            ++lastRead;
        }
        await queue.DisposeAsync().ConfigureAwait(false);
        lastRead.Expect(iterations);

        await writeTask.ConfigureAwait(false);
    }
}

static class SimulatedOperations {
    public static async IAsyncEnumerator<int> SimulateProcessing(this IAsyncEnumerator<int> enumerator, int procesingFrequency, TimeSpan processingLength) {
        await using var en = enumerator.ConfigureAwait(false);

        int count = 0;
        while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
            if (++count == procesingFrequency) {
                count = 0;
                Thread.Sleep(processingLength);
            }
            yield return enumerator.Current + 1;
        }
    }

    public static async ValueTask ConsumeEnumerator<T>(this IAsyncEnumerator<T> enumerator) {
        await using var en = enumerator.ConfigureAwait(false);
        while (await enumerator.MoveNextAsync().ConfigureAwait(false)) ;
    }

    private static IAsyncEnumerator<T> ProcessAsynchronouslyUsingChannel<T>(this IAsyncEnumerator<T> enumerator, Channel<T> channel) {
        var writer = channel.Writer;

        async void WriteToChannel() {
            try {
                while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
                    await writer.WriteAsync(enumerator.Current).ConfigureAwait(false);
                }
                writer.Complete();
            }
            catch (Exception e) {
                writer.Complete(e);
            }
            finally {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
        }

        WriteToChannel();

        return channel.Reader.ReadAllAsync().GetAsyncEnumerator();
    }

    public static IAsyncEnumerator<T> ProcessAsynchronouslyUsingBoundedChannel<T>(this IAsyncEnumerator<T> enumerator, int bufferSize) =>
        enumerator.ProcessAsynchronouslyUsingChannel(Channel.CreateBounded<T>(new BoundedChannelOptions(bufferSize) { SingleReader = true, SingleWriter = true }));

    public static IAsyncEnumerator<T> ProcessAsynchronouslyUsingUnboundedChannel<T>(this IAsyncEnumerator<T> enumerator) =>
        enumerator.ProcessAsynchronouslyUsingChannel(Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true }));
}

[MemoryDiagnoser]
public class BoundedChannelComparison {
    private static async IAsyncEnumerator<int> SimulateSocketReading(int cycles, int cycleSize, TimeSpan cycleSleep) {
        int val = 0;
        for (int x = 0; x < cycles; ++x) {
            for (int y = 0; y < cycleSize; ++y) {
                yield return val++;
            }

            await Task.Delay(cycleSleep).ConfigureAwait(false);
        }
    }

    #pragma warning disable 1998
    private static async IAsyncEnumerator<int> SimulateFastReading(int cycles, int cycleSize) {
        int val = 0;
        for (int x = 0; x < cycles; ++x) {
            for (int y = 0; y < cycleSize; ++y) {
                yield return val++;
            }
        }
    }
    #pragma warning restore 1998

    private const int _cycles = 100, _iterationCount = 1000;
    private static readonly TimeSpan _socketReadingDelay = TimeSpan.FromMilliseconds(1);

    private static readonly TimeSpan _processingDelay = TimeSpan.FromMilliseconds(1);
    private const int _processingFrequency = 1500;

    [Benchmark]
    public ValueTask SynchronousReading() =>
        SimulateFastReading(_cycles, _iterationCount)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SocketReading() =>
        SimulateSocketReading(_cycles, _iterationCount, _socketReadingDelay)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SynchronousReadingAndProcessing() =>
        SimulateFastReading(_cycles, _iterationCount)
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SocketReadingAndProcessing() =>
        SimulateSocketReading(_cycles, _iterationCount, _socketReadingDelay)
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SocketReadingAndAsyncBoundedChannelProcessing() =>
        SimulateSocketReading(_cycles, _iterationCount, _socketReadingDelay)
        .ProcessAsynchronouslyUsingBoundedChannel(1024)
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SocketReadingAndAsyncUnboundedChannelProcessing() =>
        SimulateSocketReading(_cycles, _iterationCount, _socketReadingDelay)
        .ProcessAsynchronouslyUsingUnboundedChannel()
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();

    [Benchmark]
    public ValueTask SocketReadingAndAsyncProcessing() =>
        SimulateSocketReading(_cycles, _iterationCount, _socketReadingDelay)
        .ProcessAsynchronously(1024)
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();
}


class Program {
    static void Main() => BenchmarkRunner.Run<BoundedChannelComparison>();
}
