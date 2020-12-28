using BenchmarkDotNet.Attributes;
using System.Threading.Tasks;
using Iftm.AsyncQueue;
using BenchmarkDotNet.Running;
using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Channels;
using System.Diagnostics;

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
                left.SetResult(x, true);
                (await right.GetResultAsync().ConfigureAwait(false)).Expect(x);
            }
        }

        async Task TestRight() {
            for (int x = 0; x < iterations; ++x) {
                (await left.GetResultAsync().ConfigureAwait(false)).Expect(x);
                right.SetResult(x, true);
            }
        }

        var t1 = TestLeft();
        var t2 = TestRight();

        await t1;
        await t2;
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
        .ProcessAsynchronously(1024, 128, 128)
        .SimulateProcessing(_processingFrequency, _processingDelay)
        .ConsumeEnumerator();
}

[MemoryDiagnoser]
public class SlowConsumerAndProducerBenchmarks {
    private static void Compute(int cycles) {
        long sum = 0;
        for (int x = 0; x < cycles; ++x) {
            sum += x;
        }
        if (sum != (cycles - 1) * cycles / 2) Error();
    }

    #pragma warning disable 1998
    public static async IAsyncEnumerator<int> ProduceFast(long count) {
        int val = 0;
        for (long x = 0; x < count; ++x) {
            yield return val++;
        }
    }

    public static async IAsyncEnumerator<int> ProduceSlow(long count, int computationCycles) {
        int val = 0;
        for (long x = 0; x < count; ++x) {
            Compute(computationCycles);

            yield return val++;
        }
    }
    #pragma warning restore 1998

    public static void Error() => throw new Exception("Error.");

    public static async ValueTask ConsumeSlow(IAsyncEnumerator<int> enumerator, long expectedCount, int computationCycles) {
        int expected = 0;
        long count = 0;
        while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
            if (expected != enumerator.Current) Error();
            ++expected;
            ++count;

            Compute(computationCycles);
        }

        if (count != expectedCount) Error();
    }

    public static async ValueTask ConsumeFast(IAsyncEnumerator<int> enumerator, long expectedCount) {
        int expected = 0;
        long count = 0;
        while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
            if (expected != enumerator.Current) Error();
            ++expected;
            ++count;
        }

        if (count != expectedCount) Error();
    }

    [Benchmark]
    public ValueTask Direct_Fast_To_Fast() => ConsumeFast(ProduceFast(1_000_000), 1_000_000);

    [Benchmark]
    public ValueTask Amortized_Fast_To_Fast() => ConsumeFast(ProduceFast(1_000_000).ProcessAsynchronously(16 * 1024, 4 * 1024, 4 * 1024), 1_000_000);


    [Benchmark]
    public ValueTask Direct_Fast_To_Slow() => ConsumeSlow(ProduceFast(1_000_000), 1_000_000, 1000);

    [Benchmark]
    public ValueTask Direct_Slow_To_Fast() => ConsumeFast(ProduceSlow(1_000_000, 2000), 1_000_000);

    [Benchmark]
    public ValueTask Amortized_Fast_To_Slow() => ConsumeSlow(ProduceFast(1_000_000).ProcessAsynchronously(10 * 1024, 1024, 1024), 1_000_000, 1000);

    [Benchmark]
    public ValueTask Direct_Slow_To_Slow() => ConsumeSlow(ProduceSlow(1_000_000, 2000), 1_000_000, 1000);

    [Benchmark]
    public ValueTask Amortized_Slow_To_Slow() => ConsumeSlow(ProduceSlow(1_000_000, 2000).ProcessAsynchronously(8 * 1024, 2048, 2048), 1_000_000, 1000);

    [Benchmark]
    public ValueTask Amortized_Slow_To_Slow2() => ConsumeSlow(ProduceSlow(1_000_000, 2000).ProcessAsynchronously(8 * 1024), 1_000_000, 1000);
}

//public class QueueWorkItemTest : IThreadPoolWorkItem {
//    public long Count;
//    public static readonly object Lock = new object();

//    public void Execute() {
//        if (Interlocked.Increment(ref Count) >= 100_000_000) {
//            lock (Lock) {
//                Monitor.Pulse(Lock);
//            }
//        }
//        else {
//            ThreadPool.UnsafeQueueUserWorkItem(this, false);
//        }
//    }
//}

class Program {
    static void Main() => BenchmarkRunner.Run<SlowConsumerAndProducerBenchmarks>();

    //static async Task Main() {
    //    var item = new QueueWorkItemTest();
    //    lock (QueueWorkItemTest.Lock) {
    //        var sw = new Stopwatch();
    //        sw.Start();
    //        for (int x = 0; x < Environment.ProcessorCount * 2; ++x) {
    //            ThreadPool.UnsafeQueueUserWorkItem(item, false);
    //        }

    //        Monitor.Wait(QueueWorkItemTest.Lock);
    //        sw.Stop();

    //        Console.WriteLine(sw.ElapsedMilliseconds + " ms.");
    //    }

    //    //for (int x = 0; x < 20; ++x) {
    //    //    await new SlowConsumerAndProducerBenchmarks().Amortized_Fast_To_Fast().ConfigureAwait(false);
    //    //}
    //}
}
