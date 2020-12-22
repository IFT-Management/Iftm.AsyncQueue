using Iftm.AsyncQueue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

public class TestAsyncQueue  {

    [Fact]
    public async Task NormalFlow() {
        // Blocking writes to a full pipeline

        var queue = new AsyncQueue<int>(4);

        // Fill the queue.
        for (int x = 1; x <= 4; ++x) {
            Assert.True(await queue.WriteAsync(x));
        }

        // Attempt to write from the queue. Verify the returned tasks
        // are pending. Read from queue, verify the tasks have completed.
        for (int x = 1; x <= 10; ++x) {
            var writeTask = queue.WriteAsync(x + 4);
            Assert.False(writeTask.IsCompleted);

            Assert.True(await queue.MoveNextAsync());
            Assert.Equal(x, queue.Current);

            var writeResult = await writeTask;
            Assert.True(writeResult);
        }

        // Drain the queue.
        for (int x = 11; x <= 14; ++x) {
            Assert.True(await queue.MoveNextAsync());
            Assert.Equal(x, queue.Current);
        }

        // Blocking reads from an empty queue

        for (int x = 1; x <= 10; ++x) {
            var readTask = queue.MoveNextAsync();
            Assert.False(readTask.IsCompleted);

            Assert.True(await queue.WriteAsync(x));

            Assert.True(await readTask);
            Assert.Equal(x, queue.Current);
        }

        // Half-full queue

        Assert.True(await queue.WriteAsync(1));
        Assert.True(await queue.WriteAsync(2));

        for (int x = 3; x <= 10; ++x) {
            Assert.True(await queue.MoveNextAsync());
            Assert.Equal(x - 2, queue.Current);
            Assert.True(await queue.WriteAsync(x));
        }

        Assert.True(await queue.MoveNextAsync());
        Assert.Equal(9, queue.Current);
        Assert.True(await queue.MoveNextAsync());
        Assert.Equal(10, queue.Current);
    }

    [Fact]
    public async Task DisposeFlowNoWriteAwaiters() {
        var queue = new AsyncQueue<int>(4);
        await queue.DisposeAsync();
        Assert.False(await queue.WriteAsync(4));
    }

    [Fact]
    public async Task DisposeFlowWithWriteAwaiters() {
        var queue = new AsyncQueue<int>(1);
        Assert.True(await queue.WriteAsync(1));
        var task = queue.WriteAsync(2);
        Assert.False(task.IsCompleted);
        await queue.DisposeAsync();
        Assert.False(await task);
    }

    [Fact]
    public async Task Completion() {
        var pipeline = new AsyncQueue<int>(4);
        
        Assert.True(await pipeline.WriteAsync(1));
        pipeline.Complete();

        Assert.True(await pipeline.MoveNextAsync());
        Assert.Equal(1, pipeline.Current);

        Assert.False(await pipeline.MoveNextAsync());
    }

    [Fact]
    public async Task CompleteWithWaitingReaders() {
        var pipeline = new AsyncQueue<int>(4);

        var task = pipeline.MoveNextAsync();
        Assert.False(task.IsCompleted);

        pipeline.Complete();

        Assert.False(await task);
    }

    [Fact]
    public async Task CompleteWithException() {
        var pipeline = new AsyncQueue<int>(4);

        var readTask = pipeline.MoveNextAsync();
        Assert.False(readTask.IsCompleted);

        pipeline.Complete(new ArgumentException("test"));

        try {
            await readTask;
            Assert.False(true);
        }
        catch (ArgumentException e) {
            Assert.Equal("test", e.Message);
        }
    }

    #pragma warning disable 1998
    private static async IAsyncEnumerator<int> SimulateFastProducer(long count) {
        int val = 0;
        for (long x = 0; x < count; ++x) {
            yield return val++;
        }
    }
    #pragma warning restore 1998

    private static void Error() => throw new Exception("Error.");

    private static async ValueTask ConsumeAsyncEnumerator(IAsyncEnumerator<int> enumerator, long expectedCount) {
        int expected = 0;
        long count = 0;
        while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
            if (expected != enumerator.Current) Error();
            ++expected;
            ++count;
        }

        if (count != expectedCount) Error();
    }

    [Fact]
    public async Task QuickTest() => await ConsumeAsyncEnumerator(SimulateFastProducer(1_000_000), 1_000_000);
}
