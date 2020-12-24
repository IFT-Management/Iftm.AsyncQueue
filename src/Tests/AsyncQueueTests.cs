using Iftm.AsyncQueue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

public class AsyncQueueTests {
    [Fact]
    public async Task PlainWrite() {
        var queue = new AsyncQueue<int>(2, 1, 1);
        var writeBuffer = await queue.GetWriteBufferAsync();
        Assert.Equal(1, writeBuffer.Length);

        writeBuffer.Span[0] = 1;
        queue.Commit(1);

        writeBuffer = await queue.GetWriteBufferAsync();
        Assert.Equal(1, writeBuffer.Length);

        writeBuffer.Span[0] = 2;
        queue.Commit(1);

        var task = queue.GetWriteBufferAsync();
        Assert.False(task.IsCompleted);

        var readBuffer = await queue.GetReadBufferAsync();
        Assert.False(task.IsCompleted);

        Assert.Equal(1, readBuffer.Span[0]);
        queue.MarkRead(1);

        writeBuffer = await task;
    }
}
