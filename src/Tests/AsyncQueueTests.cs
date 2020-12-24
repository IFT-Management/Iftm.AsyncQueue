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
        var writer = queue.AsWriter;
        var reader = queue.AsReader;

        var writeBuffer = await writer.GetWriteBufferAsync();
        Assert.Equal(1, writeBuffer.Length);

        writeBuffer.Span[0] = 1;
        writer.Commit(1);

        writeBuffer = await writer.GetWriteBufferAsync();
        Assert.Equal(1, writeBuffer.Length);

        writeBuffer.Span[0] = 2;
        writer.Commit(1);

        var task = writer.GetWriteBufferAsync();
        Assert.False(task.IsCompleted);

        var readBuffer = await reader.GetReadBufferAsync();
        Assert.False(task.IsCompleted);

        Assert.Equal(1, readBuffer[0]);
        reader.MarkRead(1);

        writeBuffer = await task;
    }
}
