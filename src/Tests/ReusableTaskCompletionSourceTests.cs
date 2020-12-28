using System;
using System.Threading.Tasks;
using Xunit;
using Iftm.AsyncQueue;

public class ReusableTaskCompletionSourceTests {
    private static async Task<T> Await<T>(ValueTask<T> task) => await task.ConfigureAwait(false);

    [Fact]
    public async Task AllCombinations() {
        var tcs = new ReusableTaskCompletionSource<int>();

        // write then read

        tcs.SetResult(1, true);
        var r1 = await tcs.GetResultAsync().ConfigureAwait(false);
        Assert.Equal(1, r1);

        // read then write

        var r2 = tcs.GetResultAsync();
        Assert.False(r2.IsCompleted);

        tcs.SetResult(2, true);
        Assert.Equal(2, await r2.ConfigureAwait(false));

        // read and await then write 

        var r3 = Await(tcs.GetResultAsync());
        Assert.False(r3.IsCompleted);

        tcs.SetResult(3, true);
        Assert.Equal(3, await r3.ConfigureAwait(false));
    }

}
