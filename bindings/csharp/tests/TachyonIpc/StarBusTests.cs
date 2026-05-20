using TachyonIpc;
using TachyonIpc.Native;
using Xunit;

namespace TachyonIpc.Tests;

public sealed unsafe class StarBusTests : IDisposable
{
    private readonly string _socketPath;

    public StarBusTests()
    {
        _socketPath = Path.Combine(Path.GetTempPath(), $"tst_{Guid.NewGuid():N}.sock");
    }

    public void Dispose()
    {
        if (File.Exists(_socketPath)) File.Delete(_socketPath);
    }

    private static string TempSock() =>
        Path.Combine(Path.GetTempPath(), $"tst_{Guid.NewGuid():N}.sock");

    private static Thread StartListener(string path, nuint capacity, Action<Bus> work)
    {
        var t = new Thread(() =>
        {
            using var bus = Bus.Listen(path, capacity);
            work(bus);
        }) { IsBackground = true };
        t.Start();
        return t;
    }

    private static Bus ConnectWithRetry(string path, int maxAttempts = 200)
    {
        for (var i = 0; i < maxAttempts; i++)
        {
            try
            {
                return Bus.Connect(path);
            }
            catch (TachyonException e) when (e.Error == TachyonError.Network)
            {
                Thread.Sleep(10);
            }
        }

        throw new TimeoutException($"Could not connect to {path} after {maxAttempts} attempts.");
    }

    [Fact]
    public void NSpokes_ReflectsCreatedCount()
    {
        var path1 = TempSock();
        try
        {
            var t0 = StartListener(_socketPath, 1024 * 1024, _ => { });
            var t1 = StartListener(path1, 1024 * 1024, _ => { });
            Thread.Sleep(20);
            using var c0 = ConnectWithRetry(_socketPath);
            using var c1 = ConnectWithRetry(path1);
            t0.Join(2000);
            t1.Join(2000);

            using var star = StarBus.Create([c0, c1]);
            Assert.Equal(2, star.NSpokes);
        }
        finally
        {
            if (File.Exists(path1)) File.Delete(path1);
        }
    }

    [Fact]
    public void Poll_SingleSpoke_DrainsSingleMessage()
    {
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF];
        var typeId = TypeId.Make(0, 7);

        var t = StartListener(_socketPath, 1024 * 1024, bus =>
        {
            Assert.True(bus.TryAcquireTx((nuint)payload.Length, out var tx));
            using (tx)
            {
                payload.CopyTo(tx.Buffer);
                tx.Commit(payload.Length, typeId);
            }

            bus.Flush();
        });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        var views = stackalloc TachyonMsgView[8];
        var indices = stackalloc nuint[8];

        var count = star.Poll(views, indices, 8, 5_000);
        Assert.Equal(1u, count);
        Assert.Equal(typeId, views[0].TypeId);
        Assert.Equal((nuint)payload.Length, views[0].ActualSize);
        Assert.Equal(0u, (uint)indices[0]);
        var received = new Span<byte>(views[0].Ptr, (int)views[0].ActualSize).ToArray();
        Assert.Equal(payload, received);
        star.Commit();
    }

    [Fact]
    public void Poll_MultipleSpokes_DrainsFromBoth()
    {
        var path1 = TempSock();
        try
        {
            var typeId0 = TypeId.Make(0, 10);
            var typeId1 = TypeId.Make(0, 11);
            byte[] p0 = [0xAA, 0xBB];
            byte[] p1 = [0xCC, 0xDD];

            var t0 = StartListener(_socketPath, 1024 * 1024, bus =>
            {
                Assert.True(bus.TryAcquireTx(2, out var tx));
                using (tx)
                {
                    p0.CopyTo(tx.Buffer);
                    tx.Commit(2, typeId0);
                }

                bus.Flush();
            });
            var t1 = StartListener(path1, 1024 * 1024, bus =>
            {
                Assert.True(bus.TryAcquireTx(2, out var tx));
                using (tx)
                {
                    p1.CopyTo(tx.Buffer);
                    tx.Commit(2, typeId1);
                }

                bus.Flush();
            });
            Thread.Sleep(20);
            using var c0 = ConnectWithRetry(_socketPath);
            using var c1 = ConnectWithRetry(path1);
            t0.Join(2000);
            t1.Join(2000);

            using var star = StarBus.Create([c0, c1]);
            var views = stackalloc TachyonMsgView[16];
            var indices = stackalloc nuint[16];

            var count = star.Poll(views, indices, 16, 5_000);
            Assert.Equal(2u, count);

            uint gotType0 = 0, gotType1 = 0;
            byte[]? gotData0 = null, gotData1 = null;
            for (var i = 0; i < (int)count; i++)
            {
                var data = new Span<byte>(views[i].Ptr, (int)views[i].ActualSize).ToArray();
                if (indices[i] == 0)
                {
                    gotType0 = views[i].TypeId;
                    gotData0 = data;
                }
                else
                {
                    gotType1 = views[i].TypeId;
                    gotData1 = data;
                }
            }

            Assert.Equal(typeId0, gotType0);
            Assert.Equal(p0, gotData0);
            Assert.Equal(typeId1, gotType1);
            Assert.Equal(p1, gotData1);
            star.Commit();
        }
        finally
        {
            if (File.Exists(path1)) File.Delete(path1);
        }
    }

    [Fact]
    public void Poll_ReturnsZero_WhenBudgetExpires()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        var views = stackalloc TachyonMsgView[8];
        var indices = stackalloc nuint[8];

        var count = star.Poll(views, indices, 8, 200 /* 200 µs */);
        Assert.Equal(0u, count);
    }

    [Fact]
    public void Commit_ReleasesSlots_SecondPollIsEmpty()
    {
        var t = StartListener(_socketPath, 1024 * 1024, bus =>
        {
            Assert.True(bus.TryAcquireTx(4, out var tx));
            using (tx) tx.Commit(4, 0, 1);
            bus.Flush();
        });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        var views = stackalloc TachyonMsgView[8];
        var indices = stackalloc nuint[8];

        var first = star.Poll(views, indices, 8, 5_000);
        Assert.Equal(1u, first);
        star.Commit();

        var second = star.Poll(views, indices, 8, 200);
        Assert.Equal(0u, second);
    }

    [Fact]
    public void Commit_IsIdempotent()
    {
        var t = StartListener(_socketPath, 1024 * 1024, bus =>
        {
            Assert.True(bus.TryAcquireTx(4, out var tx));
            using (tx) tx.Commit(4, 0, 1);
            bus.Flush();
        });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        var views = stackalloc TachyonMsgView[8];
        var indices = stackalloc nuint[8];

        star.Poll(views, indices, 8, 5_000);
        star.Commit();
        star.Commit(); // must not throw
    }

    [Fact]
    public void TryAcquireTx_Rollback_OnDispose_WhenNotCommitted()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);

        Assert.True(star.TryAcquireTx(0, 64, out var tx));
        Assert.Equal(64, tx.Data.Length);
        tx.Dispose();
    }

    [Fact]
    public void TryAcquireTx_ReturnsFalse_WhenOobSpokeIdx()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        Assert.False(star.TryAcquireTx(999, 64, out _));
    }

    [Fact]
    public void StarTxGuard_Commit_ThrowsWhenAlreadyConsumed()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        Assert.True(star.TryAcquireTx(0, 64, out var tx));
        tx.Commit(4, TypeId.Make(0, 1));
        Assert.Throws<InvalidOperationException>(() => tx.Commit(4, TypeId.Make(0, 1)));
    }

    [Fact]
    public void Flush_DoesNotThrow()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        star.Flush(0); // must not throw
    }

    [Fact]
    public void GetState_IsNotFatalError_AfterCreate()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        Assert.NotEqual(TachyonState.FatalError, star.GetState(0));
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        var star = StarBus.Create([connector]);
        star.Dispose();
        star.Dispose(); // must not throw
    }

    [Fact]
    public void ThrowsAfterDispose()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        var star = StarBus.Create([connector]);
        star.Dispose();
        Assert.Throws<ObjectDisposedException>(() => star.NSpokes);
    }

    [Fact]
    public void Create_Throws_WhenBusesEmpty()
    {
        Assert.Throws<ArgumentException>(() => StarBus.Create([]));
    }

    [Fact]
    public void Create_Throws_WhenNodeIdsMismatch()
    {
        var t = StartListener(_socketPath, 1024 * 1024, _ => { });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        Assert.Throws<ArgumentException>(() => StarBus.Create([connector], [0, 1]));
    }

    [Fact]
    public void Poll_MultiMessage_SingleSpoke_InOrder()
    {
        const int N = 8;

        var t = StartListener(_socketPath, 1024 * 1024, bus =>
        {
            for (ushort i = 0; i < N; i++)
            {
                Assert.True(bus.TryAcquireTx(4, out var tx));
                using (tx) tx.Commit(4, 0, i);
            }

            bus.Flush();
        });
        Thread.Sleep(20);
        using var connector = ConnectWithRetry(_socketPath);
        t.Join(2000);

        using var star = StarBus.Create([connector]);
        var views = stackalloc TachyonMsgView[N];
        var indices = stackalloc nuint[N];

        nuint count = 0;
        for (var attempt = 0; attempt < 1_000_000 && count < N; attempt++)
            count = star.Poll(views, indices, N, 5_000);

        Assert.Equal((nuint)N, count);
        for (var i = 0; i < N; i++)
        {
            Assert.Equal(0u, (uint)indices[i]);
            Assert.Equal((ushort)i, TypeId.MsgType(views[i].TypeId));
        }

        star.Commit();
    }
}