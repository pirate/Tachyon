using System.Runtime.CompilerServices;
using TachyonIpc.Native;

namespace TachyonIpc;

/// <summary>
/// Aggregates N independent SPSC arenas into a single round-robin polling loop bounded by a TSC-calibrated time budget.
/// One consumer per <see cref="StarBus"/>; one producer per spoke.
/// </summary>
/// 
/// <remarks>
/// Each spoke is a <see cref="Bus"/> created by the producer (Listener) side. The star holds Connector-side handles and
/// ref-counts them internally; the caller may dispose its own handles after <see cref="Create"/> returns.
/// Not thread-safe: all polling and TX operations must be driven from a single consumer thread.
/// <see cref="Dispose"/> is safe from any thread.
/// </remarks>
public sealed unsafe class StarBus : IDisposable
{
    private nint _star;

    static StarBus() => NativeLoader.Register();

    private StarBus(nint star) => _star = star;

    /// <summary>
    /// Creates a <see cref="StarBus"/> from the given connector-side <see cref="Bus"/> instances.
    /// Each bus is ref-counted internally; the caller may dispose its own handles after this returns.
    /// </summary>
    /// <param name="buses">Connector-side buses, one per spoke. Must not be empty.</param>
    /// <param name="nodeIds">
    /// Optional NUMA node IDs; length must equal <paramref name="buses"/>.Length if non-null.
    /// Negative values skip NUMA binding for that spoke. <c>null</c> disables NUMA binding entirely.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="buses"/> is empty, or <paramref name="nodeIds"/> length mismatches.
    /// </exception>
    /// <exception cref="TachyonException">Native SHM or TSC calibration failure.</exception>
    public static StarBus Create(Bus[] buses, int[]? nodeIds = null)
    {
        if (buses is null || buses.Length == 0)
            throw new ArgumentException("buses must not be empty", nameof(buses));
        if (nodeIds is not null && nodeIds.Length != buses.Length)
            throw new ArgumentException("nodeIds length must equal buses.Length", nameof(nodeIds));

        nint* handles = stackalloc nint[buses.Length];
        for (int i = 0; i < buses.Length; i++)
        {
            handles[i] = buses[i].Handle;
        }

        nint star;
        fixed (int* nodeIdsPtr = nodeIds)
        {
            TachyonException.ThrowIfError(
                TachyonNative.tachyon_star_create(handles, (nuint)buses.Length, nodeIdsPtr, &star),
                nameof(TachyonNative.tachyon_star_create));
        }

        return new StarBus(star);
    }

    /// <summary>The number of spokes.</summary>
    public int NSpokes
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ThrowIfDisposed();
            return (int)TachyonNative.tachyon_star_n_spokes(_star);
        }
    }

    /// <summary>
    /// Reads the internal atomic state of the arena for spoke <paramref name="spokeIdx"/>.
    /// Returns <see cref="TachyonState.Unknown"/> if <paramref name="spokeIdx"/> is out of range.
    /// </summary>
    /// <param name="spokeIdx">Zero-based spoke index.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TachyonState GetState(int spokeIdx)
    {
        ThrowIfDisposed();
        return TachyonNative.tachyon_star_get_state(_star, (nuint)spokeIdx);
    }

    /// <summary>
    /// Drains up to <paramref name="maxTotal"/> messages across all spokes within
    /// <paramref name="budgetUs"/> microseconds. Returns the number of messages written
    /// into <paramref name="views"/> and <paramref name="spokeIndices"/>.
    /// Returns zero if the budget expires with no data.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="views"/> and <paramref name="spokeIndices"/> must each hold at least
    /// <paramref name="maxTotal"/> elements; use <c>stackalloc</c> for hot-path allocation.
    /// </para>
    /// <para>
    /// Call <see cref="Commit"/> after processing all views to release the ring-buffer slots.
    /// Internal <c>pending_</c> state accumulates; a single commit releases all pending slots.
    /// </para>
    /// </remarks>
    /// <param name="views">Caller-allocated view buffer. Typically <c>stackalloc TachyonMsgView[N]</c>.</param>
    /// <param name="spokeIndices">Caller-allocated buffer receiving the spoke index per message.</param>
    /// <param name="maxTotal">Upper bound on messages to drain. Must be positive.</param>
    /// <param name="budgetUs">TSC-bounded polling budget in microseconds.</param>
    /// <returns>Number of messages drained.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public nuint Poll(TachyonMsgView* views, nuint* spokeIndices, int maxTotal, ulong budgetUs)
    {
        ThrowIfDisposed();
        return TachyonNative.tachyon_star_poll(_star, views, (nuint)maxTotal, budgetUs, spokeIndices);
    }

    /// <summary>
    /// Advances the consumer tail for all polled spokes, releasing their ring-buffer slots.
    /// All <see cref="TachyonMsgView.Ptr"/> pointers obtained from the last <see cref="Poll"/>
    /// become invalid. Safe to call when the last poll returned zero.
    /// </summary>
    /// <exception cref="TachyonException">Native fatal-error state.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Commit()
    {
        ThrowIfDisposed();
        TachyonException.ThrowIfError(
            TachyonNative.tachyon_star_commit(_star),
            nameof(TachyonNative.tachyon_star_commit));
    }

    /// <summary>
    /// Non-blocking TX slot acquisition on spoke <paramref name="spokeIdx"/>.
    /// Returns <c>false</c> if the ring is full or <paramref name="spokeIdx"/> is out of range.
    /// </summary>
    /// <param name="spokeIdx">Zero-based spoke index.</param>
    /// <param name="maxSize">Required contiguous byte capacity.</param>
    /// <param name="guard">Write-side guard; valid only if the method returns <c>true</c>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquireTx(int spokeIdx, nuint maxSize, out StarTxGuard guard)
    {
        ThrowIfDisposed();
        var ptr = TachyonNative.tachyon_star_acquire_tx(_star, (nuint)spokeIdx, maxSize);
        if (ptr == null)
        {
            guard = default;
            return false;
        }

        guard = new StarTxGuard(_star, ptr, maxSize, spokeIdx);
        return true;
    }

    /// <summary>
    /// Notifies sleeping consumers on spoke <paramref name="spokeIdx"/> via a futex wake-up signal.
    /// Not needed after <see cref="StarTxGuard.Commit"/>, which flushes internally.
    /// </summary>
    /// <param name="spokeIdx">Zero-based spoke index.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Flush(int spokeIdx)
    {
        ThrowIfDisposed();
        TachyonNative.tachyon_star_flush(_star, (nuint)spokeIdx);
    }

    /// <summary>Destroys the native star bus, releasing all internal bus references. Idempotent.</summary>
    public void Dispose()
    {
        var star = Interlocked.Exchange(ref _star, nint.Zero);
        if (star != nint.Zero)
            TachyonNative.tachyon_star_destroy(star);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed()
    {
        if (_star == nint.Zero)
            throw new ObjectDisposedException(nameof(StarBus));
    }
}

/// <summary>
/// Manages an exclusive, zero-copy TX slot in the producer arena of a specific spoke.
/// Write the payload directly into <see cref="Data"/>, then call <see cref="Commit"/> or
/// <see cref="Rollback"/>. <see cref="Dispose"/> rolls back automatically if not yet committed.
/// </summary>
/// <remarks>
/// <see cref="Commit"/> calls <c>tachyon_star_commit_tx</c>, which flushes the spoke arena
/// internally; no separate <see cref="StarBus.Flush"/> call is required.
/// </remarks>
public unsafe struct StarTxGuard : IDisposable
{
    private readonly nint _star;
    private readonly void* _ptr;
    private readonly nuint _maxSize;
    private readonly int _spokeIdx;
    private bool _consumed;

    internal StarTxGuard(nint star, void* ptr, nuint maxSize, int spokeIdx)
    {
        _star = star;
        _ptr = ptr;
        _maxSize = maxSize;
        _spokeIdx = spokeIdx;
        _consumed = false;
    }

    /// <summary>
    /// Writable span pointing directly into shared memory.
    /// Invalid after <see cref="Commit"/> or <see cref="Rollback"/>.
    /// </summary>
    public readonly Span<byte> Data => new(_ptr, (int)_maxSize);

    /// <summary>
    /// Publishes <paramref name="actualSize"/> bytes with <paramref name="typeId"/> and
    /// flushes the spoke arena.
    /// </summary>
    /// <param name="actualSize">Number of bytes written. Must not exceed the reservation.</param>
    /// <param name="typeId">User-defined protocol identifier.</param>
    /// <exception cref="InvalidOperationException">Guard already consumed.</exception>
    /// <exception cref="TachyonException">Native error (size exceeded or no slot acquired).</exception>
    public void Commit(nuint actualSize, uint typeId)
    {
        if (_consumed) throw new InvalidOperationException("StarTxGuard already consumed");
        _consumed = true;
        TachyonException.ThrowIfError(
            TachyonNative.tachyon_star_commit_tx(_star, (nuint)_spokeIdx, actualSize, typeId),
            nameof(TachyonNative.tachyon_star_commit_tx));
    }

    /// <summary>Aborts the TX slot without publishing. No-op if already consumed.</summary>
    public void Rollback()
    {
        if (_consumed) return;
        _consumed = true;
        TachyonNative.tachyon_star_rollback_tx(_star, (nuint)_spokeIdx);
    }

    /// <summary>Calls <see cref="Rollback"/> if not yet consumed.</summary>
    public void Dispose() => Rollback();
}