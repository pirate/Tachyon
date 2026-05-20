package dev.tachyon_ipc

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.yield
import java.lang.foreign.ValueLayout

/**
 * Immutable message received from a specific spoke of a [StarBus].
 */
data class StarMessage(
    val payload: ByteArray,
    val typeId: Int,
    val actualSize: Long,
    val spokeIdx: Int,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StarMessage) return false
        return typeId == other.typeId &&
                actualSize == other.actualSize &&
                spokeIdx == other.spokeIdx &&
                payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int {
        var result = payload.contentHashCode()
        result = 31 * result + typeId
        result = 31 * result + actualSize.hashCode()
        result = 31 * result + spokeIdx
        return result
    }
}

/**
 * Aggregates N independent SPSC arenas into a single round-robin polling loop bounded by a TSC-calibrated time budget.
 * One consumer per [StarBus]; one producer per spoke.
 *
 * Each spoke is a [Bus] created by the producer (Listener) side. The star holds Connector-side handles and ref-counts
 * them internally; the caller may close its own handles after construction returns.
 *
 * @apiNote All polling and TX operations must be driven from a single consumer thread.
 * [close] is safe from any thread.
 */
class StarBus private constructor(private val inner: TachyonStarBus) : AutoCloseable {

    companion object {
        /**
         * Creates a [StarBus] from the given connector-side [Bus] instances.
         * Each bus is ref-counted internally; the caller may close its own handles after this returns.
         *
         * @param buses   Connector-side [Bus] instances, one per spoke. Must not be empty.
         * @param nodeIds Optional NUMA node IDs; length must equal [buses].size if non-null.
         *                Negative values skip NUMA binding for the corresponding spoke.
         *                {@code null} disables NUMA binding entirely.
         */
        fun create(buses: Array<Bus>, nodeIds: IntArray? = null): StarBus =
            StarBus(TachyonStarBus.create(Array(buses.size) { buses[it].bus }, nodeIds))
    }

    /** The number of spokes. */
    val nSpokes: Int get() = inner.nSpokes()

    /**
     * Reads the internal atomic state of the C++ arena for spoke [spokeIdx].
     *
     * @return The integer mapping of {@code tachyon_state_t}.
     */
    fun getState(spokeIdx: Int): Int = inner.getState(spokeIdx)

    /**
     * Drains up to [maxTotal] messages across all spokes within [budgetUs] microseconds.
     * Returns an empty guard if the budget expires before any message arrives.
     *
     * The returned guard must be committed (or closed) before the next [poll] call.
     *
     * @param maxTotal  Upper bound on messages to drain in one call. Must be strictly positive.
     * @param budgetUs  TSC-bounded polling budget in microseconds.
     */
    fun poll(maxTotal: Int, budgetUs: Long): TachyonStarBus.StarPollGuard =
        inner.poll(maxTotal, budgetUs)

    /**
     * Converts the poll loop into an asynchronous Kotlin [Flow].
     * Payload bytes are copied out of SHM before the guard is committed, so each
     * [StarMessage] is fully owned by the caller.
     *
     * @param maxBatch  Maximum messages to drain per poll call.
     * @param budgetUs  TSC-bounded polling budget per call in microseconds.
     * @param onEmpty   Suspending callback invoked when a poll call returns no messages.
     *                  Defaults to [yield] to cooperatively release the coroutine.
     */
    fun receive(
        maxBatch: Int = 64,
        budgetUs: Long = 1_000L,
        onEmpty: suspend () -> Unit = { yield() },
    ): Flow<StarMessage> = flow {
        while (true) {
            inner.poll(maxBatch, budgetUs).use { guard ->
                if (guard.isEmpty) {
                    onEmpty()
                } else {
                    repeat(guard.count()) { i ->
                        val view = guard.get(i)
                        val bytes = view.payload().toArray(ValueLayout.JAVA_BYTE)
                        emit(StarMessage(bytes, view.typeId(), view.actualSize(), view.spokeIdx()))
                    }
                }
            }
        }
    }.flowOn(Dispatchers.IO).cancellable()

    /**
     * Requests an exclusive, zero-copy memory slot from the producer arena of spoke [spokeIdx].
     *
     * @return A [TachyonStarBus.StarTxGuard] for zero-copy writes, or {@code null} if the ring
     *         is full or [spokeIdx] is out of range.
     */
    fun acquireTx(spokeIdx: Int, maxSize: Long): TachyonStarBus.StarTxGuard? =
        inner.acquireTx(spokeIdx, maxSize)

    /**
     * Notifies sleeping consumers on spoke [spokeIdx] via a futex wake-up signal.
     * Not needed after [TachyonStarBus.StarTxGuard.commit], which flushes internally.
     */
    fun flush(spokeIdx: Int) {
        inner.flush(spokeIdx)
    }

    override fun close() {
        inner.close()
    }
}
