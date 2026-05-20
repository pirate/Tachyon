package dev.tachyon_ipc;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aggregates N independent SPSC arenas into a single round-robin polling loop bounded by a TSC-calibrated time budget.
 * One consumer per {@code TachyonStarBus}, one producer per spoke.
 *
 * <p>Each spoke is a {@link TachyonBus} created by the producer (Listener) side. The star holds connector-side handles
 * and ref-counts them internally; the caller may close its handles after construction returns.
 *
 * @apiNote All polling and TX operations must be driven from a single consumer thread.
 * {@link #close()} is safe from any thread.
 * @implSpec The native star pointer is bound to a shared FFM {@link Arena} that dictates the spatial bounds of payload
 * segments returned by {@link #poll}.
 */
public final class TachyonStarBus implements AutoCloseable {
	private static final long VIEW_STRIDE = 32L;
	private static final long VIEW_OFF_PTR = 0L;
	private static final long VIEW_OFF_SIZE = 8L;
	private static final long VIEW_OFF_TYPE = 24L;

	private final MemorySegment starHandle;
	private final AtomicBoolean closed;
	private final Arena starArena;

	private TachyonStarBus(MemorySegment starHandle) {
		this.starHandle = starHandle;
		this.closed = new AtomicBoolean(false);
		this.starArena = Arena.ofShared();
	}

	/**
	 * Creates a {@code TachyonStarBus} from the given connector-side buses.
	 * Each bus is ref-counted internally; the caller may close its own handles after this returns.
	 *
	 * @param buses   Connector-side {@link TachyonBus} instances, one per spoke. Must not be empty.
	 * @param nodeIds Optional NUMA node IDs; length must equal {@code buses.length} if non-null.
	 *                Negative values skip NUMA binding for the corresponding spoke.
	 *                {@code null} disables NUMA binding entirely.
	 * @return A new active {@code TachyonStarBus} instance.
	 * @throws IllegalArgumentException If {@code buses} is empty or {@code nodeIds} length mismatches.
	 * @throws NullPointerException     If any element of {@code buses} is {@code null}.
	 */
	public static TachyonStarBus create(TachyonBus[] buses, int[] nodeIds) {
		if (buses == null || buses.length == 0) {
			throw new IllegalArgumentException("buses must not be empty");
		}

		if (nodeIds != null && nodeIds.length != buses.length) {
			throw new IllegalArgumentException("nodeIds length must equal buses.length");
		}

		MemorySegment[] handles = new MemorySegment[buses.length];
		for (int i = 0; i < buses.length; i++) {
			if (buses[i] == null) {
				throw new NullPointerException(STR."buses[\{i}] is null");
			}

			handles[i] = buses[i].handle();
		}

		return new TachyonStarBus(TachyonABI.starCreate(handles, nodeIds));
	}

	/**
	 * Returns the number of spokes.
	 *
	 * @return The spoke count.
	 */
	public int nSpokes() {
		checkOpen();
		return (int) TachyonABI.starNSpokes(starHandle);
	}

	/**
	 * Reads the internal atomic state of the C++ arena for spoke {@code spokeIdx}.
	 *
	 * @param spokeIdx The zero-based spoke index.
	 * @return The integer mapping of the {@code tachyon_state_t} enumeration, or {@code TACHYON_STATE_UNKNOWN} (0)
	 * if {@code spokeIdx} is out of range.
	 */
	public int getState(int spokeIdx) {
		checkOpen();
		return TachyonABI.starGetState(starHandle, spokeIdx);
	}

	/**
	 * Drains up to {@code maxTotal} messages across all spokes within {@code budgetUs} microseconds.
	 * Returns an empty guard if the budget expires before any message arrives.
	 *
	 * <p>The returned guard must be committed (or closed) before the next call to {@code poll}.
	 * Internal {@code pending_} state accumulates across calls; a single commit releases all pending slots.
	 *
	 * @param maxTotal Upper bound on messages to drain in one call. Must be strictly positive.
	 * @param budgetUs TSC-bounded polling budget in microseconds.
	 * @return A {@link StarPollGuard} containing zero or more zero-copy message views.
	 * @throws IllegalArgumentException If {@code maxTotal} is zero or negative.
	 * @implSpec Allocates a confined FFM arena for the native view and index buffers; payload pointers are
	 * reinterpreted into the shared {@code starArena} before the confined arena is released.
	 */
	public StarPollGuard poll(int maxTotal, long budgetUs) {
		checkOpen();
		if (maxTotal <= 0) {
			throw new IllegalArgumentException("maxTotal must be strictly positive");
		}

		try (Arena arena = Arena.ofConfined()) {
			MemorySegment viewsSeg = arena.allocate(VIEW_STRIDE * maxTotal, VIEW_STRIDE);
			MemorySegment idxSeg = arena.allocate(ValueLayout.JAVA_LONG.byteSize() * maxTotal);
			long count = TachyonABI.starPoll(starHandle, viewsSeg, maxTotal, budgetUs, idxSeg);

			StarMsgView[] views = new StarMsgView[(int) count];
			for (int i = 0; i < (int) count; i++) {
				long base = i * VIEW_STRIDE;
				MemorySegment rawPtr = viewsSeg.get(ValueLayout.ADDRESS, base + VIEW_OFF_PTR);
				long actualSize = viewsSeg.get(ValueLayout.JAVA_LONG, base + VIEW_OFF_SIZE);
				int typeId = viewsSeg.get(ValueLayout.JAVA_INT, base + VIEW_OFF_TYPE);
				int spokeIdx = (int) idxSeg.get(ValueLayout.JAVA_LONG, (long) i * 8);
				MemorySegment payload = actualSize > 0 ? rawPtr.reinterpret(actualSize, starArena, null) : MemorySegment.NULL;
				views[i] = new StarMsgView(payload, typeId, actualSize, spokeIdx);
			}

			return new StarPollGuard(starHandle, views);
		}
	}

	/**
	 * Requests an exclusive, zero-copy memory slot from the producer arena of spoke {@code spokeIdx}.
	 *
	 * @param spokeIdx The zero-based spoke index.
	 * @param maxSize  The required contiguous byte capacity.
	 * @return A {@link StarTxGuard} managing the allocated slot, or {@code null} if the ring is full or {@code spokeIdx}
	 * is out of range.
	 * @implSpec The returned guard's slot is reinterpreted into the shared {@code starArena}.
	 */
	public StarTxGuard acquireTx(int spokeIdx, long maxSize) {
		checkOpen();

		MemorySegment ptr = TachyonABI.starAcquireTx(starHandle, spokeIdx, maxSize);
		if (ptr.address() == 0L) {
			return null;
		}

		return new StarTxGuard(starHandle, ptr.reinterpret(maxSize, starArena, null), spokeIdx);
	}

	/**
	 * Notifies sleeping consumers on spoke {@code spokeIdx} via a futex wake-up signal.
	 * Not needed after {@link StarTxGuard#commit}, which flushes internally.
	 *
	 * @param spokeIdx The zero-based spoke index.
	 */
	public void flush(int spokeIdx) {
		checkOpen();
		TachyonABI.starFlush(starHandle, spokeIdx);
	}

	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			try {
				TachyonABI.starDestroy(starHandle);
			} finally {
				starArena.close();
			}
		}
	}

	private void checkOpen() {
		if (closed.get()) {
			throw new IllegalArgumentException("TachyonStarBus is closed.");
		}
	}

	/**
	 * Zero-copy view into one message inside a {@link StarPollGuard}.
	 * {@link #payload()} is valid only until the parent guard is committed.
	 */
	public record StarMsgView(MemorySegment payload, int typeId, long actualSize, int spokeIdx) {
	}

	/**
	 * Holds a zero-copy batch drained from a {@link TachyonStarBus#poll} call.
	 * All {@link StarMsgView#payload()} segments are valid until {@link #commit()} is called.
	 *
	 * @apiNote Must be committed or closed before the next {@link TachyonStarBus#poll} call.
	 * @implSpec {@link #commit()} delegates to {@code tachyon_star_commit}, which uses the internal {@code pending_}
	 * state of the C++ Core {@code StarBus}; the view's array is not passed back to native code.
	 */
	public static final class StarPollGuard implements AutoCloseable {
		private final MemorySegment starHandle;
		private final StarMsgView[] views;
		private boolean committed;

		StarPollGuard(MemorySegment starHandle, StarMsgView[] views) {
			this.starHandle = starHandle;
			this.views = views;
			this.committed = false;
		}

		/**
		 * Number of messages in this guard.
		 */
		public int count() {
			return views.length;
		}

		/**
		 * {@code true} if no messages were drained.
		 */
		public boolean isEmpty() {
			return views.length == 0;
		}

		/**
		 * Returns the message at {@code index}.
		 *
		 * @throws IndexOutOfBoundsException If {@code index} is out of range.
		 */
		public StarMsgView get(int index) {
			if (index < 0 || index >= views.length) {
				throw new IndexOutOfBoundsException(index);
			}

			return views[index];
		}

		/**
		 * Advances the consumer tail for all polled spokes, releasing their ring buffer slots.
		 * All {@link StarMsgView#payload()} segments become invalid. Safe to call multiple times.
		 *
		 * @throws TachyonException If the star is in fatal error state.
		 */
		public void commit() {
			if (committed) {
				return;
			}

			committed = true;
			if (views.length > 0) {
				TachyonABI.starCommit(starHandle);
			}
		}

		/**
		 * Calls {@link #commit()} if not already committed.
		 */
		@Override
		public void close() {
			commit();
		}
	}

	/**
	 * Manages an exclusive, zero-copy TX slot in the producer arena of a specific spoke.
	 * Write the payload directly into {@link #slot()}, then call {@link #commit} or {@link #rollback}.
	 *
	 * @apiNote {@link #close()} rolls back automatically if the slot has not been committed.
	 * @implSpec {@link #commit} calls {@code tachyon_star_commit_tx}, which flushes the spoke arena internally; no
	 * separate {@link TachyonStarBus#flush} call is required.
	 */
	public static final class StarTxGuard implements AutoCloseable {
		private final MemorySegment starHandle;
		private final MemorySegment slot;
		private final int spokeIdx;
		private boolean consumed;

		StarTxGuard(MemorySegment starHandle, MemorySegment slot, int spokeIdx) {
			this.starHandle = starHandle;
			this.slot = slot;
			this.spokeIdx = spokeIdx;
			this.consumed = false;
		}

		/**
		 * Writable segment pointing directly into shared memory.
		 *
		 * @throws IllegalStateException If the slot has already been committed or rolled back.
		 */
		public MemorySegment slot() {
			if (consumed) {
				throw new IllegalStateException("StarTxGuard already consumed");
			}

			return slot;
		}

		/**
		 * Publishes {@code actualSize} bytes with {@code typeId} and flushes the spoke arena.
		 *
		 * @param actualSize The number of bytes written to the slot. Must not exceed the reservation.
		 * @param typeId     The user-defined protocol identifier.
		 * @throws IllegalStateException If the slot has already been committed or rolled back.
		 * @throws TachyonException      If no slot was acquired or {@code actualSize} exceeds the reservation.
		 */
		public void commit(long actualSize, int typeId) {
			if (consumed) {
				throw new IllegalStateException("StarTxGuard already consumed");
			}

			consumed = true;
			TachyonABI.starCommitTx(starHandle, spokeIdx, actualSize, typeId);
		}

		/**
		 * Aborts the TX slot without publishing. No-op if already consumed.
		 */
		public void rollback() {
			if (consumed) {
				return;
			}

			consumed = true;
			TachyonABI.starRollbackTx(starHandle, spokeIdx);
		}

		/**
		 * Calls {@link #rollback()} if not yet consumed.
		 */
		@Override
		public void close() throws Exception {
			rollback();
		}
	}
}
