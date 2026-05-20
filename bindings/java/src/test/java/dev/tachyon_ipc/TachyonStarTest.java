package dev.tachyon_ipc;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

public class TachyonStarTest {
	private static final long DEFAULT_CAPACITY = 1024 * 1024;
	private static final long BUDGET_US = 5_000L;

	private static Thread spawnSender(String socketPath, byte[] payload, int typeId) {
		return Thread.ofVirtual().start(() -> {
			try (TachyonBus producer = TachyonBus.listen(socketPath, DEFAULT_CAPACITY)) {
				try (TxGuard tx = producer.acquireTx(payload.length)) {
					MemorySegment.copy(payload, 0, tx.getData(), ValueLayout.JAVA_BYTE, 0L, payload.length);
					tx.commit(payload.length, typeId);
				}
			}
		});
	}

	private static Thread spawnIdleListener(String socketPath) {
		return Thread.ofVirtual().start(() -> {
			try (TachyonBus ignored = TachyonBus.listen(socketPath, DEFAULT_CAPACITY)) {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
	}

	private static Thread spawnListener(String socketPath, AtomicReference<TachyonBus> ref, CountDownLatch latch) {
		return Thread.ofVirtual().start(() -> {
			TachyonBus bus = TachyonBus.listen(socketPath, DEFAULT_CAPACITY);
			ref.set(bus);
			latch.countDown();
		});
	}

	private static TachyonBus connectWithRetry(String socketPath) throws InterruptedException {
		for (int attempt = 0; attempt < 100; attempt++) {
			try {
				return TachyonBus.connect(socketPath);
			} catch (TachyonException e) {
				if (e.getCode() != 11) throw e;
				Thread.sleep(10);
			}
		}

		throw new RuntimeException(STR."connect() failed after 100 retries (1s) on: \{socketPath}");
	}

	private static byte[] longToBytes(long value) {
		byte[] buf = new byte[8];
		ByteBuffer.wrap(buf).order(ByteOrder.nativeOrder()).putLong(0, value);
		return buf;
	}

	@BeforeAll
	static void init() {
		NativeLoader.load();
	}

	@Test
	void testNSpokes(@TempDir Path tempDir) throws Exception {
		String path0 = tempDir.resolve("s0.sock").toString();
		String path1 = tempDir.resolve("s1.sock").toString();
		Thread t0 = spawnIdleListener(path0);
		Thread t1 = spawnIdleListener(path1);

		try (TachyonBus c0 = connectWithRetry(path0);
		     TachyonBus c1 = connectWithRetry(path1);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{c0, c1}, null)) {
			assertEquals(2, star.nSpokes(), "nSpokes() must reflect the number of buses passed at creation");
		}

		t0.interrupt();
		t1.interrupt();
		t0.join(2000);
		t1.join(2000);
	}

	@Test
	void testPollSingleSpoke(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("single.sock").toString();
		long expected = 0xDEADBEEFCAFEL;
		int typeId = 42;
		Thread sender = spawnSender(path, longToBytes(expected), typeId);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null);
		     TachyonStarBus.StarPollGuard guard = star.poll(8, BUDGET_US)) {

			assertEquals(1, guard.count(), "Exactly one message must be drained");
			TachyonStarBus.StarMsgView msg = guard.get(0);
			assertEquals(typeId, msg.typeId(), "typeId must match");
			assertEquals(8, msg.actualSize(), "actualSize must match");
			assertEquals(0, msg.spokeIdx(), "spokeIdx must be 0 for the sole spoke");
			long received = ByteBuffer.wrap(msg.payload().toArray(ValueLayout.JAVA_BYTE))
					.order(ByteOrder.nativeOrder()).getLong(0);
			assertEquals(expected, received, "Payload must be byte-exact");
		}

		sender.join(2000);
	}

	@Test
	void testPollMultipleSpokes(@TempDir Path tempDir) throws Exception {
		String path0 = tempDir.resolve("m0.sock").toString();
		String path1 = tempDir.resolve("m1.sock").toString();
		long val0 = 111L;
		long val1 = 222L;
		Thread t0 = spawnSender(path0, longToBytes(val0), 10);
		Thread t1 = spawnSender(path1, longToBytes(val1), 11);

		try (TachyonBus c0 = connectWithRetry(path0);
		     TachyonBus c1 = connectWithRetry(path1);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{c0, c1}, null);
		     TachyonStarBus.StarPollGuard guard = star.poll(16, BUDGET_US)) {

			assertEquals(2, guard.count(), "Both spokes must contribute one message each");

			long received0 = Long.MIN_VALUE;
			long received1 = Long.MIN_VALUE;
			int typeId0 = -1;
			int typeId1 = -1;

			for (int i = 0; i < guard.count(); i++) {
				TachyonStarBus.StarMsgView msg = guard.get(i);
				long v = ByteBuffer.wrap(msg.payload().toArray(ValueLayout.JAVA_BYTE))
						.order(ByteOrder.nativeOrder()).getLong(0);
				if (msg.spokeIdx() == 0) {
					received0 = v;
					typeId0 = msg.typeId();
				} else {
					received1 = v;
					typeId1 = msg.typeId();
				}
			}

			assertEquals(val0, received0, "Spoke 0 payload must be byte-exact");
			assertEquals(val1, received1, "Spoke 1 payload must be byte-exact");
			assertEquals(10, typeId0, "Spoke 0 typeId must match");
			assertEquals(11, typeId1, "Spoke 1 typeId must match");
		}

		t0.join(2000);
		t1.join(2000);
	}

	@Test
	void testPollEmptyOnBudgetExpiry(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("empty.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null);
		     TachyonStarBus.StarPollGuard guard = star.poll(8, 200L /* 200 µs */)) {

			assertTrue(guard.isEmpty(), "No message must be drained when the spoke is silent");
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testPollGuardCommitThenPollAgain(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("commit.sock").toString();
		AtomicReference<TachyonBus> producerRef = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		Thread listenerThread = spawnListener(path, producerRef, latch);

		try (TachyonBus connector = connectWithRetry(path)) {
			assertTrue(latch.await(2, TimeUnit.SECONDS));
			try (TachyonBus producer = producerRef.get()) {
				try (TxGuard tx = producer.acquireTx(4)) {
					tx.getData().set(ValueLayout.JAVA_INT, 0, 99);
					tx.commit(4, 7);
				}

				try (TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null)) {
					try (TachyonStarBus.StarPollGuard first = star.poll(8, BUDGET_US)) {
						assertEquals(1, first.count(), "First poll must drain the message");
					}

					try (TachyonStarBus.StarPollGuard second = star.poll(8, 200L)) {
						assertTrue(second.isEmpty(), "Ring must be empty after commit");
					}
				}
			}
		}

		listenerThread.join(2000);
	}

	@Test
	void testPollGuardIdempotentCommit(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("idempotent.sock").toString();
		AtomicReference<TachyonBus> producerRef = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		Thread listenerThread = spawnListener(path, producerRef, latch);

		try (TachyonBus connector = connectWithRetry(path)) {
			assertTrue(latch.await(2, TimeUnit.SECONDS));
			try (TachyonBus producer = producerRef.get()) {
				try (TxGuard tx = producer.acquireTx(4)) {
					tx.getData().set(ValueLayout.JAVA_INT, 0, 1);
					tx.commit(4, 1);
				}

				try (TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null);
				     TachyonStarBus.StarPollGuard guard = star.poll(8, BUDGET_US)) {
					assertEquals(1, guard.count());
					guard.commit();
					assertDoesNotThrow(guard::commit, "A second commit must be a no-op");
				}
			}
		}

		listenerThread.join(2000);
	}

	@Test
	void testAcquireTxRollback(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("tx_rollback.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null)) {

			try (TachyonStarBus.StarTxGuard tx = star.acquireTx(0, 64)) {
				assertNotNull(tx, "acquireTx must succeed on an empty ring");
				assertEquals(64, tx.slot().byteSize(), "Slot bounds must match requested maxSize");
			}

			try (TachyonStarBus.StarPollGuard g = star.poll(4, 200L)) {
				assertTrue(g.isEmpty(), "Rolled-back TX must not be visible to the consumer");
			}
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testAcquireTxOobReturnsNull(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("tx_oob.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null)) {
			assertNull(star.acquireTx(999, 64), "acquireTx with out-of-range spokeIdx must return null");
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testFlushDoesNotThrow(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("flush.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null)) {
			assertDoesNotThrow(() -> star.flush(0), "An explicit flush on a live spoke must not throw");
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testGetState(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("state.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path);
		     TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null)) {

			assertNotEquals(4 /* TACHYON_STATE_FATAL_ERROR */, star.getState(0),
					"A freshly created spoke must not be in fatal error state");
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testDoubleClose(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("dclose.sock").toString();
		Thread idle = spawnIdleListener(path);

		TachyonBus connector = connectWithRetry(path);
		TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null);
		star.close();
		assertDoesNotThrow(star::close, "A second close must be a no-op");
		connector.close();

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testCreateRejectsEmptyBuses() {
		assertThrows(IllegalArgumentException.class, () -> TachyonStarBus.create(new TachyonBus[0], null),
				"create() with an empty bus array must throw");
		assertThrows(IllegalArgumentException.class, () -> TachyonStarBus.create(null, null),
				"create() with a null bus array must throw");
	}

	@Test
	void testCreateRejectsNodeIdsMismatch(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("mismatch.sock").toString();
		Thread idle = spawnIdleListener(path);

		try (TachyonBus connector = connectWithRetry(path)) {
			assertThrows(IllegalArgumentException.class,
					() -> TachyonStarBus.create(new TachyonBus[]{connector}, new int[]{0, 1}),
					"nodeIds length mismatch must throw");
		}

		idle.interrupt();
		idle.join(2000);
	}

	@Test
	void testMultiMessageSingleSpoke(@TempDir Path tempDir) throws Exception {
		String path = tempDir.resolve("multi.sock").toString();
		int n = 5;
		AtomicReference<TachyonBus> producerRef = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		Thread listenerThread = spawnListener(path, producerRef, latch);

		try (TachyonBus connector = connectWithRetry(path)) {
			assertTrue(latch.await(2, TimeUnit.SECONDS));
			try (TachyonBus producer = producerRef.get()) {
				for (int i = 0; i < n; i++) {
					try (TxGuard tx = producer.acquireTx(4)) {
						tx.getData().set(ValueLayout.JAVA_INT, 0, i);
						tx.commitUnflushed(4, 100 + i);
					}
				}
				producer.flush();

				try (TachyonStarBus star = TachyonStarBus.create(new TachyonBus[]{connector}, null);
				     TachyonStarBus.StarPollGuard guard = star.poll(n, BUDGET_US)) {

					assertEquals(n, guard.count(), "All messages must be drained in one poll");
					for (int i = 0; i < guard.count(); i++) {
						TachyonStarBus.StarMsgView msg = guard.get(i);
						assertEquals(0, msg.spokeIdx());
						assertEquals(100 + i, msg.typeId(), "typeId must be in send order");
						assertEquals(i, msg.payload().get(ValueLayout.JAVA_INT, 0),
								"Payload must be in send order");
					}
				}
			}
		}

		listenerThread.join(2000);
	}
}
