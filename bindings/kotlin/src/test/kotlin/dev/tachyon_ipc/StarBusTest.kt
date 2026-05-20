package dev.tachyon_ipc

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class StarBusTest {

    companion object {
        private const val DEFAULT_CAPACITY = 1024L * 1024L
        private const val BUDGET_US = 5_000L

        @BeforeAll
        @JvmStatic
        fun init() {
            NativeLoader.load()
        }

        private fun spawnSender(socketPath: String, payload: ByteArray, typeId: Int): Thread =
            Thread.ofVirtual().start {
                Bus.listen(socketPath, DEFAULT_CAPACITY).use { producer ->
                    producer.acquireTx(payload.size.toLong()).use { tx ->
                        MemorySegment.copy(
                            MemorySegment.ofArray(payload), 0L,
                            tx.data, 0L,
                            payload.size.toLong(),
                        )
                        tx.commit(payload.size.toLong(), typeId)
                    }
                }
            }

        private fun spawnIdleListener(socketPath: String): Thread =
            Thread.ofVirtual().start {
                Bus.listen(socketPath, DEFAULT_CAPACITY).use {
                    try {
                        Thread.sleep(Long.MAX_VALUE)
                    } catch (_: InterruptedException) {
                        Thread.currentThread().interrupt()
                    }
                }
            }

        private fun spawnListener(
            socketPath: String,
            ref: AtomicReference<Bus>,
            latch: CountDownLatch,
        ): Thread = Thread.ofVirtual().start {
            val bus = Bus.listen(socketPath, DEFAULT_CAPACITY)
            ref.set(bus)
            latch.countDown()
        }

        private fun connectWithRetry(socketPath: String): Bus {
            repeat(100) {
                try {
                    return Bus.connect(socketPath)
                } catch (e: TachyonException) {
                    if (e.code != 11) throw e
                    Thread.sleep(10)
                }
            }
            throw RuntimeException("connect() failed after 100 retries on: $socketPath")
        }

        private fun longToBytes(value: Long): ByteArray =
            ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(value).array()
    }

    @Test
    fun testNSpokes(@TempDir tempDir: Path) {
        val path0 = tempDir.resolve("s0.sock").toString()
        val path1 = tempDir.resolve("s1.sock").toString()
        val t0 = spawnIdleListener(path0)
        val t1 = spawnIdleListener(path1)

        connectWithRetry(path0).use { c0 ->
            connectWithRetry(path1).use { c1 ->
                StarBus.create(arrayOf(c0, c1)).use { star ->
                    assertEquals(2, star.nSpokes, "nSpokes must reflect the number of buses passed at creation")
                }
            }
        }

        t0.interrupt(); t1.interrupt()
        t0.join(2000); t1.join(2000)
    }

    @Test
    fun testPollSingleSpoke(@TempDir tempDir: Path) {
        val path = tempDir.resolve("single.sock").toString()
        val expected = 0xDEADBEEFCAFEL
        val sender = spawnSender(path, longToBytes(expected), 42)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                star.poll(8, BUDGET_US).use { guard ->
                    assertEquals(1, guard.count(), "Exactly one message must be drained")
                    val msg = guard.get(0)
                    assertEquals(42, msg.typeId(), "typeId must match")
                    assertEquals(8L, msg.actualSize(), "actualSize must match")
                    assertEquals(0, msg.spokeIdx(), "spokeIdx must be 0 for the sole spoke")
                    val received = ByteBuffer.wrap(msg.payload().toArray(ValueLayout.JAVA_BYTE))
                        .order(ByteOrder.nativeOrder()).getLong()
                    assertEquals(expected, received, "Payload must be byte-exact")
                }
            }
        }

        sender.join(2000)
    }

    @Test
    fun testPollMultipleSpokes(@TempDir tempDir: Path) {
        val path0 = tempDir.resolve("m0.sock").toString()
        val path1 = tempDir.resolve("m1.sock").toString()
        val val0 = 111L
        val val1 = 222L
        val t0 = spawnSender(path0, longToBytes(val0), 10)
        val t1 = spawnSender(path1, longToBytes(val1), 11)

        connectWithRetry(path0).use { c0 ->
            connectWithRetry(path1).use { c1 ->
                StarBus.create(arrayOf(c0, c1)).use { star ->
                    star.poll(16, BUDGET_US).use { guard ->
                        assertEquals(2, guard.count(), "Both spokes must contribute one message each")

                        var received0 = Long.MIN_VALUE
                        var received1 = Long.MIN_VALUE
                        var typeId0 = -1
                        var typeId1 = -1

                        repeat(guard.count()) { i ->
                            val msg = guard.get(i)
                            val v = ByteBuffer.wrap(msg.payload().toArray(ValueLayout.JAVA_BYTE))
                                .order(ByteOrder.nativeOrder()).getLong()
                            if (msg.spokeIdx() == 0) {
                                received0 = v; typeId0 = msg.typeId()
                            } else {
                                received1 = v; typeId1 = msg.typeId()
                            }
                        }

                        assertEquals(val0, received0, "Spoke 0 payload must be byte-exact")
                        assertEquals(val1, received1, "Spoke 1 payload must be byte-exact")
                        assertEquals(10, typeId0, "Spoke 0 typeId must match")
                        assertEquals(11, typeId1, "Spoke 1 typeId must match")
                    }
                }
            }
        }

        t0.join(2000); t1.join(2000)
    }

    @Test
    fun testReceiveFlow(@TempDir tempDir: Path) = runBlocking {
        val path = tempDir.resolve("flow.sock").toString()
        val expected = 0xCAFEBABEL
        val sender = spawnSender(path, longToBytes(expected), 7)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                val msg = withTimeout(5_000.milliseconds) {
                    star.receive(maxBatch = 8, budgetUs = 1_000L).first()
                }
                assertEquals(7, msg.typeId, "typeId must match")
                val received = ByteBuffer.wrap(msg.payload)
                    .order(ByteOrder.nativeOrder()).getLong()
                assertEquals(expected, received, "Payload must be byte-exact")
            }
        }

        sender.join(2000)
    }

    @Test
    fun testPollEmptyOnBudgetExpiry(@TempDir tempDir: Path) {
        val path = tempDir.resolve("empty.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                star.poll(8, 200L).use { guard ->
                    assertTrue(guard.isEmpty, "No message must be drained when the spoke is silent")
                }
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testPollGuardCommitThenPollAgain(@TempDir tempDir: Path) {
        val path = tempDir.resolve("commit.sock").toString()
        val producerRef = AtomicReference<Bus>()
        val latch = CountDownLatch(1)
        val listener = spawnListener(path, producerRef, latch)

        connectWithRetry(path).use { connector ->
            assertTrue(latch.await(2, TimeUnit.SECONDS))
            producerRef.get().use { producer ->
                producer.acquireTx(4).use { tx ->
                    tx.data.set(ValueLayout.JAVA_INT, 0, 99)
                    tx.commit(4, 7)
                }

                StarBus.create(arrayOf(connector)).use { star ->
                    star.poll(8, BUDGET_US).use { first ->
                        assertEquals(1, first.count(), "First poll must drain the message")
                    }
                    star.poll(8, 200L).use { second ->
                        assertTrue(second.isEmpty, "Ring must be empty after commit")
                    }
                }
            }
        }

        listener.join(2000)
    }

    @Test
    fun testPollGuardIdempotentCommit(@TempDir tempDir: Path) {
        val path = tempDir.resolve("idempotent.sock").toString()
        val producerRef = AtomicReference<Bus>()
        val latch = CountDownLatch(1)
        val listener = spawnListener(path, producerRef, latch)

        connectWithRetry(path).use { connector ->
            assertTrue(latch.await(2, TimeUnit.SECONDS))
            producerRef.get().use { producer ->
                producer.acquireTx(4).use { tx ->
                    tx.data.set(ValueLayout.JAVA_INT, 0, 1)
                    tx.commit(4, 1)
                }

                StarBus.create(arrayOf(connector)).use { star ->
                    star.poll(8, BUDGET_US).use { guard ->
                        assertEquals(1, guard.count())
                        guard.commit()
                        guard.commit()
                    }
                }
            }
        }

        listener.join(2000)
    }

    @Test
    fun testAcquireTxRollback(@TempDir tempDir: Path) {
        val path = tempDir.resolve("tx_rollback.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                star.acquireTx(0, 64)!!.use { tx ->
                    assertEquals(64, tx.slot().byteSize(), "Slot bounds must match requested maxSize")
                }
                star.poll(4, 200L).use { guard ->
                    assertTrue(guard.isEmpty, "Rolled-back TX must not be visible to the consumer")
                }
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testAcquireTxOobReturnsNull(@TempDir tempDir: Path) {
        val path = tempDir.resolve("tx_oob.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                assertNull(star.acquireTx(999, 64), "acquireTx with out-of-range spokeIdx must return null")
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testFlushDoesNotThrow(@TempDir tempDir: Path) {
        val path = tempDir.resolve("flush.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                star.flush(0)
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testGetState(@TempDir tempDir: Path) {
        val path = tempDir.resolve("state.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            StarBus.create(arrayOf(connector)).use { star ->
                assertNotEquals(
                    4 /* TACHYON_STATE_FATAL_ERROR */, star.getState(0),
                    "A freshly created spoke must not be in fatal error state"
                )
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testDoubleClose(@TempDir tempDir: Path) {
        val path = tempDir.resolve("dclose.sock").toString()
        val idle = spawnIdleListener(path)

        val connector = connectWithRetry(path)
        val star = StarBus.create(arrayOf(connector))
        star.close()
        star.close()

        connector.close()
        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testCreateRejectsEmptyBuses() {
        assertFailsWith<IllegalArgumentException>("create() with an empty array must throw") {
            StarBus.create(emptyArray())
        }
    }

    @Test
    fun testCreateRejectsNodeIdsMismatch(@TempDir tempDir: Path) {
        val path = tempDir.resolve("mismatch.sock").toString()
        val idle = spawnIdleListener(path)

        connectWithRetry(path).use { connector ->
            assertFailsWith<IllegalArgumentException>("nodeIds length mismatch must throw") {
                StarBus.create(arrayOf(connector), intArrayOf(0, 1))
            }
        }

        idle.interrupt()
        idle.join(2000)
    }

    @Test
    fun testMultiMessageSingleSpoke(@TempDir tempDir: Path) {
        val path = tempDir.resolve("multi.sock").toString()
        val n = 5
        val producerRef = AtomicReference<Bus>()
        val latch = CountDownLatch(1)
        val listener = spawnListener(path, producerRef, latch)

        connectWithRetry(path).use { connector ->
            assertTrue(latch.await(2, TimeUnit.SECONDS))
            producerRef.get().use { producer ->
                repeat(n) { i ->
                    producer.acquireTx(4).use { tx ->
                        tx.data.set(ValueLayout.JAVA_INT, 0, i)
                        tx.commitUnflushed(4, 100 + i)
                    }
                }
                producer.flush()

                StarBus.create(arrayOf(connector)).use { star ->
                    star.poll(n, BUDGET_US).use { guard ->
                        assertEquals(n, guard.count(), "All messages must be drained in one poll")
                        repeat(guard.count()) { i ->
                            val msg = guard.get(i)
                            assertEquals(0, msg.spokeIdx())
                            assertEquals(100 + i, msg.typeId(), "typeId must be in send order")
                            assertEquals(
                                i, msg.payload().get(ValueLayout.JAVA_INT, 0),
                                "Payload must be in send order"
                            )
                        }
                    }
                }
            }
        }

        listener.join(2000)
    }
}
