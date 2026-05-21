import binascii
import os
import struct
import threading
import time

import pytest
import tachyon
from tachyon.star import StarBus, StarMsgView

CAPACITY = 1 << 16
BUDGET_US = 5_000


def _sock(tmp_path, name: str) -> str:
	unique_suffix = binascii.hexlify(os.urandom(4)).decode()
	return f"/tmp/tachyon_{unique_suffix}_{name}"

def _start_sender(socket_path: str, payload: bytes, type_id: int = 0) -> threading.Thread:
	def _run():
		with tachyon.Bus.listen(socket_path, CAPACITY) as producer:
			producer.send(payload, type_id=type_id)

	t = threading.Thread(target=_run, daemon=True)
	t.start()
	return t


def _connect_with_retry(socket_path: str, attempts: int = 100) -> tachyon.Bus:
	for _ in range(attempts):
		try:
			return tachyon.Bus.connect(socket_path)
		except (ConnectionError, OSError):
			time.sleep(0.01)
	return tachyon.Bus.connect(socket_path)


def test_n_spokes(tmp_path):
	p0, p1 = _sock(tmp_path, "s0.sock"), _sock(tmp_path, "s1.sock")
	t0 = _start_sender(p0, b"\x00", 0)
	t1 = _start_sender(p1, b"\x00", 0)

	with _connect_with_retry(p0) as c0, _connect_with_retry(p1) as c1:
		with StarBus.create([c0, c1]) as star:
			assert star.n_spokes == 2

	t0.join(2);
	t1.join(2)


def test_poll_single_spoke_payload(tmp_path):
	path = _sock(tmp_path, "single.sock")
	expected = struct.pack(">Q", 0xDEADBEEFCAFE)
	t = _start_sender(path, expected, type_id=42)

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			msgs = star.poll(max_total=8, budget_us=BUDGET_US)
			star.commit()

	t.join(2)

	assert len(msgs) == 1
	msg = msgs[0]
	assert isinstance(msg, StarMsgView)
	assert msg.type_id == 42
	assert msg.actual_size == len(expected)
	assert msg.spoke_idx == 0
	assert msg.data == expected


def test_poll_multiple_spokes(tmp_path):
	p0, p1 = _sock(tmp_path, "m0.sock"), _sock(tmp_path, "m1.sock")
	payload0 = struct.pack(">Q", 111)
	payload1 = struct.pack(">Q", 222)
	t0 = _start_sender(p0, payload0, type_id=10)
	t1 = _start_sender(p1, payload1, type_id=11)

	with _connect_with_retry(p0) as c0, _connect_with_retry(p1) as c1:
		with StarBus.create([c0, c1]) as star:
			msgs = star.poll(max_total=16, budget_us=BUDGET_US)
			star.commit()

	t0.join(2);
	t1.join(2)

	assert len(msgs) == 2
	by_spoke = {m.spoke_idx: m for m in msgs}
	assert by_spoke[0].data == payload0
	assert by_spoke[1].data == payload1
	assert by_spoke[0].type_id == 10
	assert by_spoke[1].type_id == 11


def test_poll_empty_on_budget_expiry(tmp_path):
	path = _sock(tmp_path, "empty.sock")
	ready = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			ready.set()
			time.sleep(30)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()
	ready.wait(2)

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			msgs = star.poll(max_total=8, budget_us=200)

	assert msgs == []
	t.join(0)


def test_poll_commit_releases_slots(tmp_path):
	path = _sock(tmp_path, "commit.sock")

	ready = threading.Event()
	sent = threading.Event()

	def _producer():
		with tachyon.Bus.listen(path, CAPACITY) as producer:
			ready.set()
			sent.wait(2)
			producer.send(b"\x01\x02\x03\x04", type_id=7)

	t = threading.Thread(target=_producer, daemon=True)
	t.start()
	ready.wait(2)

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			sent.set()
			first = star.poll(max_total=8, budget_us=BUDGET_US)
			assert len(first) == 1
			star.commit()

			second = star.poll(max_total=8, budget_us=200)
			assert second == [], "ring must be empty after commit"

	t.join(2)


def test_poll_commit_idempotent(tmp_path):
	path = _sock(tmp_path, "idempotent.sock")
	t = _start_sender(path, b"\xff", type_id=1)

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			star.poll(max_total=8, budget_us=BUDGET_US)
			star.commit()
			star.commit()

	t.join(2)


def test_send_zero_copy_commits(tmp_path):
	p0 = _sock(tmp_path, "txzc_s.sock")
	results = []

	ready = threading.Event()

	def _receiver():
		with tachyon.Bus.listen(p0, CAPACITY) as producer:
			ready.set()
			msg = next(iter(producer))
			results.append(msg)

	t = threading.Thread(target=_receiver, daemon=True)
	t.start()
	ready.wait(2)

	with _connect_with_retry(p0) as connector:
		with StarBus.create([connector]) as star:
			payload = struct.pack(">Q", 0xCAFEBABE)
			with star.send_zero_copy(spoke_idx=0, size=len(payload), type_id=55) as slot:
				slot[:] = payload

	t.join(2)
	assert len(results) == 1
	assert results[0].type_id == 55
	assert results[0].data == payload


def test_send_zero_copy_rollback_on_exception(tmp_path):
	path = _sock(tmp_path, "txzc_rb.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			with pytest.raises(ZeroDivisionError):
				with star.send_zero_copy(spoke_idx=0, size=8) as slot:
					slot[:4] = b"\x00\x00\x00\x01"
					raise ZeroDivisionError("intentional")

			msgs = star.poll(max_total=4, budget_us=200)
			assert msgs == []

	idle_done.set()
	t.join(2)


def test_acquire_tx_oob_returns_none(tmp_path):
	path = _sock(tmp_path, "oob.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			assert star.acquire_tx(spoke_idx=999, size=64) is None

	idle_done.set()
	t.join(2)


def test_flush_does_not_raise(tmp_path):
	path = _sock(tmp_path, "flush.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			star.flush(0)

	idle_done.set()
	t.join(2)


def test_get_state_not_fatal(tmp_path):
	path = _sock(tmp_path, "state.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			assert star.get_state(0) != 4

	idle_done.set()
	t.join(2)


def test_close_idempotent(tmp_path):
	path = _sock(tmp_path, "close.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		star = StarBus.create([connector])
		star.close()
		star.close()

	idle_done.set()
	t.join(2)


def test_create_rejects_empty_buses():
	with pytest.raises(ValueError, match="must not be empty"):
		StarBus.create([])


def test_create_rejects_node_ids_mismatch(tmp_path):
	path = _sock(tmp_path, "mismatch.sock")
	idle_done = threading.Event()

	def _idle():
		with tachyon.Bus.listen(path, CAPACITY):
			idle_done.wait(5)

	t = threading.Thread(target=_idle, daemon=True)
	t.start()

	with _connect_with_retry(path) as connector:
		with pytest.raises(ValueError, match="node_ids length"):
			StarBus.create([connector], node_ids=[0, 1])

	idle_done.set()
	t.join(2)


def test_multi_message_single_spoke_in_order(tmp_path):
	path = _sock(tmp_path, "multi.sock")
	n = 5

	ready = threading.Event()

	def _producer():
		with tachyon.Bus.listen(path, CAPACITY) as producer:
			ready.set()
			for i in range(n):
				producer.send(struct.pack(">I", i), type_id=100 + i)

	t = threading.Thread(target=_producer, daemon=True)
	t.start()
	ready.wait(2)

	with _connect_with_retry(path) as connector:
		with StarBus.create([connector]) as star:
			msgs = star.poll(max_total=n, budget_us=BUDGET_US)
			star.commit()

	t.join(2)

	assert len(msgs) == n
	for i, msg in enumerate(msgs):
		assert msg.spoke_idx == 0
		assert msg.type_id == 100 + i
		assert struct.unpack(">I", msg.data)[0] == i
