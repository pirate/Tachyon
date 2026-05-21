import types
from typing import Any, Callable, Generator, Iterator, List, NamedTuple, Optional, Sequence, Type
from . import _tachyon

__all__ = [
	"TachyonBus",
	"TachyonError",
	"PeerDeadError",
	"TxGuard",
	"RxGuard",
	"RxBatchGuard",
	"RxMsgView",
	"TachyonRpcBus",
	"RpcTxGuard",
	"RpcRxGuard",
	"Bus",
	"BusStats",
	"RpcBus",
	"RpcDispatcher",
	"RpcEndpoint",
	"TachyonStarBus",
	"StarBus",
	"StarMsgView",
	"tachyon_rpc",
	"MSG_TYPE_ERROR",
	"Message",
	"make_type_id",
	"route_id",
	"msg_type",
]
__version__ = "0.5.1"

MSG_TYPE_ERROR: int


def make_type_id(route: int, msg_type: int) -> int:
	"""Encodes route_id and msg_type into a single type_id value.

	Bits [31:16] = route_id, bits [15:0] = msg_type.
	Use route=0 to preserve v0.3.x behavior: make_type_id(0, 42) == 42.
	route >= 1 is reserved for RPC.
	"""
	...


def route_id(type_id: int) -> int:
	"""Extracts the route_id from bits [31:16] of type_id."""
	...


def msg_type(type_id: int) -> int:
	"""Extracts the msg_type from bits [15:0] of type_id."""
	...


class TachyonError(Exception):
	"""Base Tachyon IPC exception."""
	pass


class PeerDeadError(TachyonError):
	"""Raised when the bus detects a corrupted message header (FatalError state)."""
	pass


TachyonBus = _tachyon.TachyonBus
TxGuard = _tachyon.TxGuard
RxGuard = _tachyon.RxGuard
RxBatchGuard = _tachyon.RxBatchGuard
RxMsgView = _tachyon.RxMsgView
TachyonRpcBus = _tachyon.TachyonRpcBus
RpcTxGuard = _tachyon.RpcTxGuard
RpcRxGuard = _tachyon.RpcRxGuard
TachyonStarBus = _tachyon.TachyonStarBus


class Message:
	"""Tachyon Message Dataclass."""
	type_id: int
	size: int
	data: bytes | memoryview


class Bus:
	"""Tachyon IPC High-Level API."""

	def __init__(self) -> None:
		"""Private. Use listen() or connect()."""
		...

	@classmethod
	def listen(cls, socket_path: str, capacity: int) -> "Bus":
		"""Creates SHM arena and binds UNIX socket."""
		...

	@classmethod
	def connect(cls, socket_path: str) -> "Bus":
		"""Attaches to existing SHM arena via UNIX socket."""
		...

	def __enter__(self) -> "Bus": ...

	def __exit__(
			self,
			exc_type: Optional[Type[BaseException]],
			exc_val: Optional[BaseException],
			exc_tb: Optional[types.TracebackType]
	) -> None:
		"""Unmaps SHM and closes FDs."""
		...

	def set_numa_node(self, node_id: int) -> None:
		"""
		Binds the shared memory backing this bus to a specific NUMA node.

		Uses MPOL_PREFERRED + MPOL_MF_MOVE. Call immediately after listen()/connect().
		No-op on non-Linux platforms.

		:param node_id: NUMA node index (0-based, 0–63).
		:raise OSError: mbind() failure (invalid node or missing CAP_SYS_NICE).
		:raise ValueError: node_id negative or >= 64.
		"""
		...

	def set_polling_mode(self, pure_spin: int) -> None:
		"""
		Signals that the consumer will never sleep, skipping the futex wake check
		on every producer flush.

		When pure_spin=1, the producer omits the atomic_thread_fence(seq_cst) and
		the consumer_sleeping load on every flush_tx. Use only when the consumer
		thread is dedicated and SCHED_FIFO, if it ever parks, the producer will
		not wake it.

		Call immediately after listen()/connect(), before the first message.

		:param pure_spin: 1 to enable pure-spin mode, 0 to restore hybrid mode.
		"""
		...

	def send(self, data: bytes, type_id: int = 0) -> None:
		"""Blocking SPSC write. Copies payload, commits, and flushes."""
		...

	def __iter__(self) -> Iterator[Message]:
		"""Blocking RX iterator. Yields copied Messages (bytes) to prevent Use-After-Commit."""
		...

	def send_zero_copy(self, size: int, type_id: int = 0) -> Generator[TxGuard, None, None]:
		"""Zero-copy TX lock. Yields raw TxGuard. Caller must update actual_size. Auto-flushes on exit."""
		...

	def recv_zero_copy(self) -> RxGuard:
		"""Zero-copy RX lock. Returns raw RxGuard. Caller must release memoryview before context exit."""
		...

	def drain_batch(self, max_msgs: int = 1024, spin_threshold: int = 10000) -> RxBatchGuard:
		"""Batch RX. Blocks until ≥1 message, drains up to max_msgs."""
		...

	def stats(self) -> "BusStats":
		"""Returns a read-only snapshot of the bus state."""
		...


class BusStats(NamedTuple):
	"""Read-only snapshot of bus state."""
	ring_capacity: int
	ring_occupancy: int
	consumer_state: int  # 0 = awake, 1 = sleeping on futex, 2 = pure-spin
	state: int


class RpcBus:
	"""Tachyon RPC High-Level API."""

	def __init__(self) -> None:
		"""Private. Use rpc_listen() or rpc_connect()."""
		...

	@classmethod
	def rpc_listen(cls, socket_path: str, cap_fwd: int, cap_rev: int) -> "RpcBus":
		"""Creates two SHM arenas (fwd + rev) and binds UNIX socket."""
		...

	@classmethod
	def rpc_connect(cls, socket_path: str) -> "RpcBus":
		"""Attaches to existing SHM arenas via UNIX socket."""
		...

	def __enter__(self) -> "RpcBus": ...

	def __exit__(
			self,
			exc_type: Optional[Type[BaseException]],
			exc_val: Optional[BaseException],
			exc_tb: Optional[types.TracebackType],
	) -> None: ...

	def set_polling_mode(self, pure_spin: int) -> None:
		"""
		Signals that both consumers (fwd + rev arenas) will never sleep.
		pure_spin=1 enables, 0 restores hybrid mode.
		"""
		...

	def call(
			self,
			payload: bytes,
			msg_type: int,
			max_payload_size: Optional[int] = None,
			spin_threshold: int = 10000,
	) -> int:
		"""
		Copies payload into arena_fwd and returns the assigned correlation_id.

		:param max_payload_size: Reserve size. Defaults to len(payload).
		:return: correlation_id for use with wait().
		"""
		...

	def call_zero_copy(self, max_payload_size: int, msg_type: int) -> RpcTxGuard:
		"""
		Zero-copy TX call guard. Fill buffer, set actual_size, exit context.
		Read out_cid after context exit.
		"""
		...

	def wait(self, correlation_id: int, spin_threshold: int = 10000) -> RpcRxGuard:
		"""
		Blocks until the response matching correlation_id arrives.
		Returns an RpcRxGuard context manager.

		:raise PeerDeadError: Cid mismatch or FatalError on arena_rev.
		:raise KeyboardInterrupt: Interrupted by signal.
		"""
		...

	def serve(self, spin_threshold: int = 10000) -> RpcRxGuard:
		"""
		Blocks until a request arrives. Returns an RpcRxGuard.
		Read correlation_id from the guard before exiting the context.

		:raise PeerDeadError: FatalError on arena_fwd.
		:raise KeyboardInterrupt: Interrupted by signal.
		"""
		...

	def reply(
			self,
			correlation_id: int,
			payload: bytes,
			msg_type: int,
			max_payload_size: Optional[int] = None,
	) -> None:
		"""
		Copies payload into arena_rev as a response to correlation_id.

		:raise ValueError: correlation_id == 0.
		:raise PeerDeadError: FatalError on arena_rev.
		"""
		...

	def reply_zero_copy(self, correlation_id: int, max_payload_size: int, msg_type: int) -> RpcTxGuard:
		"""
		Zero-copy TX reply guard. Fill buffer, set actual_size, exit context.

		:raise ValueError: correlation_id == 0.
		"""
		...


def _decode_error(payload: bytes) -> int: ...


def tachyon_rpc(msg_type: int) -> Callable[[Callable[[memoryview], bytes]], RpcEndpoint]:
	"""
	Decorator: bind a memoryview -> bytes handler to a msg_type.
	msg_type must be in [0, 65534]. 65535 is reserved for protocol errors.
	"""
	...


class RpcEndpoint:
	msg_type: int
	__name__: str

	def __call__(self, payload: bytes | memoryview) -> bytes: ...

	def call(
			self,
			bus: RpcBus,
			payload: bytes,
			on_response: Optional[Callable[[memoryview], Any]] = None,
			spin_threshold: int = 10000,
	) -> bytes: ...


class RpcDispatcher:
	def register(self, endpoint: RpcEndpoint) -> "RpcDispatcher": ...

	def handler(self, msg_type: int) -> Callable[[Callable[[memoryview], bytes]], RpcEndpoint]: ...

	def serve_once(self, bus: RpcBus, spin_threshold: int = 10000) -> None: ...

	def serve_forever(self, bus: RpcBus, spin_threshold: int = 10000) -> None: ...


class StarMsgView(NamedTuple):
	"""Immutable view of one message received from a specific spoke."""
	data: bytes
	type_id: int
	actual_size: int
	spoke_idx: int


class StarBus:
	"""
	Aggregates N independent SPSC arenas into a single round-robin polling loop bounded by a TSC-calibrated time budget.
	One consumer per StarBus; one producer per spoke.
	"""

	def __init__(self) -> None:
		"""Private. Use :meth:`create`."""
		...

	@classmethod
	def create(
			cls,
			buses: Sequence["Bus"],
			node_ids: Optional[Sequence[int]] = None,
	) -> "StarBus":
		"""
		Creates a StarBus from *buses* (connector-side).

		:param buses:    Non-empty sequence of :class:`Bus` instances.
		:param node_ids: Optional NUMA node IDs, same length as *buses*.
						 Negative values skip binding. ``None`` disables NUMA.
		:raises ValueError:  *buses* empty or *node_ids* length mismatches.
		:raises TypeError:   An element of *buses* is not a :class:`Bus`.
		:raises OSError:     Native SHM or TSC calibration failure.
		"""
		...

	def __enter__(self) -> "StarBus": ...

	def __exit__(
			self,
			exc_type: Optional[Type[BaseException]],
			exc_val: Optional[BaseException],
			exc_tb: Optional[types.TracebackType],
	) -> None: ...

	@property
	def n_spokes(self) -> int:
		"""Number of spokes."""
		...

	def get_state(self, spoke_idx: int) -> int:
		"""
		Raw ``tachyon_state_t`` integer for *spoke_idx*.
		Returns ``TACHYON_STATE_UNKNOWN`` (5) if *spoke_idx* is out of range.
		"""
		...

	def poll(self, max_total: int, budget_us: int) -> List[StarMsgView]:
		"""
		Drains up to *max_total* messages across all spokes within *budget_us*
		microseconds. Returns an empty list when the budget expires.

		Payload bytes are heap-copied; ownership passes to the caller.
		Must call :meth:`commit` to release ring-buffer slots.

		:param max_total: Upper bound on messages. Must be > 0.
		:param budget_us: TSC-bounded polling budget in microseconds.
		"""
		...

	def commit(self) -> None:
		"""
		Advances consumer tails for all polled spokes, releasing ring-buffer slots.
		Safe to call when the last poll returned an empty list.

		:raises OSError: Native fatal-error state.
		"""
		...

	def send_zero_copy(
			self, spoke_idx: int, size: int, type_id: int = 0
	) -> Generator[memoryview, None, None]:
		"""
		Zero-copy TX context manager on *spoke_idx*. Yields a writable
		:class:`memoryview` into SHM. Commits and flushes on normal exit;
		rolls back on exception.

		:raises RuntimeError: Ring full or *spoke_idx* out of range.
		"""
		...

	def acquire_tx(self, spoke_idx: int, size: int) -> Optional[memoryview]:
		"""
		Non-blocking TX slot. Returns a writable memoryview or ``None`` if
		the ring is full / *spoke_idx* is out of range.
		"""
		...

	def commit_tx(self, spoke_idx: int, actual_size: int, type_id: int = 0) -> None:
		"""Publishes *actual_size* bytes and flushes. No separate flush needed."""
		...

	def rollback_tx(self, spoke_idx: int) -> None:
		"""Aborts the pending TX slot. No-op if no slot is held."""
		...

	def flush(self, spoke_idx: int) -> None:
		"""Futex wake on *spoke_idx* without committing a TX slot."""
		...

	def close(self) -> None:
		"""Explicit teardown. Idempotent."""
		...
