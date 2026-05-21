from __future__ import annotations

import types
from contextlib import contextmanager
from typing import Generator, Iterator, List, NamedTuple, Optional, Sequence, Tuple, Type

from . import _tachyon
from .bus import Bus


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

	Each spoke is a :class:`Bus` created by the producer (Listener) side. The star holds Connector-side handles and
	ref-counts them internally; the caller may close its own handles after :meth:`create` returns.

	Not thread-safe. All polling and TX operations must be driven from a single consumer thread.
	"""

	__slots__ = ("_star",)

	def __init__(self) -> None:
		"""Private. Use :meth:`create` factory."""
		self._star = _tachyon.TachyonStarBus()

	def __enter__(self) -> StarBus:
		return self

	def __exit__(
			self,
			exc_type: Optional[Type[BaseException]],
			exc_val: Optional[BaseException],
			exc_tb: Optional[types.TracebackType],
	) -> None:
		self._star.destroy()

	@classmethod
	def create(cls, buses: Sequence[Bus], node_ids: Optional[Sequence[int]] = None) -> StarBus:
		"""
		Creates a :class:`StarBus` from *buses* (connector-side).

		Each bus is ref-counted internally; the caller may close its own handles after this returns.

		:param buses:    Non-empty sequence of :class:`Bus` instances, one per spoke.
		:param node_ids: Optional NUMA node IDs, same length as *buses*.
						 Negative values skip binding for that spoke.
						 ``None`` disables NUMA binding entirely.
		:raises ValueError:  *buses* is empty or *node_ids* length mismatches.
		:raises TypeError:   An element of *buses* is not a :class:`Bus`.
		:raises OSError:     Native SHM or TSC calibration failure.
		"""
		if not buses:
			raise ValueError("buses must not be empty")
		if node_ids is not None and len(node_ids) != len(buses):
			raise ValueError("node_ids length must equal len(buses)")

		instance = cls()
		raw_buses = [b._bus for b in buses]
		instance._star.create(raw_buses, list(node_ids) if node_ids is not None else None)
		return instance

	@property
	def n_spokes(self) -> int:
		"""Number of spokes."""
		return self._star.n_spokes()

	def get_state(self, spoke_idx: int) -> int:
		"""
		Raw ``tachyon_state_t`` integer for *spoke_idx*.
		Returns ``TACHYON_STATE_UNKNOWN`` (5) if *spoke_idx* is out of range.
		"""
		return self._star.get_state(spoke_idx=spoke_idx)

	def poll(self, max_total: int, budget_us: int) -> List[StarMsgView]:
		"""
		Drains up to *max_total* messages across all spokes within *budget_us* microseconds (TSC-bounded).
		Returns an empty list if the budget expires before any message arrives.

		Payload bytes are copied out of SHM before returning, so the list is fully owned by the caller.

		**Must call** :meth:`commit` after processing to release ring-buffer slots.
		Internal ``pending_`` state accumulates; a single commit releases all pending slots across all spokes.

		:param max_total:  Upper bound on messages. Must be > 0.
		:param budget_us:  Polling budget in microseconds.
		:returns:          List of :class:`StarMsgView`.
		"""
		raw: List[Tuple[bytes, int, int, int]] = self._star.poll(
			max_total=max_total, budget_us=budget_us
		)
		return [StarMsgView(data=data, type_id=tid, actual_size=size, spoke_idx=sidx)
		        for data, tid, size, sidx in raw]

	def commit(self) -> None:
		"""
		Advances consumer tails for all spokes that had messages in the last :meth:`poll` call, releasing the
		ring-buffer slots. Safe to call when the last poll returned an empty list.

		:raises OSError: Native fatal-error state.
		"""
		self._star.commit()

	@contextmanager
	def send_zero_copy(
			self, spoke_idx: int, size: int, type_id: int = 0
	) -> Generator[memoryview, None, None]:
		"""
		Zero-copy TX context manager on *spoke_idx*. Yields a writable :class:`memoryview` into SHM.
		On normal exit, commits and flushes. On exception, rolls back without publishing.

		:param spoke_idx: Zero-based spoke index.
		:param size:      Required contiguous byte capacity.
		:param type_id:   User-defined protocol identifier.
		:raises RuntimeError: Ring is full or *spoke_idx* is out of range.
		"""
		mv = self._star.acquire_tx(spoke_idx=spoke_idx, size=size)
		if mv is None:
			raise RuntimeError(
				f"acquire_tx failed: ring full or spoke_idx {spoke_idx} out of range"
			)
		try:
			yield mv
		except Exception:
			self._star.rollback_tx(spoke_idx=spoke_idx)
			raise
		else:
			self._star.commit_tx(spoke_idx=spoke_idx, actual_size=size, type_id=type_id)

	def acquire_tx(self, spoke_idx: int, size: int) -> Optional[memoryview]:
		"""
		Non-blocking TX slot acquisition on *spoke_idx*. Returns a writable :class:`memoryview` into SHM, or ``None``
		if the ring is full or *spoke_idx* is out of range.

		Caller must explicitly call :meth:`commit_tx` or :meth:`rollback_tx`.
		Prefer :meth:`send_zero_copy` for automatic resource management.
		"""
		return self._star.acquire_tx(spoke_idx=spoke_idx, size=size)

	def commit_tx(self, spoke_idx: int, actual_size: int, type_id: int = 0) -> None:
		"""
		Publishes *actual_size* bytes with *type_id* and flushes the spoke arena.
		No separate :meth:`flush` call is required.

		:raises OSError: Native error (size exceeded or no slot acquired).
		"""
		self._star.commit_tx(spoke_idx=spoke_idx, actual_size=actual_size, type_id=type_id)

	def rollback_tx(self, spoke_idx: int) -> None:
		"""Aborts the pending TX slot without publishing. No-op if no slot is held."""
		self._star.rollback_tx(spoke_idx=spoke_idx)

	def flush(self, spoke_idx: int) -> None:
		"""
		Issues a futex wake on *spoke_idx* without committing a TX slot.
		Not needed after :meth:`commit_tx`, which flushes internally.
		"""
		self._star.flush(spoke_idx=spoke_idx)

	def close(self) -> None:
		"""Explicit teardown. Idempotent. Also called by ``__exit__``."""
		self._star.destroy()
