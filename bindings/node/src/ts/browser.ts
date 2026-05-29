import type { BusHandle, RawBatchMessage, RawRx } from './bus_core.ts';
import { BusBase } from './bus_core.ts';
import { ErrorCode, TachyonError } from './error.ts';
import type { CwrapFn, TachyonCoreModule } from './wasm/tachyon.js';
import createTachyonCore from './wasm/tachyon.js';

const TACHYON_SUCCESS = 0;

/**
 * wasm32 pointers and `size_t` are 32-bit, so the core rejects any capacity
 * larger than `INT32_MAX` (see `tachyon_bus_listen` / `SharedMemory::create`).
 * We reject the same boundary here, at the JS API surface, with a clean
 * exception so a 2GB+ request never reaches the core as a silent overflow.
 */
const MAX_CAPACITY = 0x7fff_ffff; // 2 GiB - 1

/** Maps a `tachyon_error_t` code to the JS error-code surface. */
function mapError(code: number): ErrorCode {
	switch (code) {
		case 1:
			return ErrorCode.NullPtr;
		case 2:
			return ErrorCode.Mem;
		case 8:
			return ErrorCode.InvalidSz;
		case 9:
			return ErrorCode.Full;
		case 10:
			return ErrorCode.Empty;
		default:
			return ErrorCode.System;
	}
}

// The Emscripten module is instantiated once per page. The compiled C++ core is
// the single source of truth; every ring operation goes through the fuzzed C ABI.
const core: TachyonCoreModule = await createTachyonCore();

const abi: {
	busListen: CwrapFn;
	busDestroy: CwrapFn;
	getShmPtr: CwrapFn;
	acquireTx: CwrapFn;
	commitTx: CwrapFn;
	rollbackTx: CwrapFn;
	acquireRx: CwrapFn;
	commitRx: CwrapFn;
	flush: CwrapFn;
	getState: CwrapFn;
	setPollingMode: CwrapFn;
} = {
	busListen: core.cwrap('tachyon_bus_listen', 'number', ['string', 'number', 'number']),
	busDestroy: core.cwrap('tachyon_bus_destroy', null, ['number']),
	getShmPtr: core.cwrap('tachyon_bus_get_shm_ptr', 'number', ['number']),
	acquireTx: core.cwrap('tachyon_acquire_tx', 'number', ['number', 'number']),
	commitTx: core.cwrap('tachyon_commit_tx', 'number', ['number', 'number', 'number']),
	rollbackTx: core.cwrap('tachyon_rollback_tx', 'number', ['number']),
	acquireRx: core.cwrap('tachyon_acquire_rx', 'number', ['number', 'number', 'number']),
	commitRx: core.cwrap('tachyon_commit_rx', 'number', ['number']),
	flush: core.cwrap('tachyon_flush', null, ['number']),
	getState: core.cwrap('tachyon_get_state', 'number', ['number']),
	setPollingMode: core.cwrap('tachyon_bus_set_polling_mode', null, ['number', 'number']),
};

// Scratch heap cells for C out-parameters. acquire_rx writes the type_id and the
// actual size here; listen writes the out bus pointer. Reused across calls.
const scratch = core._malloc(16);
const OUT_TYPE_ID = scratch; // uint32_t
const OUT_SIZE = scratch + 4; // size_t (32-bit on wasm32)
const OUT_BUS = scratch + 8; // tachyon_bus_t*

/** Maps a heap offset + length onto the live WASM memory as a zero-copy view. */
function slot(ptr: number, len: number): Uint8Array {
	// HEAPU8.buffer is re-read every call because memory growth swaps the buffer.
	return new Uint8Array(core.HEAPU8.buffer, ptr, len);
}

function detachArrayBuffer(buffer: ArrayBuffer): void {
	if (buffer.byteLength === 0) return;
	structuredClone(buffer, { transfer: [buffer] });
}

interface BrowserEndpoint {
	/** Raw `tachyon_bus_t*` shared by the listener and every connected peer. */
	busPtr: number;
	refs: number;
}

const endpoints = new Map<string, BrowserEndpoint>();

class BrowserBusHandle implements BusHandle<Uint8Array> {
	#endpoint: BrowserEndpoint;
	#path: string;
	#batchBuffers: ArrayBuffer[] = [];

	public constructor(path: string, endpoint: BrowserEndpoint) {
		this.#path = path;
		this.#endpoint = endpoint;
	}

	get #bus(): number {
		return this.#endpoint.busPtr;
	}

	public close(): void {
		this.#endpoint.refs -= 1;
		if (this.#endpoint.refs <= 0) {
			endpoints.delete(this.#path);
			abi.busDestroy(this.#endpoint.busPtr);
			this.#endpoint.busPtr = 0;
		}
	}

	public send(data: Buffer | Uint8Array, typeId = 0): void {
		const ptr = abi.acquireTx(this.#bus, data.length);
		if (ptr === 0) throw new TachyonError('Bus.send: the ring buffer is full.', ErrorCode.Full);
		core.HEAPU8.set(data, ptr);
		const rc = abi.commitTx(this.#bus, data.length, typeId);
		if (rc !== TACHYON_SUCCESS) throw new TachyonError(`Bus.send: commit failed (error ${rc}).`, mapError(rc));
		abi.flush(this.#bus);
	}

	public acquireTx(maxSize: number): Uint8Array {
		const ptr = abi.acquireTx(this.#bus, maxSize);
		if (ptr === 0) throw new TachyonError('Bus.acquireTx: the ring buffer is full.', ErrorCode.Full);
		return slot(ptr, maxSize);
	}

	public commitTx(actualSize: number, typeId: number): void {
		const rc = abi.commitTx(this.#bus, actualSize, typeId);
		if (rc !== TACHYON_SUCCESS) throw new TachyonError(`Bus.commitTx: commit failed (error ${rc}).`, mapError(rc));
		abi.flush(this.#bus);
	}

	public commitTxUnflushed(actualSize: number, typeId: number): void {
		const rc = abi.commitTx(this.#bus, actualSize, typeId);
		if (rc !== TACHYON_SUCCESS) {
			throw new TachyonError(`Bus.commitTxUnflushed: commit failed (error ${rc}).`, mapError(rc));
		}
	}

	public rollbackTx(): void {
		abi.rollbackTx(this.#bus);
	}

	public flush(): void {
		abi.flush(this.#bus);
	}

	public acquireRx(): RawRx<Uint8Array> | null {
		const ptr = abi.acquireRx(this.#bus, OUT_TYPE_ID, OUT_SIZE);
		if (ptr === 0) return null;
		const typeId = core.getValue(OUT_TYPE_ID, 'i32') >>> 0;
		const actualSize = core.getValue(OUT_SIZE, 'i32') >>> 0;
		return { data: slot(ptr, actualSize), typeId, actualSize };
	}

	public drainBatch(maxMsgs: number): RawBatchMessage<Uint8Array>[] {
		this.#batchBuffers = [];
		const messages: RawBatchMessage<Uint8Array>[] = [];
		for (let i = 0; i < maxMsgs; i += 1) {
			const result = this.acquireRx();
			if (result === null) break;

			// Copy out of the ring before committing so the slot can be reused.
			const data = new Uint8Array(result.data);
			this.#batchBuffers.push(data.buffer);
			messages.push({ data, typeId: result.typeId, size: result.actualSize });
			this.commitRx();
		}
		return messages;
	}

	public commitRx(): void {
		abi.commitRx(this.#bus);
	}

	public commitBatch(): void {
		for (const buffer of this.#batchBuffers) {
			detachArrayBuffer(buffer);
		}
		this.#batchBuffers = [];
	}

	public setPollingMode(spinMode: number): void {
		// Browser delivery is direct and non-blocking, but the core still tracks
		// the pure-spin hint, so forward it to keep parity with the native path.
		abi.setPollingMode(this.#bus, spinMode);
	}

	public setNumaNode(_nodeId: number): void {
		// WASM memory is page-local and cannot be NUMA-bound from browser JS.
	}

	public getState(): number {
		return abi.getState(this.#bus);
	}
}

/**
 * Creates a page-local ring through the C core and returns the raw bus pointer.
 *
 * @throws {TachyonError} If the core rejects the capacity or allocation fails.
 */
function listenBus(socketPath: string, capacity: number): number {
	core.setValue(OUT_BUS, 0, 'i32');
	const rc = abi.busListen(socketPath, capacity, OUT_BUS);
	if (rc !== TACHYON_SUCCESS) {
		throw new TachyonError(`Bus.listen: the core rejected the request (error ${rc}).`, mapError(rc));
	}

	const busPtr = core.getValue(OUT_BUS, 'i32');
	// tachyon_bus_get_shm_ptr exposes the arena base; a null base means the
	// allocation never mapped, so refuse to hand back an unusable bus.
	if (busPtr === 0 || abi.getShmPtr(busPtr) === 0) {
		throw new TachyonError('Bus.listen: the core returned an unmapped bus.', ErrorCode.Mem);
	}
	return busPtr;
}

// GC safety net: if a Bus is dropped without close(), free the underlying core
// bus. The held value is the handle (which never references the Bus), and the
// unregister token is also the handle, so there is no strong cycle back to the
// Bus instance and the registry can never pin it in memory.
const busRegistry = new FinalizationRegistry<BrowserBusHandle>((handle) => {
	handle.close();
});

/**
 * Browser implementation of the Tachyon SPSC bus.
 *
 * Bundlers resolve `@tachyon-ipc/core` to this entry through the package
 * `browser` export condition. The constructor shape matches Node:
 * `Bus.listen(path, capacity)` creates a page-local ring and
 * `Bus.connect(path)` attaches to it. Both share one `tachyon_bus_t`, so the
 * single fuzzed C++ ring is the only engine in play.
 */
export class Bus extends BusBase<Uint8Array> {
	readonly #handle: BrowserBusHandle;

	private constructor(path: string, endpoint: BrowserEndpoint) {
		const handle = new BrowserBusHandle(path, endpoint);
		super(handle, {
			defaultSpinThreshold: 0,
			retryNullRecv: false,
			copyData: (data) => new Uint8Array(data),
		});
		this.#handle = handle;
		busRegistry.register(this, handle, handle);
	}

	public override close(): void {
		busRegistry.unregister(this.#handle);
		super.close();
	}

	public static listen(socketPath: string, capacity: number): Bus {
		if (!Number.isInteger(capacity) || capacity <= 0) {
			throw new TachyonError('Bus.listen: capacity must be a positive integer.', ErrorCode.InvalidSz);
		}
		if (capacity > MAX_CAPACITY) {
			throw new TachyonError(
				`Bus.listen: capacity ${capacity} exceeds the 2GB limit for wasm32 builds.`,
				ErrorCode.InvalidSz,
			);
		}
		if (endpoints.has(socketPath)) {
			throw new Error(`Bus.listen: browser endpoint already exists for ${socketPath}`);
		}

		const busPtr = listenBus(socketPath, capacity);
		const endpoint: BrowserEndpoint = { busPtr, refs: 1 };
		endpoints.set(socketPath, endpoint);
		return new Bus(socketPath, endpoint);
	}

	public static connect(socketPath: string): Bus {
		const endpoint = endpoints.get(socketPath);
		if (endpoint === undefined) {
			throw new Error(`Bus.connect: no browser endpoint is listening at ${socketPath}`);
		}

		endpoint.refs += 1;
		return new Bus(socketPath, endpoint);
	}
}

export {
	TachyonError,
	AbiMismatchError,
	PeerDeadError,
	ErrorCode,
	isAbiMismatch,
	isFull,
	isTachyonError,
	isPeerDead,
} from './error.ts';
export type { ErrorCode as ErrorCodeType } from './error.ts';
export { RxBatch } from './batch.ts';
export type { RxMessage } from './batch.ts';
export { TxGuard, RxGuard } from './guards.ts';
export type { TxSlot, RxSlot } from './guards.ts';
export { makeTypeId, msgType, routeId } from './type_id.ts';
