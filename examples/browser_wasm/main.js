import createTachyonExample from "./pkg/tachyon_example.js";

const CAPACITY = 1 << 20;
const BATCH_SIZE = 4096;

const els = {
  status: document.querySelector("#wasm-status"),
  capacity: document.querySelector("#capacity"),
  lastReply: document.querySelector("#last-reply"),
  value: document.querySelector("#value"),
  send: document.querySelector("#send"),
  iterations: document.querySelector("#iterations"),
  bench: document.querySelector("#bench"),
  log: document.querySelector("#log"),
  benchTable: document.querySelector("#bench-table"),
};

let core;
let abi;
let view;
let scratch;
let jsToCpp;
let cppToJs;
let typeCounter;

function makeTypeId(route, msgType) {
  return ((route & 0xffff) << 16) | (msgType & 0xffff);
}

function routeId(typeId) {
  return (typeId >>> 16) & 0xffff;
}

function msgType(typeId) {
  return typeId & 0xffff;
}

function appendLog(line) {
  const time = new Date().toLocaleTimeString();
  els.log.textContent = `[${time}] ${line}\n${els.log.textContent}`;
}

function listenBus(path) {
  core.setValue(scratch, 0, "i32");
  const rc = abi.busListen(path, CAPACITY, scratch);
  if (rc !== 0) throw new Error(`tachyon_bus_listen failed for ${path} (error ${rc})`);
  const busPtr = core.getValue(scratch, "i32");
  if (busPtr === 0) throw new Error(`tachyon_bus_listen returned null for ${path}`);
  return busPtr;
}

// Two page-local rings: JS produces into jsToCpp, the C++ echo consumes it and
// produces into cppToJs, then JS consumes the reply.
function writeU32ToBus(bus, value, typeId) {
  const ptr = abi.acquireTx(bus, 4);
  if (ptr === 0) throw new Error("ring full");
  view.setUint32(ptr, value >>> 0, true);
  abi.commitTx(bus, 4, typeId);
  abi.flush(bus);
}

function readU32FromBus(bus) {
  const ptr = abi.acquireRx(bus, scratch, scratch + 4);
  if (ptr === 0) return null;
  const typeId = core.getValue(scratch, "i32") >>> 0;
  const size = core.getValue(scratch + 4, "i32") >>> 0;
  const value = size === 4 ? view.getUint32(ptr, true) : null;
  abi.commitRx(bus);
  return { value, size, typeId };
}

function pingCpp(value) {
  writeU32ToBus(jsToCpp, value, typeCounter);

  if (abi.echoOnce(jsToCpp, cppToJs) !== 1) {
    throw new Error("C++ WASM program did not receive the JS message");
  }

  const reply = readU32FromBus(cppToJs);
  if (!reply) {
    throw new Error("JS did not receive the C++ WASM reply");
  }

  return reply;
}

function pingCppFast(value) {
  const txPtr = abi.acquireTx(jsToCpp, 4);
  view.setUint32(txPtr, value >>> 0, true);
  abi.commitTx(jsToCpp, 4, typeCounter);
  abi.flush(jsToCpp);
  if (abi.echoOnce(jsToCpp, cppToJs) !== 1) {
    throw new Error("C++ WASM program did not receive the JS message");
  }
  const rxPtr = abi.acquireRx(cppToJs, scratch, scratch + 4);
  if (rxPtr === 0) {
    throw new Error("JS did not receive the C++ WASM reply");
  }
  const replyValue = view.getUint32(rxPtr, true);
  abi.commitRx(cppToJs);
  return replyValue;
}

function percentile(sorted, pct) {
  const idx = Math.min(
    sorted.length - 1,
    Math.floor((sorted.length - 1) * pct),
  );
  return sorted[idx];
}

function formatNs(ns) {
  if (ns >= 1000) return `${(ns / 1000).toFixed(2)} us`;
  return `${ns.toFixed(1)} ns`;
}

function setBenchRows(rows) {
  els.benchTable.replaceChildren(
    ...rows.map(([label, value]) => {
      const tr = document.createElement("tr");
      const left = document.createElement("td");
      const right = document.createElement("td");
      left.textContent = label;
      right.textContent = value;
      tr.append(left, right);
      return tr;
    }),
  );
}

async function runBench() {
  const iterations = Math.max(
    1000,
    Number.parseInt(els.iterations.value, 10) || 1000000,
  );
  const warmup = Math.min(10000, Math.floor(iterations / 10));

  els.bench.disabled = true;
  setBenchRows([["Running", `${iterations.toLocaleString()} RTTs`]]);
  await new Promise((resolve) => requestAnimationFrame(resolve));

  for (let i = 0; i < warmup; i += 1) {
    pingCpp(i);
  }

  const samples = [];
  let totalStart = performance.now();
  for (let i = 0; i < iterations; i += BATCH_SIZE) {
    const batchCount = Math.min(BATCH_SIZE, iterations - i);
    const batchStart = performance.now();
    for (let j = 0; j < batchCount; j += 1) {
      pingCppFast(i + j);
    }
    samples.push(((performance.now() - batchStart) * 1_000_000) / batchCount);
  }
  const totalMs = performance.now() - totalStart;

  samples.sort((a, b) => a - b);
  const throughput = iterations / (totalMs / 1000);
  setBenchRows([
    ["Payload", "4 bytes u32"],
    [
      "Samples",
      `${samples.length.toLocaleString()} batch averages x ${BATCH_SIZE}`,
    ],
    ["Direct doorbell p50", formatNs(percentile(samples, 0.5))],
    ["Direct doorbell p90", formatNs(percentile(samples, 0.9))],
    ["Direct doorbell p99", formatNs(percentile(samples, 0.99))],
    ["Direct doorbell mean", formatNs((totalMs * 1_000_000) / iterations)],
    ["Throughput", `${(throughput / 1000).toFixed(1)} K RTT/sec`],
  ]);
  appendLog(
    `browser bench completed: ${(throughput / 1000).toFixed(1)} K RTT/sec`,
  );
  els.bench.disabled = false;
}

async function main() {
  core = await createTachyonExample();
  abi = {
    busListen: core.cwrap("tachyon_bus_listen", "number", ["string", "number", "number"]),
    acquireTx: core.cwrap("tachyon_acquire_tx", "number", ["number", "number"]),
    commitTx: core.cwrap("tachyon_commit_tx", "number", ["number", "number", "number"]),
    flush: core.cwrap("tachyon_flush", null, ["number"]),
    acquireRx: core.cwrap("tachyon_acquire_rx", "number", ["number", "number", "number"]),
    commitRx: core.cwrap("tachyon_commit_rx", "number", ["number"]),
    echoOnce: core.cwrap("tachyon_browser_echo_once", "number", ["number", "number"]),
  };
  // 16-byte scratch for C out-parameters (out_bus / out_type_id / out_size).
  scratch = core._malloc(16);

  typeCounter = makeTypeId(0, 7);
  jsToCpp = listenBus("/example/js-to-cpp");
  cppToJs = listenBus("/example/cpp-to-js");
  view = new DataView(core.HEAPU8.buffer);

  els.status.textContent = "ready";
  els.capacity.textContent = `${CAPACITY / 1024} KiB x 2`;
  els.send.disabled = false;
  els.bench.disabled = false;

  els.send.addEventListener("click", () => {
    const value = Number.parseInt(els.value.value, 10) >>> 0;
    const reply = pingCpp(value);
    els.lastReply.textContent = `${reply.value}`;
    appendLog(
      `JS sent ${value}, C++ replied ${reply.value}; route=${routeId(reply.typeId)} type=${msgType(
        reply.typeId,
      )}`,
    );
  });

  els.bench.addEventListener("click", () => {
    runBench().catch((err) => {
      appendLog(`bench failed: ${err.message}`);
      els.bench.disabled = false;
    });
  });

  appendLog("WASM module initialized");
}

main().catch((err) => {
  els.status.textContent = "failed";
  appendLog(err.stack || err.message);
});
