# Tachyon Browser WASM Example

This example runs Tachyon inside a single browser page. Page JavaScript writes
binary payloads through the package Bus API into WebAssembly memory, a small
C++ WASM function (`tachyon_browser_echo_once`) polls the inbound ring and
replies on a second ring, and JavaScript reads the reply from WASM memory.

The demo program lives in `examples/browser_wasm/echo/echo.cpp`. It is compiled
by Emscripten and linked against the fuzzed Tachyon C++ core into a single WASM
module, so the page JavaScript and the C++ program share one WASM memory and one
ring engine. Both sides drive the rings through the exact same sanitized C ABI —
the C++ core is the single source of truth.

The browser build does not use POSIX shared memory or UNIX sockets. Those APIs
are unavailable in browsers, so the WASM path is a page-local Tachyon ring with
the same 64-byte message header, alignment, `type_id`, and skip-marker rules.

## Run

```bash
# from the repo root, make the Emscripten toolchain available:
source .emsdk/emsdk_env.sh     # run `bash ci/setup/install_emsdk.sh` first if needed

cd examples/browser_wasm
npm --prefix ../../bindings/js ci --ignore-scripts
npm --prefix ../../bindings/js run build:ts
npm ci --ignore-scripts
npm run build:wasm             # builds the C++ core + echo into pkg/tachyon_example.js
npm run dev
```

Open the Vite URL, then use **Send To C++** or **Run Browser RTT Bench**.

## Native Comparison

The Send To C++ button exercises the package wrapper bound to the demo module.
The benchmark separately measures direct C ABI calls, excluding wrapper copies and validation.

The browser benchmark reports batch-averaged round-trip time because
`performance.now()` is too coarse for individual sub-microsecond samples in many
browsers. Compare the browser mean/p50 against the native Tachyon benchmark
(`cmake --build --preset <preset>` then run the `benchmark/` target, or the
numbers in the root README) for a practical JS/WASM overhead view. The browser path is in-process and has no syscalls, so these measurements do not
measure cross-process latency or imply native-equivalent performance.
