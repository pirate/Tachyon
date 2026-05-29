// Builds the example's single WASM module: the page-specific C++ echo program
// linked against the fuzzed Tachyon core. Both the C ABI (used by the page JS)
// and `tachyon_browser_echo_once` (the demo program) are exported from one
// module, so JavaScript and C++ share one WASM memory and one ring engine.
//
// Requires the Emscripten SDK on PATH (`source .emsdk/emsdk_env.sh`).

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(HERE, '../..');
const CORE_LIB = resolve(REPO_ROOT, 'build/emscripten-release/core/libtachyon.a');
const OUT_DIR = resolve(HERE, 'pkg');

function run(cmd, args, opts = {}) {
	const result = spawnSync(cmd, args, { stdio: 'inherit', ...opts });
	if (result.status !== 0) {
		console.error(`\n[build-wasm] command failed: ${cmd} ${args.join(' ')}`);
		process.exit(result.status ?? 1);
	}
}

// 1. Build the core static library with the Emscripten toolchain (cached by CMake).
if (!existsSync(CORE_LIB)) {
	run('cmake', ['--preset', 'emscripten-release'], { cwd: REPO_ROOT });
	run('cmake', ['--build', '--preset', 'emscripten-release'], { cwd: REPO_ROOT });
}

// 2. Link the demo program against the whole core archive into one ES module.
mkdirSync(OUT_DIR, { recursive: true });
run('em++', [
	'-O3',
	'-std=c++23',
	resolve(HERE, 'echo/echo.cpp'),
	'-I',
	resolve(REPO_ROOT, 'core/include'),
	'-Wl,--whole-archive',
	CORE_LIB,
	'-Wl,--no-whole-archive',
	'-sALLOW_MEMORY_GROWTH=1',
	'-sMODULARIZE=1',
	'-sEXPORT_ES6=1',
	'-sEXPORT_NAME=TachyonExample',
	'-sENVIRONMENT=web,worker',
	'-sSTRICT=1',
	'-sNO_EXIT_RUNTIME=1',
	'-sWASM_BIGINT=1',
	'-sFILESYSTEM=0',
	'--no-entry',
	'--minify=0',
	'-sEXPORTED_FUNCTIONS=_malloc,_free,_tachyon_browser_echo_once',
	'-sEXPORTED_RUNTIME_METHODS=cwrap,getValue,setValue,UTF8ToString,HEAPU8,HEAPU32',
	'-o',
	resolve(OUT_DIR, 'tachyon_example.js'),
]);

console.log('[build-wasm] wrote pkg/tachyon_example.js + pkg/tachyon_example.wasm');
