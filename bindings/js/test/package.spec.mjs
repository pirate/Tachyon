import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fixture = mkdtempSync(join(tmpdir(), 'tachyon-package-'));
function run(command, args, options = {}) {
	const result = spawnSync(command, args, { cwd: fixture, encoding: 'utf8', ...options });
	assert.equal(result.status, 0, `${command} failed:\n${result.stdout}\n${result.stderr}`);
	return result.stdout;
}
try {
	const [pack] = JSON.parse(
		run('npm', ['pack', '--ignore-scripts', '--json', '--pack-destination', fixture], { cwd: root }),
	);
	for (const name of [
		'browser.js',
		'browser_bindings.d.ts',
		'wasm/tachyon.js',
		'wasm/tachyon.wasm',
		'wasm/tachyon.d.ts',
	]) {
		assert.ok(
			pack.files.some(({ path }) => path === `dist/${name}`),
			`Missing ${name}`,
		);
	}
	assert.ok(pack.files.every(({ path }) => !path.startsWith('build')));
	run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', join(fixture, pack.filename)]);
	writeFileSync(
		join(fixture, 'check.ts'),
		`
import {Bus, type TxGuard} from '@tachyon-ipc/core/browser';
import {createBrowserBindings} from '@tachyon-ipc/core/browser/bindings';
const bus = Bus.listen('typed', 1024);
const tx: TxGuard = bus.acquireTx(4);
const bytes: Uint8Array = tx.bytes();
// @ts-expect-error Browser views do not have Buffer methods.
bytes.writeUInt32LE(1);
const result = bus.recv();
if (result) result.data.set([1]);
void createBrowserBindings;
`,
	);
	writeFileSync(
		join(fixture, 'tsconfig.json'),
		JSON.stringify({
			compilerOptions: {
				target: 'ES2022',
				module: 'NodeNext',
				moduleResolution: 'NodeNext',
				strict: true,
				noEmit: true,
				types: [],
				lib: ['ES2022', 'DOM', 'ESNext.Disposable'],
			},
			include: ['check.ts'],
		}),
	);
	run(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '-p', fixture]);
	process.stdout.write(
		run(process.execPath, [join(root, 'test/browser_wasm.spec.mjs')], {
			env: {
				...process.env,
				TACHYON_PACKAGE_ROOT: join(fixture, 'node_modules/@tachyon-ipc/core'),
			},
		}),
	);
	console.log('ok - packed browser assets and TypeScript consumer without Node types');
} finally {
	rmSync(fixture, { recursive: true, force: true });
}
