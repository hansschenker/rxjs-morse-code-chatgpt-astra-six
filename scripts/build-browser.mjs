import { build } from 'esbuild';
import { readFile, writeFile } from 'node:fs/promises';

await build({
  entryPoints: ['src/browser.ts'], bundle: true, format: 'iife',
  minify: true, legalComments: 'inline', outfile: 'browser/app.js',
});

const template = await readFile('browser/index.html', 'utf8');
const bundle = await readFile('browser/app.js', 'utf8');
const notices = await readFile('THIRD_PARTY_NOTICES.txt', 'utf8');
// Escape closing script tags in both executable code and embedded license data.
const script = `<script>${bundle.replace(/<\/script/gi, '<\\/script')}</script>`;
const licenses = `<script type="application/json" id="third-party-notices">${JSON.stringify(notices).replace(/</g, '\\u003c')}</script>`;
await writeFile('morse-code.html', template.replace('<script src="app.js"></script>', script + '\n' + licenses));
console.log('Built browser/app.js and standalone morse-code.html');
