#!/usr/bin/env bun
import { readFile } from 'node:fs/promises';
import { createWriteStream, promises as fs } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Light-weight plotting using chart.js via quickchart.io API would need network;
// instead emit a simple CSV so users can plot with their tool of choice.

async function main(): Promise<void> {
  const dir = 'perf-results';
  const args = process.argv.slice(2);
  let modeFilter: string | undefined;
  for (let i = 0; i < args.length; i++) {
    const a = args[i]!;
    if (a === '--mode') {
      modeFilter = args[i + 1];
      i += 1;
    } else if (a === '--single') {
      modeFilter = 'single';
    } else if (a === '--pooled') {
      modeFilter = 'pooled';
    }
  }

  const files = (await fs.readdir(dir))
    .filter((f) => f.endsWith('.json'))
    .sort();
  if (files.length === 0) {
    console.error('no perf results found');
    process.exit(1);
  }

  const metrics = [
    'select-constant (builder)',
    'where + param (builder)',
    'aggregation fact_large (builder)',
    'insert batch (builder)',
    'prepared select reuse',
    'stream-batches',
  ];

  const rows: string[] = [];
  rows.push(['file', 'timestamp', 'mode', ...metrics].join(','));

  // Keep only runs from December 2025 onward to focus on recent perf changes.
  const minDate = new Date('2025-12-01T00:00:00Z');

  for (const file of files) {
    const path = join(dir, file);
    const data = JSON.parse(await readFile(path, 'utf8')) as {
      meta?: { timestamp?: string; mode?: string };
      results?: Array<{ name: string; hz: number }>;
    };
    const ts = data.meta?.timestamp ?? '';
    if (ts) {
      const date = new Date(ts);
      if (Number.isFinite(date.getTime()) && date < minDate) {
        continue;
      }
    }
    const mode = data.meta?.mode ?? 'single';
    if (modeFilter && mode !== modeFilter) {
      continue;
    }
    const map = new Map<string, number>();
    for (const r of data.results ?? []) {
      map.set(r.name, r.hz);
    }
    const line = [file, ts, mode, ...metrics.map((m) => map.get(m) ?? '')];
    rows.push(line.join(','));
  }

  const suffix = modeFilter ? `-${modeFilter}` : '';
  const outPath = join(dir, `perf-evolution${suffix}.csv`);
  await fs.writeFile(outPath, rows.join('\n'));
  console.log(`wrote ${outPath}`);
  console.log(
    'Load this CSV into your plotting tool (Sheets/Numbers/Excel/gnuplot) and chart ops/sec over time.'
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
