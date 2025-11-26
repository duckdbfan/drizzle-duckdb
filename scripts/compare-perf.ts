#!/usr/bin/env bun
import { readFile } from 'node:fs/promises';
import process from 'node:process';

type Result = {
  name: string;
  hz: number;
  mean: number;
  sd: number;
  samples: number;
  rme?: number;
};

type PerfFile = {
  meta?: Record<string, unknown>;
  results: Result[];
};

const threshold = Number.parseFloat(process.env.THRESHOLD ?? '5');

async function main(): Promise<void> {
  const [oldPath, newPath] = process.argv.slice(2);
  if (!oldPath || !newPath) {
    console.error(
      'usage: bun run scripts/compare-perf.ts <old.json> <new.json>'
    );
    process.exit(1);
  }

  const oldFile = await load(oldPath);
  const newFile = await load(newPath);

  const oldMap = toMap(oldFile.results);
  const newMap = toMap(newFile.results);

  const names = new Set([...oldMap.keys(), ...newMap.keys()]);

  for (const name of names) {
    const oldRes = oldMap.get(name);
    const newRes = newMap.get(name);

    if (!oldRes || !newRes) {
      console.log(`${name}: missing in ${oldRes ? 'new' : 'old'} run`);
      continue;
    }

    const delta = percentChange(oldRes.hz, newRes.hz);
    const direction = delta >= 0 ? 'faster' : 'slower';
    const flag = Math.abs(delta) >= threshold ? ' ⚠️ regression risk' : '';

    console.log(
      `${name}: ${oldRes.hz.toFixed(2)} -> ${newRes.hz.toFixed(2)} ops/sec (${delta.toFixed(2)}% ${direction})${flag}`
    );
  }
}

async function load(path: string): Promise<PerfFile> {
  const data = await readFile(path, 'utf8');
  return JSON.parse(data) as PerfFile;
}

function toMap(results: Result[]): Map<string, Result> {
  return new Map(results.map((r) => [r.name, r]));
}

function percentChange(oldHz: number, newHz: number): number {
  if (oldHz === 0) return 0;
  return ((newHz - oldHz) / oldHz) * 100;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
