#!/usr/bin/env bun
import { execFile } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { cpus } from 'node:os';
import process from 'node:process';
import { join } from 'node:path';
import { asc, avg, eq, sql, sum } from 'drizzle-orm';
import { Bench } from 'tinybench';
import { closePerfHarness, createPerfHarness } from '../test/perf/setup.ts';
import type { AnyPerfHarness } from '../test/perf/setup.ts';
import {
  benchComplex,
  benchInsert,
  benchPrepared,
  factLarge,
  narrowWide,
} from '../test/perf/schema.ts';
import { olap, sumN } from '../src/olap.ts';
import type { RewriteArraysMode } from '../src/options.ts';

type CliOptions = {
  ghaOutput?: string;
  repeat: number;
  pooled: boolean;
  poolSize: number;
  rewriteArrays: RewriteArraysMode;
  rawStream: boolean;
};

function parseArgs(argv: string[]): CliOptions {
  const opts: CliOptions = {
    repeat: 1,
    pooled: false,
    poolSize: 4,
    rewriteArrays: 'auto',
    rawStream: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--gha-output') {
      opts.ghaOutput = argv[++i];
    } else if (arg === '--repeat') {
      const next = argv[++i];
      opts.repeat = next ? Number.parseInt(next, 10) : 1;
    } else if (arg === '--pooled') {
      opts.pooled = true;
    } else if (arg === '--pool-size') {
      const next = argv[++i];
      opts.poolSize = next ? Number.parseInt(next, 10) : 4;
    } else if (arg === '--rewrite-arrays') {
      const next = argv[++i];
      if (next === 'auto' || next === 'always' || next === 'never') {
        opts.rewriteArrays = next;
      }
    } else if (arg === '--raw-stream') {
      opts.rawStream = true;
    }
  }
  return opts;
}

function percentile(samples: number[], p: number): number {
  if (samples.length === 0) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  if (lower === upper) {
    return sorted[lower] as number;
  }
  const weight = idx - lower;
  return sorted[lower]! * (1 - weight) + sorted[upper]! * weight;
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  let harness: AnyPerfHarness | undefined;
  let memStart: NodeJS.MemoryUsage | undefined;
  let memEnd: NodeJS.MemoryUsage | undefined;
  try {
    harness = await createPerfHarness({
      pooled: opts.pooled,
      poolSize: opts.poolSize,
      rewriteArrays: opts.rewriteArrays,
    });
    const modeLabel =
      harness.mode === 'pooled'
        ? `pooled (size=${(harness as { poolSize: number }).poolSize})`
        : 'single';
    console.log(`Running benchmarks in ${modeLabel} mode...`);
    memStart = process.memoryUsage();
    const insertBatch = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      val: `payload-${i}`,
    }));

    const preparedSelect = harness.db
      .select({ id: benchPrepared.id, val: benchPrepared.val })
      .from(benchPrepared)
      .where(eq(benchPrepared.id, sql.placeholder('id')))
      .prepare('perf_prepared_select');

    const runIteration = async () => {
      const bench = new Bench({ time: 700, warmupTime: 200 });
      let mixedId = 1000;

      bench.add('select-constant (builder)', async () => {
        await harness!.db
          .select({ one: factLarge.id })
          .from(factLarge)
          .limit(1);
      });

      bench.add('where + param (builder)', async () => {
        await harness!.db
          .select({ id: factLarge.id })
          .from(factLarge)
          .where(eq(factLarge.id, 42))
          .limit(1);
      });

      bench.add('scan fact_large (builder)', async () => {
        await harness!.db.select().from(factLarge);
      });

      bench.add('aggregation fact_large (builder)', async () => {
        await harness!.db
          .select({
            mod100: factLarge.mod100,
            avgValue: avg(factLarge.value),
            sumValue: sum(factLarge.value),
          })
          .from(factLarge)
          .groupBy(factLarge.mod100)
          .orderBy(asc(factLarge.mod100));
      });

      bench.add('olap builder', async () => {
        await olap(harness!.db)
          .from(factLarge)
          .groupBy([factLarge.cat])
          .selectNonAggregates(
            { anyPayload: factLarge.payload },
            { anyValue: true }
          )
          .measures({
            total: sumN(factLarge.value),
            avgValue: avg(factLarge.value),
          })
          .orderBy(asc(factLarge.cat))
          .run();
      });

      bench.add('wide row materialization', async () => {
        await harness!.db.select().from(narrowWide);
      });

      bench.add('insert batch (builder)', async () => {
        await harness!.db.delete(benchInsert);
        await harness!.db.insert(benchInsert).values(insertBatch);
      });

      bench.add('insert batch returning', async () => {
        await harness!.db.delete(benchInsert);
        await harness!.db.insert(benchInsert).values(insertBatch).returning();
      });

      bench.add('upsert do update', async () => {
        await harness!.db.delete(benchInsert);
        await harness!.db.insert(benchInsert).values([{ id: 1, val: 'seed' }]);
        await harness!.db
          .insert(benchInsert)
          .values([{ id: 1, val: 'updated' }])
          .onConflictDoUpdate({
            target: benchInsert.id,
            set: { val: sql`excluded.val` },
          });
      });

      bench.add('upsert do nothing', async () => {
        await harness!.db.delete(benchInsert);
        await harness!.db.insert(benchInsert).values([{ id: 1, val: 'seed' }]);
        await harness!.db
          .insert(benchInsert)
          .values([{ id: 1, val: 'dup' }])
          .onConflictDoNothing({ target: benchInsert.id });
      });

      bench.add('transaction insert+select', async () => {
        await harness!.db.transaction(async (tx) => {
          await tx.delete(benchInsert);
          await tx.insert(benchInsert).values(insertBatch.slice(0, 20));
          await tx.select().from(benchInsert).limit(5);
        });
      });

      bench.add('prepared select reuse', async () => {
        await preparedSelect.execute({ id: 10 });
      });

      bench.add('mixed workload (select + insert)', async () => {
        await harness!.db
          .select()
          .from(factLarge)
          .orderBy(asc(factLarge.id))
          .limit(20);
        await harness!.db
          .insert(benchInsert)
          .values([{ id: mixedId++, val: 'mix' }]);
      });

      bench.add('complex param mapping', async () => {
        await harness!.db.delete(benchComplex);
        await harness!.db.insert(benchComplex).values({
          id: 1,
          meta: { version: 2, tag: 'alpha' },
          attrs: { region: 'us-east', env: 'dev' },
          nums: [1, 2, 3, 4],
          tags: ['a', 'b', 'c'],
        });
        await harness!.db
          .select()
          .from(benchComplex)
          .where(eq(benchComplex.id, 1))
          .limit(1);
      });

      bench.add('stream-batches', async () => {
        let total = 0;
        for await (const chunk of harness!.db.executeBatches(
          sql`select * from ${factLarge}`,
          { rowsPerChunk: 10000 }
        )) {
          total += chunk.length;
        }
        if (total !== 100000) {
          throw new Error(`expected 100000 rows, saw ${total}`);
        }
      });

      if (opts.rawStream) {
        bench.add('stream-batches-raw', async () => {
          let total = 0;
          for await (const chunk of harness!.db.executeBatchesRaw(
            sql`select * from ${factLarge}`,
            { rowsPerChunk: 10000 }
          )) {
            total += chunk.rows.length;
          }
          if (total !== 100000) {
            throw new Error(`expected 100000 rows, saw ${total}`);
          }
        });
      }

      bench.add('arrow-fetch', async () => {
        await harness!.db.executeArrow(sql`select * from ${factLarge}`);
      });

      // Parallel benchmarks - only run in pooled mode because DuckDB's Node API
      // has intermittent failures with concurrent parameterized queries on a
      // single connection. The pool provides separate connections per query.
      if (harness!.mode === 'pooled') {
        bench.add('parallel select x4', async () => {
          await Promise.all([
            harness!.db
              .select({ id: factLarge.id })
              .from(factLarge)
              .where(eq(factLarge.id, 1))
              .limit(1),
            harness!.db
              .select({ id: factLarge.id })
              .from(factLarge)
              .where(eq(factLarge.id, 2))
              .limit(1),
            harness!.db
              .select({ id: factLarge.id })
              .from(factLarge)
              .where(eq(factLarge.id, 3))
              .limit(1),
            harness!.db
              .select({ id: factLarge.id })
              .from(factLarge)
              .where(eq(factLarge.id, 4))
              .limit(1),
          ]);
        });

        bench.add('parallel select x8', async () => {
          await Promise.all(
            Array.from({ length: 8 }, (_, i) =>
              harness!.db
                .select({ id: factLarge.id })
                .from(factLarge)
                .where(eq(factLarge.id, i))
                .limit(1)
            )
          );
        });

        bench.add('parallel aggregation x4', async () => {
          await Promise.all([
            harness!.db
              .select({ avgValue: avg(factLarge.value) })
              .from(factLarge)
              .where(eq(factLarge.mod100, 0)),
            harness!.db
              .select({ avgValue: avg(factLarge.value) })
              .from(factLarge)
              .where(eq(factLarge.mod100, 25)),
            harness!.db
              .select({ avgValue: avg(factLarge.value) })
              .from(factLarge)
              .where(eq(factLarge.mod100, 50)),
            harness!.db
              .select({ avgValue: avg(factLarge.value) })
              .from(factLarge)
              .where(eq(factLarge.mod100, 75)),
          ]);
        });

        bench.add('parallel mixed read/write x4', async () => {
          const base = mixedId;
          mixedId += 4;
          await Promise.all([
            harness!.db
              .select()
              .from(factLarge)
              .where(eq(factLarge.id, 100))
              .limit(1),
            harness!.db
              .select()
              .from(factLarge)
              .where(eq(factLarge.id, 200))
              .limit(1),
            harness!.db.insert(benchInsert).values([{ id: base, val: 'p1' }]),
            harness!.db
              .insert(benchInsert)
              .values([{ id: base + 1, val: 'p2' }]),
          ]);
        });

        bench.add('parallel wide scan x2', async () => {
          await Promise.all([
            harness!.db.select().from(narrowWide),
            harness!.db.select().from(narrowWide),
          ]);
        });
      }

      await bench.warmup();
      await bench.run();

      return bench.tasks.map((task) => {
        const samples = task.result?.samples ?? [];
        return {
          name: task.name,
          hz: task.result?.hz ?? 0,
          mean: task.result?.mean ?? 0,
          sd: task.result?.sd ?? 0,
          samples: samples.length,
          rme: task.result?.rme ?? 0,
          p50: percentile(samples, 50),
          p95: percentile(samples, 95),
        };
      });
    };

    const aggregate = new Map<
      string,
      {
        hz: number;
        mean: number;
        sd: number;
        samples: number;
        rme: number;
        p50: number;
        p95: number;
      }
    >();

    for (let i = 0; i < opts.repeat; i++) {
      const iterResults = await runIteration();
      for (const r of iterResults) {
        const prev = aggregate.get(r.name) ?? {
          hz: 0,
          mean: 0,
          sd: 0,
          samples: 0,
          rme: 0,
          p50: 0,
          p95: 0,
        };
        aggregate.set(r.name, {
          hz: prev.hz + r.hz,
          mean: prev.mean + r.mean,
          sd: prev.sd + r.sd,
          samples: prev.samples + r.samples,
          rme: prev.rme + r.rme,
          p50: prev.p50 + r.p50,
          p95: prev.p95 + r.p95,
        });
      }
    }

    const results = Array.from(aggregate.entries()).map(([name, sums]) => ({
      name,
      hz: sums.hz / opts.repeat,
      mean: sums.mean / opts.repeat,
      sd: sums.sd / opts.repeat,
      samples: Math.round(sums.samples / opts.repeat),
      rme: sums.rme / opts.repeat,
      p50: sums.p50 / opts.repeat,
      p95: sums.p95 / opts.repeat,
    }));

    memEnd = process.memoryUsage();

    const meta = {
      gitSha: await gitSha(),
      timestamp: new Date().toISOString(),
      node: process.version,
      bun: process.versions.bun,
      platform: process.platform,
      arch: process.arch,
      cpu: cpus()[0]?.model ?? 'unknown',
      repeat: opts.repeat,
      mode: harness.mode,
      poolSize:
        harness.mode === 'pooled'
          ? (harness as { poolSize: number }).poolSize
          : undefined,
      rewriteArrays: opts.rewriteArrays,
      rawStream: opts.rawStream,
      memory:
        memStart && memEnd
          ? {
              start: { heapUsed: memStart.heapUsed, rss: memStart.rss },
              end: { heapUsed: memEnd.heapUsed, rss: memEnd.rss },
            }
          : undefined,
    };

    const payload = { meta, results };

    const outDir = 'perf-results';
    await mkdir(outDir, { recursive: true });
    const modeSuffix =
      harness.mode === 'pooled'
        ? `-pool${(harness as { poolSize: number }).poolSize}`
        : '-single';
    const outFile = join(
      outDir,
      `${meta.timestamp.replace(/[:.]/g, '-')}-${meta.gitSha}${modeSuffix}.json`
    );
    await writeFile(outFile, JSON.stringify(payload, null, 2));
    console.log(`perf results -> ${outFile}`);

    if (opts.ghaOutput) {
      const ghaShape = results.map((r) => ({
        name: r.name,
        unit: 'ops/s',
        value: r.hz,
        range: r.sd,
      }));
      await writeFile(opts.ghaOutput, JSON.stringify(ghaShape, null, 2));
      console.log(`gha benchmark output -> ${opts.ghaOutput}`);
    }
  } finally {
    if (harness) {
      await closePerfHarness(harness);
    }
  }
}

async function gitSha(): Promise<string> {
  try {
    const { stdout } = await execFileAsync('git', [
      'rev-parse',
      '--short',
      'HEAD',
    ]);
    return stdout.trim();
  } catch {
    return 'unknown';
  }
}

function execFileAsync(
  cmd: string,
  args: string[]
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, (error, stdout, stderr) => {
      if (error) {
        reject(error);
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
