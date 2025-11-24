#!/usr/bin/env node
import { DuckDBInstance } from '@duckdb/node-api';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { drizzle } from '../index.ts';
import { introspect } from '../introspect.ts';

interface CliOptions {
  url?: string;
  schemas?: string[];
  outFile: string;
  includeViews: boolean;
  useCustomTimeTypes: boolean;
  importBasePath?: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    outFile: path.resolve(process.cwd(), 'drizzle/schema.ts'),
    includeViews: false,
    useCustomTimeTypes: true,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    switch (arg) {
      case '--url':
        options.url = argv[++i];
        break;
      case '--schema':
      case '--schemas':
        options.schemas = argv[++i]?.split(',').map((s) => s.trim()).filter(Boolean);
        break;
      case '--out':
      case '--outFile':
        options.outFile = path.resolve(process.cwd(), argv[++i] ?? 'drizzle/schema.ts');
        break;
      case '--include-views':
      case '--includeViews':
        options.includeViews = true;
        break;
      case '--use-pg-time':
        options.useCustomTimeTypes = false;
        break;
      case '--import-base':
        options.importBasePath = argv[++i];
        break;
      case '--help':
      case '-h':
        printHelp();
        process.exit(0);
      default:
        if (arg.startsWith('-')) {
          console.warn(`Unknown option ${arg}`);
        }
    }
  }

  return options;
}

function printHelp(): void {
  console.log(`duckdb-introspect

Usage:
  bun x duckdb-introspect --url <duckdb path|md:> [--schema my_schema] [--out ./drizzle/schema.ts]

Options:
  --url            DuckDB database path (e.g. :memory:, ./local.duckdb, md:)
  --schema         Comma separated schema list (defaults to all non-system schemas)
  --out            Output file (default: ./drizzle/schema.ts)
  --include-views  Include views in the generated schema
  --use-pg-time    Use pg-core timestamp/date/time instead of DuckDB custom helpers
  --import-base    Override import path for duckdb helpers (default: package name)
`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!options.url) {
    printHelp();
    throw new Error('Missing required --url');
  }

  const instanceOptions =
    options.url.startsWith('md:') && process.env.MOTHERDUCK_TOKEN
      ? { motherduck_token: process.env.MOTHERDUCK_TOKEN }
      : undefined;

  const instance = await DuckDBInstance.create(options.url, instanceOptions);
  const connection = await instance.connect();
  const db = drizzle(connection);

  try {
    const result = await introspect(db, {
      schemas: options.schemas,
      includeViews: options.includeViews,
      useCustomTimeTypes: options.useCustomTimeTypes,
      importBasePath: options.importBasePath,
    });

    await mkdir(path.dirname(options.outFile), { recursive: true });
    await writeFile(options.outFile, result.files.schemaTs, 'utf8');

    console.log(`Wrote schema to ${options.outFile}`);
  } finally {
    if ('closeSync' in connection && typeof connection.closeSync === 'function') {
      connection.closeSync();
    }
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
