# Repository Guidelines

## Project Structure & Modules

- Source lives in `src/` (`driver.ts`, `session.ts`, `dialect.ts`, `columns.ts`, `migrator.ts`, `utils.ts`) with public re-exports from `src/index.ts`.
- Build artifacts (`dist/*.mjs` + `dist/*.d.ts`) are generated—never hand-edit them. `bun.lockb` and `node_modules/` are managed by Bun.
- Tests live in `test/`; `test/duckdb.test.ts` mirrors upstream Drizzle Postgres coverage and is long—add narrowly scoped specs in new files (e.g., `test/<feature>.test.ts`) unless you must touch the big suite. Migration snapshots for integration runs sit in `test/drizzle2/pg` and DuckDB-specific migrations in `test/duckdb/pg` (`meta/` holds the journals).

## Build, Test, and Development

- Use Bun: `bun install`, `bun run build` (bundles `dist/index.mjs` and then emits declarations), `bun run build:declarations`, `bun test`, `bun run t`.
- ESM only: `moduleResolution` is `bundler` and imports include `.ts` extensions. Keep relative paths explicit and prefer `import type` to avoid pulling runtime types.
- Do not introduce JSON/JSONB column types—the dialect explicitly rejects them.

## Coding Style & Patterns

- 2-space indentation, trailing commas on multi-line literals, named exports over defaults. Keep helpers camelCase and classes PascalCase.
- Collapse re-exports in `index.ts` and stick to modern syntax (`??`, optional chaining, etc.). Avoid `any` unless DuckDB bindings truly lack types.
- Document DuckDB-vs-Postgres behavior inline (e.g., aliasing or result mapping quirks in `utils.ts` and `DuckDBSelectBuilder`).

## DuckDB Runtime Notes

- Preferred client is `@duckdb/node-api@1.4.2-r.1` (used by tests). Stick to `DuckDBInstance.create(':memory:')` (or `DuckDBConnection.create`) for hermetic runs.
- Clean up connections with `closeSync`/`close`/`disconnectSync` and avoid leaving `.duckdb` files in the repo.
- Custom column helpers live in `columns.ts` (`duckDbStruct`, `duckDbMap`, `duckDbBlob`); JSON-like structures should use these or Drizzle custom types rather than native JSON columns.

## Testing Guidelines

- Vitest only; share utilities via `test/utils.ts`. When exercising migrations, mirror the layout under `test/drizzle2/pg/meta` or `test/duckdb/pg/meta` and keep snapshots in sync.
- The large `test/duckdb.test.ts` sets up sequences and schemas in `beforeAll`/`beforeEach`; follow that pattern (or create fresh tables in new files) to avoid cross-test bleed.
- Regression tests should cover DuckDB-specific branches (aliasing, selection mapping, transaction handling, migrator behavior).
- Perf benchmarks: use `bun x vitest bench --run test/perf --pool=threads --poolOptions.threads.singleThread=true --no-file-parallelism`. Add `--outputJson perf-results/latest.json` if you need an artifact. Vitest 1.6 rejects the older `--runInBand` flag.

## Commit & Pull Request Guidelines

- Use short, imperative subjects under 72 chars (e.g., “Add migrator to exports”, “Bump version to 0.0.7…”). Include a body when documenting workarounds or DuckDB quirks, and reference DuckDB tickets inline.
- PRs should link issues, summarize behavior changes, call out schema/migration updates, and attach `bun test`/`bun run build` output; screenshots only help when showing SQL traces or unexpected planner output.

## Writing style

- Avoid using em-dashes or dashes "-" and semi columns ";".
- Avoid using too many adjectives or adverbs
- Avoid using '&' sign in the middle of a sentence.
