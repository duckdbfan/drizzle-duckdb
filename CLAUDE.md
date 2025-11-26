# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `@leonardovida-md/drizzle-neo-duckdb`, a DuckDB dialect adapter for drizzle-orm. It builds on Drizzle's Postgres driver surface but targets DuckDB, providing query building, migrations, and type inference for DuckDB's Node runtime (`@duckdb/node-api`).

## Commands

- **Install dependencies:** `bun install`
- **Run all tests:** `bun test`
- **Run tests with watch mode and UI:** `bun t`
- **Run a single test file:** `bun test test/<filename>.test.ts`
- **Build:** `bun run build` (emits `dist/index.mjs`, `dist/duckdb-introspect.mjs`, and type declarations)
- **Build declarations only:** `bun run build:declarations`

## Architecture

### Core Module Structure (`src/`)

The package exports from `src/index.ts` which re-exports:

- `driver.ts` - Main entry point with `drizzle()` factory and `DuckDBDatabase` class extending `PgDatabase`
- `session.ts` - `DuckDBSession` and `DuckDBPreparedQuery` for query execution, transaction handling
- `columns.ts` - DuckDB-specific column helpers (`duckDbList`, `duckDbArray`, `duckDbStruct`, `duckDbMap`, `duckDbJson`, `duckDbTimestamp`, etc.)
- `migrator.ts` - `migrate()` function for applying SQL migrations
- `introspect.ts` - Schema introspection for generating Drizzle schema from existing DuckDB tables

### Key Design Decisions

1. **Built on Postgres Driver**: Extends `PgDialect`, `PgSession`, `PgDatabase` from `drizzle-orm/pg-core` since DuckDB's SQL is largely Postgres-compatible

2. **Array Operator Rewriting**: By default (`rewriteArrays: true`), Postgres array operators (`@>`, `<@`, `&&`) are rewritten to DuckDB's `array_has_*` functions in `src/sql/query-rewriters.ts`

3. **Custom Column Types**: DuckDB-specific types (STRUCT, MAP, LIST, JSON) use custom type builders in `columns.ts` that handle serialization to DuckDB literal syntax

4. **Result Mapping**: `src/sql/result-mapper.ts` handles converting DuckDB query results to Drizzle's expected format, including alias deduplication

5. **No Pg JSON/JSONB**: The dialect throws if Postgres JSON/JSONB columns are used - must use `duckDbJson()` instead

### Testing

Tests are in `test/` using Vitest. Key test files:

- `duckdb.test.ts` - Main integration tests
- `columns.test.ts` - Column type helpers
- `timestamps.test.ts` - Date/time handling
- `introspect.test.ts` - Schema introspection
- `motherduck.integration.test.ts` - MotherDuck cloud database tests (requires `MOTHERDUCK_TOKEN`)

### CLI Tool

`src/bin/duckdb-introspect.ts` provides a CLI for generating Drizzle schema from DuckDB:

```sh
bun x duckdb-introspect --url ':memory:' --schema my_schema --out ./drizzle/schema.ts
```

## Documentation Structure

- `README.md` - Main documentation with quick start, installation, and feature overview
- `docs/columns.md` - Complete reference for column types (standard and DuckDB-specific)
- `docs/migrations.md` - Migration setup and configuration
- `docs/introspection.md` - CLI and programmatic introspection API
- `docs/limitations.md` - Known differences from Postgres driver

## Important Conventions

- ESM only with explicit `.ts` extensions in imports
- Source uses `moduleResolution: bundler`
- Never edit files in `dist/` - they are generated
- Never use emojis in comments or code
- Be concise and to the point

<frontend_aesthetics>
You tend to converge toward generic, "on distribution" outputs. In frontend design,this creates what users call the "AI slop" aesthetic. Avoid this: make creative,distinctive frontends that surprise and delight.

Focus on:

- Typography: Choose fonts that are beautiful, unique, and interesting. Avoid generic fonts like Arial and Inter; opt instead for distinctive choices that elevate the frontend's aesthetics.
- Color & Theme: Commit to a cohesive aesthetic. Use CSS variables for consistency. Dominant colors with sharp accents outperform timid, evenly-distributed palettes. Draw from IDE themes and cultural aesthetics for inspiration.
- Motion: Use animations for effects and micro-interactions. Prioritize CSS-only solutions for HTML. Use Motion library for React when available. Focus on high-impact moments: one well-orchestrated page load with staggered reveals (animation-delay) creates more delight than scattered micro-interactions.
- Backgrounds: Create atmosphere and depth rather than defaulting to solid colors. Layer CSS gradients, use geometric patterns, or add contextual effects that match the overall aesthetic.

Avoid generic AI-generated aesthetics:

- Overused font families (Inter, Roboto, Arial, system fonts)
- Clichéd color schemes (particularly purple gradients on white backgrounds)
- Predictable layouts and component patterns
- Cookie-cutter design that lacks context-specific character

Interpret creatively and make unexpected choices that feel genuinely designed for the context. Vary between light and dark themes, different fonts, different aesthetics. You still tend to converge on common choices (Space Grotesk, for example) across generations. Avoid this: it is critical that you think outside the box!
</frontend_aesthetics>
