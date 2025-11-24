# Work plan

- [x] Add `duckDbJson` custom type with driver conversions and export.
- [x] Update dialect handling to steer Pg JSON/JSONB users toward `duckDbJson`.
- [x] Add config flags for array rewrite toggling and strict stringified array handling; adjust client/session logging.
- [x] Add JSON and array rewrite regression tests.
