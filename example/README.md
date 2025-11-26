# MotherDuck nyc.taxi example

This repo ships a tiny script that hits the sample MotherDuck share and runs a couple of Drizzle queries against `nyc.taxi`.

## Running it

- Make sure you have a MotherDuck token (`Profile -> Service Tokens` in the app) and export it as `MOTHERDUCK_TOKEN`.
- Install deps if you have not already: `bun install`
- From the repo root run: `bun example/motherduck-nyc.ts`

The script:

- Connects to MotherDuck via `md:` using your token.
- References the built-in `sample_data.nyc.taxi` share (Taxi ride data from Nov 2020, attached by default).
- Builds a temporary `taxi_sample` view limited to 50k rows, then prints a few example rows and average fare/tip numbers grouped by passenger count.
- Closes the node-api connection eagerly.
