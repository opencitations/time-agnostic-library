<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# BEAR campaign

Run these commands from the repository root after conversion and Fuseki ingestion have finished. Use one clean commit for every run, because the runner records the revision and rejects mixed result sets during rendering.

## Correctness preflight

The preflight checks all pattern families, both temporal endpoints for VM and SD, and every version for CV. It derives SD from the published `Mat` answer sets and stops at the first count or digest mismatch. BEAR-A also runs without Fuseki text search, so the two discovery paths must return the same records.

```bash
uv run python benchmark/bear/verify_results.py --corpus bear-a --timeout 600 --compare-search
uv run python benchmark/bear/verify_results.py --corpus bear-b-daily --timeout 600
uv run python benchmark/bear/verify_results.py --corpus bear-b-hourly --timeout 600
```

## Timing campaign

Each case has one warm-up and three measured runs. The timeout applies to one SPARQL request rather than the whole query.

```bash
uv run python benchmark/bear/run_benchmark.py --corpus bear-a --measurement time --runs 3 --timeout 600
uv run python benchmark/bear/run_benchmark.py --corpus bear-b-daily --measurement time --runs 3 --timeout 600
uv run python benchmark/bear/run_benchmark.py --corpus bear-b-hourly --measurement time --runs 3 --timeout 600
```

## Memory campaign

These runs use one repetition because `tracemalloc` changes execution time and is kept outside the timing campaign.

```bash
uv run python benchmark/bear/run_benchmark.py --corpus bear-a --measurement memory --runs 1 --timeout 600
uv run python benchmark/bear/run_benchmark.py --corpus bear-b-daily --measurement memory --runs 1 --timeout 600
uv run python benchmark/bear/run_benchmark.py --corpus bear-b-hourly --measurement memory --runs 1 --timeout 600
```

## Paper artifacts

The renderer checks the revision, hardware, manifests, result status, and clean-checkout flag before it writes the three TAL figures and LaTeX tables.

```bash
uv run python benchmark/bear/render_tal_campaign.py --paper-dir /home/arcangelo/Documents/paper/jws_time_traversal
```

Keep the canonical time and memory JSON files, per-run JSON and JSONL files, verification files, conversion logs, ingestion logs, query manifests, the Fuseki image identifier and version, and the hardware record. Record conversion and ingestion wall times beside their logs because they run before the query runner starts.
