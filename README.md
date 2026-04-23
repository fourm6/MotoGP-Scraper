# MotoGP results ingester

This script crawls the public Pulse Live MotoGP API using:

1. `v1/results/events?seasonUuid=...&isFinished=true`
2. `v1/results/sessions?eventUuid=...&categoryUuid=...`
3. `v2/results/classifications?session=...&test=false`

It writes both:
- raw JSON payloads for each classification fetch
- normalized tables in SQLite for seasons, events, categories, sessions, and classification entries

## Files

- `motogp_results_ingest.py` — main crawler
- `season_uuids.example.json` — template input for season UUIDs

## Install

```bash
pip install requests
```

## Prepare season UUIDs

Create a file like:

```json
[
  {"year": 2025, "uuid": "632718a6-f1fe-4f5a-b95d-03b8e7635d70"}
]
```

Or a plain text file with one UUID per line.

## Run

```bash
python motogp_results_ingest.py \
  --db motogp_results.db \
  --season-uuids-file season_uuids.json \
  --sleep-seconds 0.75
```

To ingest every session type instead of race-like sessions only:

```bash
python motogp_results_ingest.py \
  --db motogp_results.db \
  --season-uuids-file season_uuids.json \
  --all-sessions
```

## What the database contains

### `classification_fetches`
One row per fetched classification payload.

### `classification_entries`
One row per rider/result entry, with fields such as:
- position
- rider name
- rider number
- team name
- constructor/bike
- time/gaps
- points
- status

## Suggested next step

Once you verify the exact field names in a few returned payloads, tighten the extractor in `extract_classification_entries()` and the normalization logic in `replace_classification_entries()` to match the live schema exactly.
