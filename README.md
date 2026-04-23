*disclaimer this code was created using help of AI, I just needed the database for another project and I got lazy*

# MotoGP Scraper

Script grabs all race classification data requested from MotoGP website

## Files

- `motogp_scraper.py` — main crawler

## Required libraries

```bash
pip install requests
```

## Run

```bash
python motogp_results_ingest.py \
  --db motogp_results.db \
  --season-uuids-file season_uuids.json \
  --sleep-seconds 0.75
```

To scrape every session type instead of just race sessions:

```bash
python motogp_results_ingest.py \
  --db motogp_results.db \
  --season-uuids-file season_uuids.json \
  --all-sessions
```

## Database structure

### `seasons_events.jsonl`
- fetched_at
- source_urls
- events_url
- categories_url
- season, year
- event
- country_name
- circuit_name
- start_date
- event_files...

### `classifications.jsonl`
- fetched_at
- source_url
- season_uuid
- season_year
- event_uuid
- event_short_name
- event_name
- category_uuid
- category_name
- session_name
- session_code
- entries


