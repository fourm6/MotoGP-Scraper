#!/usr/bin/env python3
"""MotoGP results scraper writing two JSONL output files.

Flow used:
    seasons -> events -> categories -> sessions -> classifications

Endpoints:
    /v1/results/seasons
    /v1/results/events?seasonUuid=...&isFinished=true
    /v1/results/categories?eventUuid=...
    /v1/results/sessions?eventUuid=...&categoryUuid=...
    /v2/results/classifications?session=...&test=false

Outputs:
  1) seasons_events.jsonl
     One JSON object per event containing season metadata, event metadata,
     discovered categories, and session manifests by category.

  2) classifications.jsonl
     One JSON object per successful classification fetch containing source
     metadata, the full raw classification payload, and normalized entries.

Behavior:
  - Tries every season returned by the seasons endpoint.
  - Tries every finished event in each season.
  - Tries every category returned for each event.
  - Tries every session returned for each event/category pair.
  - If a category has no sessions, or a session has no classification,
    it skips it and keeps going.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import requests

BASE = "https://api.pulselive.motogp.com/motogp"
SEASONS_ENDPOINT = BASE + "/v1/results/seasons"
EVENTS_ENDPOINT = BASE + "/v1/results/events"
CATEGORIES_ENDPOINT = BASE + "/v1/results/categories"
SESSIONS_ENDPOINT = BASE + "/v1/results/sessions"
CLASSIFICATIONS_ENDPOINT = BASE + "/v2/results/classifications"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "motogp-results-ingester/3.0 (+local archival use)",
}

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class APIClient:
    def __init__(self, timeout: float = 30.0, sleep_seconds: float = 0.75) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=2)
        self.session.mount("https://", adapter)
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> tuple[Any, str, int]:
        prepared = requests.Request("GET", url, params=params).prepare()
        full_url = prepared.url or url
        response = self.session.get(url, params=params, timeout=self.timeout)
        time.sleep(self.sleep_seconds)
        status = response.status_code
        response.raise_for_status()
        return response.json(), full_url, status


class JSONLWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = path.open("w", encoding="utf-8")

    def write(self, obj: dict[str, Any]) -> None:
        self.handle.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self.handle.close()


def utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def first_present(obj: dict[str, Any] | None, keys: list[str]) -> Any:
    if not isinstance(obj, dict):
        return None
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return None


def first_dict(obj: dict[str, Any] | None, keys: list[str]) -> dict[str, Any] | None:
    value = first_present(obj, keys)
    return value if isinstance(value, dict) else None


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        m = re.search(r"-?\d+", str(value))
        return int(m.group()) if m else None


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        m = re.search(r"-?\d+(?:\.\d+)?", str(value))
        return float(m.group()) if m else None


def stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    mapping = {
        "ret": "dnf",
        "retired": "dnf",
        "dnf": "dnf",
        "dns": "dns",
        "dsq": "dsq",
        "disqualified": "dsq",
        "nc": "not_classified",
        "not classified": "not_classified",
        "finished": "finished",
        "classified": "classified",
    }
    return mapping.get(text, text)


def iter_dicts(node: Any) -> Iterator[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_dicts(item)


def extract_list_payload(payload: Any, preferred_keys: list[str]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in preferred_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def extract_seasons(payload: Any) -> list[dict[str, Any]]:
    seasons = extract_list_payload(payload, ["seasons", "items", "data", "content"])
    if seasons:
        return seasons
    out = []
    for obj in iter_dicts(payload):
        season_uuid = first_present(obj, ["seasonUuid", "uuid", "id"])
        if isinstance(season_uuid, str) and UUID_RE.match(season_uuid):
            out.append(obj)
    seen = set()
    unique = []
    for obj in out:
        season_uuid = first_present(obj, ["seasonUuid", "uuid", "id"])
        if season_uuid not in seen:
            seen.add(season_uuid)
            unique.append(obj)
    return unique


def extract_events(payload: Any) -> list[dict[str, Any]]:
    events = extract_list_payload(payload, ["events", "items", "data", "content"])
    if events:
        return events
    out = []
    for obj in iter_dicts(payload):
        event_uuid = first_present(obj, ["eventUuid", "uuid", "id"])
        if isinstance(event_uuid, str) and UUID_RE.match(event_uuid):
            if any(k in obj for k in ("shortName", "name", "eventName")):
                out.append(obj)
    seen = set()
    unique = []
    for obj in out:
        event_uuid = first_present(obj, ["eventUuid", "uuid", "id"])
        if event_uuid not in seen:
            seen.add(event_uuid)
            unique.append(obj)
    return unique


def extract_categories(payload: Any) -> list[dict[str, Any]]:
    direct = extract_list_payload(payload, ["categories", "classes", "items", "data", "content"])
    if direct:
        found = direct
    else:
        found = []
        for obj in iter_dicts(payload):
            category_uuid = first_present(obj, ["categoryUuid", "uuid", "id"])
            if isinstance(category_uuid, str) and UUID_RE.match(category_uuid):
                found.append(obj)
    seen = set()
    unique = []
    for obj in found:
        category_uuid = first_present(obj, ["categoryUuid", "uuid", "id"])
        if isinstance(category_uuid, str) and UUID_RE.match(category_uuid) and category_uuid not in seen:
            seen.add(category_uuid)
            unique.append(obj)
    return unique


SESSION_NAME_KEYS = ["name", "sessionName", "description", "label"]
SESSION_CODE_KEYS = ["code", "sessionCode", "shortName", "type", "sessionType"]
ENTRY_LIST_KEYS = [
    "classification",
    "classifications",
    "results",
    "entries",
    "items",
    "rows",
    "classificationEntries",
    "resultEntries",
]


def extract_sessions(payload: Any) -> list[dict[str, Any]]:
    sessions = extract_list_payload(payload, ["sessions", "items", "data", "content"])
    if sessions:
        found = sessions
    else:
        found = []
        for obj in iter_dicts(payload):
            session_uuid = first_present(obj, ["sessionUuid", "uuid", "id"])
            if isinstance(session_uuid, str) and UUID_RE.match(session_uuid):
                if any(k in obj for k in ("sessionUuid", "sessionName", "sessionCode", "sessionType", "test", "isTest", "code")):
                    found.append(obj)
    seen = set()
    unique = []
    for obj in found:
        session_uuid = first_present(obj, ["sessionUuid", "uuid", "id"])
        if session_uuid not in seen:
            seen.add(session_uuid)
            unique.append(obj)
    return unique


def extract_classification_entries(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ENTRY_LIST_KEYS:
            value = payload.get(key)
            if isinstance(value, list) and all(isinstance(x, dict) for x in value):
                return value
            if isinstance(value, dict):
                for nested_key in ENTRY_LIST_KEYS:
                    nested = value.get(nested_key)
                    if isinstance(nested, list) and all(isinstance(x, dict) for x in nested):
                        return nested
    fallback = []
    for obj in iter_dicts(payload):
        if any(k in obj for k in ("position", "pos", "rank", "rider", "riderName", "points", "gapFirst", "time")):
            fallback.append(obj)
    seen = set()
    unique = []
    for obj in fallback:
        marker = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        if marker not in seen:
            seen.add(marker)
            unique.append(obj)
    return unique


def normalize_entry(entry: dict[str, Any], index: int) -> dict[str, Any]:
    rider_obj = first_dict(entry, ["rider", "competitor", "athlete", "participant"])
    team_obj = first_dict(entry, ["team", "squad"])
    ctor_obj = first_dict(entry, ["constructor", "bike", "manufacturer", "brand"])
    best_lap_obj = first_dict(entry, ["bestLap", "fastestLap", "lap"])
    speed_obj = first_dict(entry, ["speed", "topSpeed", "bestSpeed"])
    rider_uuid = first_present(rider_obj, ["id", "uuid", "riderUuid"])
    return {
        "entry_index": index,
        "entry_uuid": first_present(entry, ["id", "uuid", "resultUuid", "classificationEntryUuid"]),
        "rider_uuid": rider_uuid,
        "position": coerce_int(first_present(entry, ["position", "pos", "rank"])),
        "position_text": stringify(first_present(entry, ["positionText", "position", "pos", "rank"])),
        "rider_name": first_present(rider_obj or entry, ["fullName", "name", "riderName", "displayName"]),
        "rider_number": stringify(first_present(rider_obj or entry, ["number", "raceNumber", "bib"])),
        "country_code": first_present(first_dict(rider_obj or entry, ["country", "nationality"]) or {}, ["iso", "code", "countryCode"]),
        "team_name": first_present(team_obj or entry, ["name", "teamName"]),
        "constructor_name": first_present(ctor_obj or entry, ["name", "constructorName", "manufacturerName", "brand"]),
        "bike_name": first_present(ctor_obj or entry, ["model", "bikeName", "name"]),
        "time_text": stringify(first_present(entry, ["time", "totalTime", "timeText", "resultTime"])),
        "gap_first": stringify(first_present(entry, ["gapFirst", "gapToFirst", "gapLeader"])),
        "gap_prev": stringify(first_present(entry, ["gapPrev", "gapToPrev", "gapPrevious"])),
        "laps_completed": coerce_int(first_present(entry, ["laps", "lapsCompleted", "completedLaps"])),
        "points": coerce_float(first_present(entry, ["points", "score"])),
        "status": normalize_status(first_present(entry, ["status", "classificationStatus", "resultStatus"])),
        "best_lap_time": stringify(first_present(best_lap_obj or entry, ["time", "lapTime", "bestLapTime"])),
        "best_lap_number": coerce_int(first_present(best_lap_obj or entry, ["number", "lap", "lapNumber"])),
        "speed_value": coerce_float(first_present(speed_obj or entry, ["value", "speed", "topSpeed"])),
        "raw_entry": entry,
    }


def fetch_seasons(client: APIClient) -> tuple[list[dict[str, Any]], str]:
    payload, url, _ = client.get_json(SEASONS_ENDPOINT)
    seasons = extract_seasons(payload)
    logging.info("Found %d seasons", len(seasons))
    return seasons, url


def is_race_like(session: dict[str, Any]) -> bool:
    text = " ".join(
        [x.lower() for x in [stringify(first_present(session, SESSION_NAME_KEYS)), stringify(first_present(session, SESSION_CODE_KEYS))] if x]
    )
    return any(token in text for token in ("rac", "race", "grand prix", "feature race"))


def crawl(client: APIClient, seasons_writer: JSONLWriter, classifications_writer: JSONLWriter, race_only: bool) -> None:
    seasons, seasons_url = fetch_seasons(client)

    for season in seasons:
        season_uuid = first_present(season, ["seasonUuid", "uuid", "id"])
        season_year = coerce_int(first_present(season, ["year", "seasonYear", "name", "label"]))
        if not season_uuid:
            continue

        logging.info("Season %s (%s)", season_year, season_uuid)
        try:
            events_payload, events_url, _ = client.get_json(EVENTS_ENDPOINT, {"seasonUuid": season_uuid, "isFinished": "true"})
        except Exception:
            logging.exception("Failed events fetch for season %s", season_uuid)
            continue

        events = extract_events(events_payload)
        logging.info("  found %d events", len(events))

        for event in events:
            event_uuid = first_present(event, ["eventUuid", "uuid", "id"])
            if not event_uuid:
                continue

            try:
                categories_payload, categories_url, _ = client.get_json(CATEGORIES_ENDPOINT, {"eventUuid": event_uuid})
            except Exception:
                logging.exception("Failed categories fetch for event %s", event_uuid)
                continue

            categories = extract_categories(categories_payload)
            session_manifests: list[dict[str, Any]] = []

            for category in categories:
                category_uuid = first_present(category, ["categoryUuid", "uuid", "id"])
                category_name = first_present(category, ["name", "displayName", "label"])
                if not category_uuid:
                    continue

                try:
                    sessions_payload, sessions_url, _ = client.get_json(
                        SESSIONS_ENDPOINT,
                        {"eventUuid": event_uuid, "categoryUuid": category_uuid},
                    )
                except requests.HTTPError as exc:
                    logging.info("    sessions miss for event=%s category=%s (%s)", event_uuid, category_uuid, exc)
                    session_manifests.append({
                        "category_uuid": category_uuid,
                        "category_name": category_name,
                        "sessions_url": None,
                        "session_count": 0,
                        "sessions": [],
                        "error": str(exc),
                    })
                    continue
                except Exception as exc:
                    logging.info("    sessions error for event=%s category=%s (%s)", event_uuid, category_uuid, exc)
                    session_manifests.append({
                        "category_uuid": category_uuid,
                        "category_name": category_name,
                        "sessions_url": None,
                        "session_count": 0,
                        "sessions": [],
                        "error": str(exc),
                    })
                    continue

                sessions = extract_sessions(sessions_payload)
                session_manifest = {
                    "category_uuid": category_uuid,
                    "category_name": category_name,
                    "sessions_url": sessions_url,
                    "session_count": len(sessions),
                    "sessions": [
                        {
                            "session_uuid": first_present(s, ["sessionUuid", "uuid", "id"]),
                            "session_name": first_present(s, SESSION_NAME_KEYS),
                            "session_code": first_present(s, SESSION_CODE_KEYS),
                            "is_test": bool(first_present(s, ["test", "isTest"])),
                            "raw": s,
                        }
                        for s in sessions
                    ],
                }
                session_manifests.append(session_manifest)

                if not sessions:
                    logging.info("    no sessions for event=%s category=%s", event_uuid, category_uuid)
                    continue

                for session in sessions:
                    session_uuid = first_present(session, ["sessionUuid", "uuid", "id"])
                    if not session_uuid:
                        continue

                    is_test = bool(first_present(session, ["test", "isTest"]))
                    if is_test:
                        continue
                    if race_only and not is_race_like(session):
                        continue

                    try:
                        class_payload, class_url, _ = client.get_json(
                            CLASSIFICATIONS_ENDPOINT,
                            {"session": session_uuid, "test": "false"},
                        )
                    except requests.HTTPError as exc:
                        logging.info("    classification miss for session %s (%s)", session_uuid, exc)
                        continue
                    except Exception:
                        logging.exception("    classification error for session %s", session_uuid)
                        continue

                    entries = extract_classification_entries(class_payload)
                    if not entries:
                        logging.info("    empty classification for session %s", session_uuid)
                        continue

                    classifications_writer.write({
                        "fetched_at": utcnow_iso(),
                        "source_url": class_url,
                        "season_uuid": season_uuid,
                        "season_year": season_year,
                        "event_uuid": event_uuid,
                        "event_short_name": first_present(event, ["shortName", "short_name"]),
                        "event_name": first_present(event, ["eventName", "name"]),
                        "category_uuid": category_uuid,
                        "category_name": category_name,
                        "session_uuid": session_uuid,
                        "session_name": first_present(session, SESSION_NAME_KEYS),
                        "session_code": first_present(session, SESSION_CODE_KEYS),
                        "entries": [normalize_entry(entry, idx) for idx, entry in enumerate(entries, start=1)],
                        "raw_classification": class_payload,
                    })
                    logging.info(
                        "    stored classification for event=%s category=%s session=%s rows=%d",
                        event_uuid,
                        category_uuid,
                        session_uuid,
                        len(entries),
                    )

            seasons_writer.write({
                "fetched_at": utcnow_iso(),
                "source_urls": {
                    "seasons_url": seasons_url,
                    "events_url": events_url,
                    "categories_url": categories_url,
                },
                "season": {
                    "season_uuid": season_uuid,
                    "year": season_year,
                    "name": first_present(season, ["name", "label", "displayName"]),
                    "raw": season,
                },
                "event": {
                    "event_uuid": event_uuid,
                    "short_name": first_present(event, ["shortName", "short_name"]),
                    "event_name": first_present(event, ["eventName", "name"]),
                    "display_name": first_present(event, ["displayName", "label"]),
                    "country_name": first_present(first_dict(event, ["country"]) or {}, ["name"]),
                    "circuit_name": first_present(first_dict(event, ["circuit", "venue"]) or {}, ["name"]),
                    "start_date": first_present(event, ["startDate", "dateStart", "start_date"]),
                    "end_date": first_present(event, ["endDate", "dateEnd", "end_date"]),
                    "raw": event,
                },
                "categories": categories,
                "sessions_by_category": session_manifests,
            })


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest MotoGP results into two JSONL files")
    parser.add_argument("--outdir", default=".", help="Directory for output files")
    parser.add_argument("--seasons-events-file", default="seasons_events.jsonl", help="Output JSONL for seasons/events")
    parser.add_argument("--classifications-file", default="classifications.jsonl", help="Output JSONL for classifications")
    parser.add_argument("--sleep-seconds", type=float, default=0.75, help="Pause between HTTP requests")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")
    parser.add_argument("--all-sessions", action="store_true", help="Try every discovered session, not just race-like sessions")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    seasons_path = outdir / args.seasons_events_file
    classifications_path = outdir / args.classifications_file

    client = APIClient(timeout=args.timeout, sleep_seconds=args.sleep_seconds)
    seasons_writer = JSONLWriter(seasons_path)
    classifications_writer = JSONLWriter(classifications_path)
    try:
        crawl(client, seasons_writer, classifications_writer, race_only=not args.all_sessions)
    finally:
        seasons_writer.close()
        classifications_writer.close()

    logging.info("Wrote %s", seasons_path)
    logging.info("Wrote %s", classifications_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
