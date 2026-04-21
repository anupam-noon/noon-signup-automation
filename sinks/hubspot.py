"""Idempotent HubSpot contact upsert.

Strategy: batch-first, per-contact fallback.
  • Batch endpoint (/batch/upsert, 100 at a time) is ~10× faster.
  • If a batch fails for a non-transient reason (e.g. 400 "Duplicate IDs",
    or one malformed record), we fall back to per-contact upsert for
    that chunk so a single bad record doesn't poison 99 good ones.
  • Transient errors (429, 5xx) go through exponential-backoff retry.

This scales: 500 contacts/day runs in seconds; 50K/day runs in ~5 min.

We also dedupe by email before batching. HubSpot's batch endpoint 400s
with "Duplicate IDs found in batch input" if the same idProperty value
appears twice — that's the bug that lost us 400/525 uploads on 2026-04-20.

Docs: https://developers.hubspot.com/docs/api/crm/contacts
"""
import datetime as dt
import time
import requests
from config import cfg

BASE = "https://api.hubapi.com"
BATCH_SIZE = 100
# Paid-plan private-app limit is ~190 req/10s, plenty of headroom.
MAX_RETRIES = 3


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {cfg.hubspot_token}",
        "Content-Type": "application/json",
    }


def _dedupe(contacts: list[dict]) -> tuple[list[dict], int]:
    """Keep LAST occurrence per email (latest data wins). Lowercases email."""
    seen: dict[str, dict] = {}
    for c in contacts:
        e = (c.get("email") or "").strip().lower()
        if not e:
            continue
        seen[e] = {**c, "email": e}
    return list(seen.values()), len(contacts) - len(seen)


def _props(c: dict) -> dict:
    """Strip empty values; email goes in `id`, not properties."""
    return {k: v for k, v in c.items() if k != "email" and v != ""}


def _request_with_retry(method: str, url: str, **kw):
    """POST/PATCH with exponential backoff on 429/5xx. Returns the final response."""
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
        r = requests.request(method, url, headers=_headers(), timeout=60, **kw)
        if r.status_code < 300 or r.status_code not in (429, 500, 502, 503, 504):
            return r
        time.sleep(backoff)
        backoff *= 2
    return r  # exhausted retries; caller inspects status


def _upsert_one(contact: dict) -> tuple[bool, str]:
    """PATCH a single contact. Returns (ok, error_detail)."""
    email = contact["email"]
    url = f"{BASE}/crm/v3/objects/contacts/{email}?idProperty=email"
    r = _request_with_retry("PATCH", url, json={"properties": _props(contact)})
    if r.status_code < 300:
        return True, ""
    return False, f"{r.status_code} {r.text[:200]}"


def _upsert_batch(chunk: list[dict]) -> tuple[int, list[dict]]:
    """Try the batch endpoint. Returns (n_ok, failed_chunk_for_fallback).

    On batch-level success: (len(chunk), []).
    On any non-transient batch failure: (0, chunk) — caller should retry
    each contact individually so one bad record doesn't sink the rest.
    On exhausted-retry transient failure: (0, chunk) — same fallback path.
    """
    url = f"{BASE}/crm/v3/objects/contacts/batch/upsert"
    body = {
        "inputs": [
            {"idProperty": "email", "id": c["email"], "properties": _props(c)}
            for c in chunk
        ]
    }
    r = _request_with_retry("POST", url, json=body)
    if r.status_code < 300:
        return len(r.json().get("results", [])), []
    # Batch failed — hand off to per-contact fallback.
    print(f"  batch of {len(chunk)} failed ({r.status_code} {r.text[:200]}); "
          f"falling back to per-contact")
    return 0, chunk


def upsert_contacts(contacts: list[dict]) -> dict:
    """Create or update contacts, keyed on email.

    Each contact dict should be:
        {
            "email": "...",
            "firstname": "...",
            "lastname": "...",
            "greenfake_contact": "Green" | "Fake",
            "send_email_date": "20 Apr - Batch 1",
            "confirmation_email_sent": "Yes" | "",
        }

    Returns a summary: {created, updated, skipped, errors, fallback_used}.
    (HubSpot doesn't distinguish created vs updated on upsert; everything
    successful is counted under `created`.)
    """
    if cfg.dry_run:
        print(f"[DRY_RUN] would upsert {len(contacts)} contacts to HubSpot")
        for c in contacts[:3]:
            print("   sample:", c)
        return {"created": 0, "updated": 0, "skipped": 0, "errors": 0, "dry_run": True}

    deduped, dropped = _dedupe(contacts)
    if dropped:
        print(f"[hubspot] deduped {dropped} duplicate/empty email(s); "
              f"uploading {len(deduped)}")

    created = errors = 0
    fallback_used = 0  # how many records fell back to per-contact

    for i in range(0, len(deduped), BATCH_SIZE):
        chunk = deduped[i : i + BATCH_SIZE]
        ok, failed = _upsert_batch(chunk)
        created += ok

        # Per-contact fallback for the records in a failed batch.
        for c in failed:
            fallback_used += 1
            ok_one, detail = _upsert_one(c)
            if ok_one:
                created += 1
            else:
                errors += 1
                print(f"  {c['email']}: {detail}")

        if (i // BATCH_SIZE) % 5 == 4:
            print(f"  progress: {min(i+BATCH_SIZE, len(deduped))}/{len(deduped)}  "
                  f"(ok={created}, errors={errors}, fallback={fallback_used})")

    return {
        "created": created,
        "updated": 0,
        "skipped": dropped,
        "errors": errors,
        "fallback_used": fallback_used,
    }


def fetch_existing_emails() -> set[str]:
    """Return the set of emails already in HubSpot (lowercased)."""
    out: set[str] = set()
    after = None
    url = f"{BASE}/crm/v3/objects/contacts"
    while True:
        params = {"limit": 100, "properties": "email"}
        if after:
            params["after"] = after
        r = requests.get(url, headers=_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("results", []):
            e = (rec.get("properties", {}).get("email") or "").strip().lower()
            if e:
                out.add(e)
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return out


def today_batch_label() -> str:
    """E.g. '21 Apr - Batch 1'."""
    return dt.datetime.now().strftime("%d %b") + " - Batch 1"
