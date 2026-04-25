"""Idempotent HubSpot contact upsert.

Strategy: validate → dedupe → batch-first → per-contact fallback (PATCH then POST).
  • We drop malformed emails up front (e.g. invalid TLD `sparsh@paasa.cp`)
    so one bad record never poisons a batch.
  • Batch endpoint (/batch/upsert, 100 at a time) is ~10× faster than per-contact.
  • If a batch fails for a non-transient reason (a single bad record left over,
    or the ever-fun "Duplicate IDs" 400), we fall back to per-contact for
    that chunk: PATCH the contact; if 404 (doesn't exist yet), POST to create.
    Both paths together = idempotent upsert even when batch refuses.
  • Transient errors (429, 5xx) go through exponential-backoff retry.

History:
  2026-04-20: lost 400/525 because batch endpoint 400'd on duplicate IDs.
              Fixed with dedupe + per-contact fallback.
  2026-04-24: lost 100/200 because (a) one malformed email crashed the batch,
              and (b) per-contact fallback was PATCH-only — PATCH 404s for new
              contacts. This commit fixes both: pre-validation + create-on-404.

This scales: 500 contacts/day runs in seconds; 50K/day runs in ~5 min.

Docs: https://developers.hubspot.com/docs/api/crm/contacts
"""
import datetime as dt
import re
import time
import requests
from config import cfg

BASE = "https://api.hubapi.com"
BATCH_SIZE = 100
# Paid-plan private-app limit is ~190 req/10s, plenty of headroom.
MAX_RETRIES = 3

# Conservative email format check. Rejects what HubSpot would reject anyway
# (no @, no dot in domain, single-char TLD, whitespace, etc.) — this isn't
# a deliverability check, just a pre-filter so one bad row doesn't tank a batch.
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[A-Za-z]{2,}$")


def _looks_valid(email: str) -> bool:
    if not email or not _EMAIL_RE.match(email):
        return False
    # `.cp`, `.co1`, etc. — TLD must be alpha-only (already enforced by regex)
    # and at least 2 chars. Common-typo TLDs like `.cm`/`.co` we leave alone:
    # they're real TLDs, not our place to second-guess.
    return True


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
    """PATCH the contact; on 404 (doesn't exist yet), POST to create.

    PATCH-only was the bug behind the 2026-04-24 incident: when a batch
    failed and we fell back to per-contact, every new contact 404'd
    because PATCH only updates existing rows.
    """
    email = contact["email"]
    props = _props(contact)

    # Try update first.
    patch_url = f"{BASE}/crm/v3/objects/contacts/{email}?idProperty=email"
    r = _request_with_retry("PATCH", patch_url, json={"properties": props})
    if r.status_code < 300:
        return True, ""
    if r.status_code != 404:
        return False, f"PATCH {r.status_code} {r.text[:200]}"

    # Doesn't exist — create it. Email goes in properties for POST.
    create_url = f"{BASE}/crm/v3/objects/contacts"
    r = _request_with_retry("POST", create_url,
                             json={"properties": {**props, "email": email}})
    if r.status_code < 300:
        return True, ""
    return False, f"POST {r.status_code} {r.text[:200]}"


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
        return {"created": 0, "updated": 0, "skipped": 0, "errors": 0,
                "dry_run": True, "errored_emails": []}

    # 1. Pre-validate. Drop obvious malformed emails before they poison a batch.
    valid: list[dict] = []
    invalid_emails: list[str] = []
    for c in contacts:
        e = (c.get("email") or "").strip().lower()
        if not _looks_valid(e):
            invalid_emails.append(e)
            continue
        valid.append({**c, "email": e})
    if invalid_emails:
        print(f"[hubspot] dropped {len(invalid_emails)} malformed-email row(s) "
              f"pre-flight (e.g. {invalid_emails[:3]})")

    # 2. Dedupe within the batch (HubSpot 400s on duplicate idProperty values).
    deduped, dropped = _dedupe(valid)
    if dropped:
        print(f"[hubspot] deduped {dropped} duplicate/empty email(s); "
              f"uploading {len(deduped)}")

    created = errors = 0
    fallback_used = 0
    errored_emails: list[str] = []  # caller holds these back from the watermark

    for i in range(0, len(deduped), BATCH_SIZE):
        chunk = deduped[i : i + BATCH_SIZE]
        ok, failed = _upsert_batch(chunk)
        created += ok

        # Per-contact fallback (PATCH then POST-on-404) for the records in a
        # failed batch. So one bad row no longer sinks 99 good ones.
        for c in failed:
            fallback_used += 1
            ok_one, detail = _upsert_one(c)
            if ok_one:
                created += 1
            else:
                errors += 1
                errored_emails.append(c["email"])
                print(f"  {c['email']}: {detail}")

        if (i // BATCH_SIZE) % 5 == 4:
            print(f"  progress: {min(i+BATCH_SIZE, len(deduped))}/{len(deduped)}  "
                  f"(ok={created}, errors={errors}, fallback={fallback_used})")

    return {
        "created": created,
        "updated": 0,
        "skipped": dropped + len(invalid_emails),
        "errors": errors,
        "fallback_used": fallback_used,
        "invalid_emails": invalid_emails,   # malformed; never retry
        "errored_emails": errored_emails,   # transient/unknown; safe to retry next run
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
