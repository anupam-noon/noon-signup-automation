"""Idempotent HubSpot contact upsert.

Uses the Batch Create endpoint where possible, falls back to per-contact update
on 409 (already exists).

Docs: https://developers.hubspot.com/docs/api/crm/contacts
"""
import datetime as dt
import requests
from config import cfg

BASE = "https://api.hubapi.com"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {cfg.hubspot_token}",
        "Content-Type": "application/json",
    }


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

    Returns a summary: {created, updated, skipped, errors}.
    """
    if cfg.dry_run:
        print(f"[DRY_RUN] would upsert {len(contacts)} contacts to HubSpot")
        for c in contacts[:3]:
            print("   sample:", c)
        return {"created": 0, "updated": 0, "skipped": 0, "errors": 0, "dry_run": True}

    created = updated = skipped = errors = 0

    # Use the upsert endpoint: /crm/v3/objects/contacts/batch/upsert
    # idProperty=email means HubSpot matches on email and creates if missing
    url = f"{BASE}/crm/v3/objects/contacts/batch/upsert"
    # batches of 100 max
    for i in range(0, len(contacts), 100):
        chunk = contacts[i : i + 100]
        body = {
            "inputs": [
                {
                    "idProperty": "email",
                    "id": c["email"],
                    "properties": {k: v for k, v in c.items() if v != ""},
                }
                for c in chunk
            ]
        }
        r = requests.post(url, headers=_headers(), json=body, timeout=60)
        if r.status_code >= 300:
            print(f"  batch {i}-{i+len(chunk)}: {r.status_code} {r.text[:400]}")
            errors += len(chunk)
            continue
        result = r.json()
        # HubSpot response doesn't split created vs updated on upsert.
        # Count results as "touched".
        touched = len(result.get("results", []))
        created += touched  # rough accounting
    return {"created": created, "updated": updated, "skipped": skipped, "errors": errors}


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
