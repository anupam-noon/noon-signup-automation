"""Post-flight check: did HubSpot actually send today's emails?

Runs ~30 min after the main pipeline. Queries HubSpot for contacts with
today's `send_email_date` and counts how many have
`confirmation_email_sent = "Yes"`. Writes a small summary so the workflow
step can post a single aggregated Slack message.

Why this isn't inside pipeline.py: the email send is HubSpot's
responsibility (workflow on Multi Email Queue) and runs asynchronously
after our pipeline exits. Polling from the main pipeline would either
report too early (workflow hasn't fired yet) or block CI minutes for
nothing. A separate cron 30 min later is the cleanest split.
"""
import datetime as dt
import json
import os
import sys
from pathlib import Path

import requests

BASE = "https://api.hubapi.com"
SUMMARY_PATH = Path(__file__).parent / "send_check_summary.json"

# Property names — keep in sync with what the pipeline writes / HubSpot sets.
PROP_BATCH = "send_email_date"
PROP_SENT = "confirmation_email_sent"
SENT_VALUE = "Yes"


def _headers() -> dict:
    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        print("HUBSPOT_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _today_batch_label() -> str:
    """Same format as sinks/hubspot.py:today_batch_label().

    Inlined (rather than importing) because the workflow runs this script
    standalone and we want zero coupling to config.py / .env.
    """
    return dt.datetime.now().strftime("%d %b") + " - Batch 1"


def search_batch(label: str) -> list[dict]:
    """Page through all contacts matching send_email_date == label."""
    url = f"{BASE}/crm/v3/objects/contacts/search"
    out: list[dict] = []
    after: str | None = None
    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": PROP_BATCH, "operator": "EQ", "value": label}
                ]
            }],
            "properties": [PROP_BATCH, PROP_SENT, "email"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = requests.post(url, headers=_headers(), json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return out


def main() -> None:
    label = os.environ.get("BATCH_LABEL_OVERRIDE") or _today_batch_label()
    print(f"checking send status for batch: {label!r}")

    try:
        contacts = search_batch(label)
    except Exception as e:
        # Don't fail the workflow — write a summary the Slack step can render.
        summary = {
            "status": "error",
            "batch_label": label,
            "reason": str(e)[:300],
        }
        SUMMARY_PATH.write_text(json.dumps(summary, indent=2))
        print(f"search failed: {e}")
        sys.exit(0)

    total = len(contacts)
    sent = sum(
        1 for c in contacts
        if (c.get("properties", {}).get(PROP_SENT) or "").strip() == SENT_VALUE
    )
    pending = total - sent

    summary = {
        "status": "ok",
        "batch_label": label,
        "total": total,
        "sent": sent,
        "pending": pending,
        "checked_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
