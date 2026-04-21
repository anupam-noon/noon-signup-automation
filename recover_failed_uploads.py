"""One-off recovery script for the 2026-04-20 run.

That run uploaded 125/525 contacts to HubSpot. The other 400 failed with
400 "Duplicate IDs found in batch input", and the watermark advanced for
all 525 so the nightly cron won't retry them.

This script:
  1. Reads the classified CSV from the failed run (download it from the
     GitHub Actions artifact and put the path below).
  2. Filters to Greens + Yellows (what the pipeline would have uploaded).
  3. Fetches the set of emails already in HubSpot.
  4. Uploads only the difference using the fixed upsert_contacts().

Usage:
  python recover_failed_uploads.py path/to/classified_*.csv

Pass one or more classified CSVs (shell glob is fine). Duplicates across
files are handled — we only upload emails not already in HubSpot.
"""
import csv
import sys
from pathlib import Path

from sinks.hubspot import fetch_existing_emails, upsert_contacts, today_batch_label


def load_classified(path: Path) -> list[dict]:
    """Read the pipeline's classified CSV and project to HubSpot contact shape."""
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            verdict = r.get("_verdict") or r.get("verdict")
            if verdict not in ("Green", "Yellow"):
                continue
            rows.append({
                "email": (r.get("email") or "").strip().lower(),
                "firstname": r.get("first_name", ""),
                "lastname": r.get("last_name", ""),
                "greenfake_contact": "Green" if verdict == "Green" else "Fake",
                "send_email_date": r.get("send_email_date") or today_batch_label(),
                "confirmation_email_sent": "",
            })
    return rows


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2

    paths = [Path(p) for p in sys.argv[1:]]
    for p in paths:
        if not p.exists():
            print(f"not found: {p}")
            return 2

    # Merge all CSVs, dedupe by email (last occurrence wins).
    merged: dict[str, dict] = {}
    for p in paths:
        rows = load_classified(p)
        print(f"  {p.name}: {len(rows)} actionable")
        for r in rows:
            merged[r["email"]] = r
    want = list(merged.values())
    print(f"merged across {len(paths)} file(s): {len(want)} unique actionable")

    print("fetching existing HubSpot emails (this can take a minute)...")
    existing = fetch_existing_emails()
    print(f"hubspot currently has {len(existing):,} contacts")

    missing = [c for c in want if c["email"] not in existing]
    print(f"missing from HubSpot: {len(missing)}")

    if not missing:
        print("nothing to do.")
        return 0

    # Show a few samples so you can eyeball before committing.
    for c in missing[:5]:
        print("  sample:", c)
    if input(f"upload {len(missing)} contacts? [y/N] ").strip().lower() != "y":
        print("aborted.")
        return 1

    summary = upsert_contacts(missing)
    print("result:", summary)
    return 0 if summary.get("errors", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
