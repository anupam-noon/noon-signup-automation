"""One-time seed of state/watermark.json from the already-classified consolidated CSV.

Run once before the first live pipeline execution. After this, the pipeline
will skip any email in this watermark and only process signups new to us.
"""
import csv
import json
import datetime as dt
from pathlib import Path

CONSOL = "/Users/anupam/Downloads/export-2026-04-04-164402-consolidated.csv"
STATE = Path(__file__).parent / "state" / "watermark.json"


def main():
    emails: set[str] = set()
    with open(CONSOL, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            e = (row.get("email") or "").strip().lower()
            if e:
                emails.add(e)

    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps({
        "seen_emails": sorted(emails),
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
        "seeded_from": CONSOL,
        "note": "last classified: jahidislam3061@gmail.com @ 2026-04-04 16:43:46 UTC",
    }))
    print(f"seeded {len(emails):,} emails into {STATE}")


if __name__ == "__main__":
    main()
