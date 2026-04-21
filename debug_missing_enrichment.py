"""Diagnose why some emails don't get enriched.

For a small sample of missing emails, try multiple lookup strategies to see
which (if any) finds them in PostHog.

Run:   python debug_missing_enrichment.py
"""
import json
from pathlib import Path
from sources.posthog import _query, enrich_by_email
from sources.sheets import fetch_rows
from pipeline import normalise, STATE_PATH


# Load the same set the pipeline just processed
seen = set(json.loads(STATE_PATH.read_text()).get("seen_emails", []))
rows = [normalise(r) for r in fetch_rows()]
new = [r for r in rows if r["email"] and r["email"].lower() not in seen]
print(f"new rows: {len(new)}")

# Find which didn't enrich via the normal path
ph = enrich_by_email([r["email"] for r in new])
missing = [r for r in new if r["email"].lower() not in ph]
print(f"missing from PostHog: {len(missing)}")

# Sample 10
sample = missing[:10]
for r in sample:
    e = r["email"].lower()
    print(f"\n--- {e} ---")
    # Strategy 1: any event with this email anywhere
    q1 = f"""
    SELECT event, count() AS n
    FROM events
    WHERE (lower(properties.$set.email) = '{e}'
        OR lower(properties.email) = '{e}'
        OR lower(person.properties.email) = '{e}'
        OR lower(distinct_id) = '{e}')
    GROUP BY event
    ORDER BY n DESC
    LIMIT 10
    """
    rows = _query(q1)
    if rows:
        print("  found events:")
        for ev, n in rows:
            print(f"    {n:>5}  {ev}")
    else:
        print("  NO events at all for this email (likely adblocker / bot / never reached PostHog)")
