"""Orchestrator — ports waitlist_scorer_v2.py into a daily incremental pipeline.

Flow:
    1. Fetch rows from Google Sheet (via Apps Script webhook)
    2. Diff against watermark — only new emails since last run
    3. Enrich via PostHog (country, city, session_duration, pageviews, autocaptures)
    4. Build IP context (counts + subnet stats across today's batch)
    5. Score each row with M1..M8 -> Green/Yellow/Red
    6. Upload Green + Yellow to HubSpot (as "Green") with today's batch label
    7. Advance watermark
"""
import csv
import json
import datetime as dt
from collections import Counter
from pathlib import Path

from config import cfg
from sources.sheets import fetch_rows
from sources.posthog import enrich_by_email
from sinks.hubspot import upsert_contacts, today_batch_label
from rules import build_ip_context, score_row

STATE_PATH = Path(__file__).parent / "state" / "watermark.json"
OUTPUT_DIR = Path(__file__).parent / "output"


def write_output_csv(rows: list[dict]) -> Path:
    """Dump every scored row (with verdict + per-module metrics) to a CSV."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = OUTPUT_DIR / f"classified_{ts}.csv"
    modules = ["M1", "M2", "M3", "M4", "M6", "M7", "M8"]
    headers = [
        "verdict", "timestamp", "email", "first_name", "last_name",
        "ip_address", "city", "country",
        "session_duration", "pageview_count", "autocapture_count",
        "twitter", "linkedin",
        *modules,
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            m = r.get("_metrics", {})
            w.writerow([
                r.get("_verdict", ""),
                r.get("timestamp", ""), r.get("email", ""),
                r.get("first_name", ""), r.get("last_name", ""),
                r.get("ip_address", ""), r.get("city", ""), r.get("country", ""),
                r.get("session_duration", ""), r.get("pageview_count", ""), r.get("autocapture_count", ""),
                r.get("twitter", ""), r.get("linkedin", ""),
                *(m.get(k, "") for k in modules),
            ])
    return path


# ---------------------------------------------------------------------------
# Sanity checks — guard against silently-broken enrichment or rules
# ---------------------------------------------------------------------------
# Countries where we expect mostly-legitimate traffic. If most signups from
# here are Red, something upstream is broken (enrichment, rules, data).
TRUSTED_COUNTRIES = {
    "united states", "canada", "united kingdom",
    "australia", "new zealand",
    "germany", "france", "the netherlands", "netherlands",
    "sweden", "norway", "denmark", "finland", "ireland",
    "switzerland", "austria", "belgium",
}


def sanity_check(rows: list[dict]) -> list[str]:
    """Return a list of failure messages. Empty list = all good.

    Invariants:
      1. Combined across US/Canada/UK/W-Europe, ≥15% should be Green.
         (Combined — not per-country — since any one country may be too
         small a sample on a given day.)
      2. Overall Red share should be ≥20% (launch-time signups attract bots).
         If lower, bot detection isn't firing.
      3. Overall Red share should be ≤95%. If higher, we're over-flagging.
    """
    failures: list[str] = []
    n = len(rows)
    if n == 0:
        return failures

    # ---- Invariant 1: trusted-country green rate ----
    trusted = [
        r for r in rows
        if (r.get("country") or "").strip().lower() in TRUSTED_COUNTRIES
    ]
    if trusted:
        green_trusted = sum(1 for r in trusted if r["_verdict"] == "Green")
        rate = green_trusted / len(trusted)
        print(
            f"sanity: trusted-country green rate = "
            f"{green_trusted}/{len(trusted)} ({rate:.1%})"
        )
        if rate < 0.15:
            failures.append(
                f"trusted-country green rate is {rate:.1%} "
                f"({green_trusted}/{len(trusted)}); expected ≥15% "
                f"combined across US/Canada/UK/W-Europe. "
                f"Likely cause: PostHog enrichment or scoring rules broken."
            )
            # Show a few examples for debugging
            reds = [r for r in trusted if r["_verdict"] == "Red"][:5]
            for r in reds:
                print(
                    f"  example red from {r.get('country')}: {r.get('email')} "
                    f"sd={r.get('session_duration')!r} "
                    f"pv={r.get('pageview_count')!r} "
                    f"ac={r.get('autocapture_count')!r}"
                )
    else:
        print("sanity: no trusted-country signups in this batch (skipping check 1)")

    # ---- Invariant 2 & 3: overall red share ----
    reds = sum(1 for r in rows if r["_verdict"] == "Red")
    red_share = reds / n
    print(f"sanity: overall red share = {reds}/{n} ({red_share:.1%})")
    if red_share < 0.20:
        failures.append(
            f"overall red share is {red_share:.1%}; expected ≥20%. "
            f"Bot detection may not be firing."
        )
    if red_share > 0.95:
        failures.append(
            f"overall red share is {red_share:.1%}; expected ≤95%. "
            f"Over-flagging — check enrichment + M4/M5/M7 thresholds."
        )

    return failures


def load_watermark() -> set[str]:
    if not STATE_PATH.exists():
        return set()
    return set(json.loads(STATE_PATH.read_text()).get("seen_emails", []))


def save_watermark(seen: set[str]) -> None:
    STATE_PATH.parent.mkdir(exist_ok=True)
    STATE_PATH.write_text(json.dumps({
        "seen_emails": sorted(seen),
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }))


def normalise(row: dict) -> dict:
    """Map sheet columns -> keys the v2 scorer expects.

    Sheet: Timestamp | Email | First Name | Last Name | Twitter | LinkedIn |
           IP | Count of Entries for this IP | Minute | RPM | Hour | RPH | Day |
           Email domain
    Scorer needs: email, first_name, last_name, ip_address, city, country,
                  session_duration, pageview_count, autocapture_count, timestamp
    """
    def g(*keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, ""):
                return str(v).strip()
        return ""

    return {
        "timestamp":         g("Timestamp"),
        "email":             g("Email"),
        "first_name":        g("First Name"),
        "last_name":         g("Last Name"),
        "twitter":           g("Twitter"),
        "linkedin":          g("LinkedIn"),
        "ip_address":        g("IP"),
        # filled by PostHog enrichment step below:
        "city":              "",
        "country":           "",
        "session_duration":  "",
        "pageview_count":    "",
        "autocapture_count": "",
    }


SUMMARY_PATH = Path(__file__).parent / "summary.json"


def write_summary(status: str, **fields) -> None:
    """Write a machine-readable summary for the Slack step to consume.

    status: "ok" | "failed"
    stage:  which phase ran/failed (sheet_fetch, posthog_enrich, scoring,
            sanity_check, hubspot_upload)
    reason: human-readable cause (only on failure)
    """
    data = {"status": status, "timestamp": dt.datetime.now().isoformat(), **fields}
    SUMMARY_PATH.write_text(json.dumps(data, indent=2))


def _fail(stage: str, reason: str, **extra) -> None:
    """Record a classified failure and exit non-zero."""
    print(f"\n!!! PIPELINE FAILED at stage={stage}")
    print(f"    reason: {reason}")
    write_summary("failed", stage=stage, reason=reason, **extra)
    import sys
    sys.exit(2)


def main() -> None:
    print(f"=== pipeline run @ {dt.datetime.now().isoformat()} ===")
    print(f"DRY_RUN={cfg.dry_run}")

    # ---- 1. fetch from sheet ----
    try:
        rows = [normalise(r) for r in fetch_rows()]
        rows = [r for r in rows if r["email"]]
    except Exception as e:
        _fail("sheet_fetch", f"Google Sheet webhook unreachable or returned bad data: {e}")
        return
    total_sheet = len(rows)
    print(f"fetched {total_sheet} rows from sheet")
    if not rows:
        write_summary("ok", stage="no_rows", total_sheet=0, new=0,
                      green=0, yellow=0, red=0, dry_run=cfg.dry_run)
        return

    # ---- 2. diff against watermark ----
    seen = load_watermark()
    new_rows = [r for r in rows if r["email"].lower() not in seen]
    print(f"{len(new_rows)} new rows since last run")
    if not new_rows:
        write_summary("ok", stage="no_new_rows", total_sheet=total_sheet,
                      new=0, green=0, yellow=0, red=0, dry_run=cfg.dry_run)
        return

    # ---- 3. enrich via PostHog ----
    try:
        emails = [r["email"] for r in new_rows]
        ph = enrich_by_email(emails)
    except Exception as e:
        _fail("posthog_enrich", f"PostHog query failed (API down or bad token?): {e}",
              total_sheet=total_sheet, new=len(new_rows))
        return
    for r in new_rows:
        d = ph.get(r["email"].lower(), {})
        if d.get("country") is not None: r["country"] = d["country"]
        if d.get("city")    is not None: r["city"] = d["city"]
        if d.get("session_sec")  is not None: r["session_duration"] = d["session_sec"]
        if d.get("pageviews")    is not None: r["pageview_count"] = d["pageviews"]
        if d.get("autocaptures") is not None: r["autocapture_count"] = d["autocaptures"]
    enriched = len(ph)
    print(f"posthog enriched {enriched}/{len(new_rows)} emails")
    if enriched == 0 and len(new_rows) > 10:
        _fail("posthog_enrich",
              f"PostHog returned 0 matches for {len(new_rows)} new emails — "
              f"event name or query shape likely broken",
              total_sheet=total_sheet, new=len(new_rows), enriched=0)
        return

    # ---- 4. IP context + scoring ----
    ctx = build_ip_context(new_rows)
    verdicts = Counter()
    for r in new_rows:
        v, metrics = score_row(r, ctx)
        r["_verdict"] = v
        r["_metrics"] = metrics
        verdicts[v] += 1
    print(f"scored: Green={verdicts['Green']} Yellow={verdicts['Yellow']} Red={verdicts['Red']}")

    out_path = write_output_csv(new_rows)
    print(f"wrote classified CSV: {out_path}")

    counts = {
        "total_sheet": total_sheet,
        "new": len(new_rows),
        "enriched": enriched,
        "green": verdicts["Green"],
        "yellow": verdicts["Yellow"],
        "red": verdicts["Red"],
    }

    # ---- 5. sanity checks ----
    failures = sanity_check(new_rows)
    if failures:
        _fail("sanity_check",
              "Threshold checks failed: " + " | ".join(failures),
              **counts)
        return

    # ---- 6. upload to HubSpot ----
    actionable = [r for r in new_rows if r["_verdict"] in ("Green", "Yellow")]
    if cfg.limit_upload and len(actionable) > cfg.limit_upload:
        print(f"LIMIT_UPLOAD={cfg.limit_upload}: capping upload "
              f"(would have been {len(actionable)})")
        actionable = actionable[:cfg.limit_upload]
    batch = today_batch_label()
    hubspot_result = {"created": 0, "updated": 0, "skipped": 0, "errors": 0, "dry_run": cfg.dry_run}
    if actionable:
        contacts = [
            {
                "email": r["email"],
                "firstname": r["first_name"],
                "lastname": r["last_name"],
                "greenfake_contact": "Green",
                "send_email_date": batch,
            }
            for r in actionable
        ]
        try:
            hubspot_result = upsert_contacts(contacts)
        except Exception as e:
            _fail("hubspot_upload",
                  f"HubSpot upload failed (auth? rate limit?): {e}",
                  **counts, send_email_date=batch)
            return
        print(f"hubspot: {hubspot_result}")

    # ---- 7. advance watermark (skip on dry run) ----
    if cfg.dry_run:
        print("DRY_RUN: watermark NOT advanced")
    else:
        for r in new_rows:
            seen.add(r["email"].lower())
        save_watermark(seen)
        print(f"watermark saved ({len(seen)} total seen emails)")

    # ---- done ----
    write_summary(
        "ok",
        stage="done",
        dry_run=cfg.dry_run,
        send_email_date=batch,
        uploaded=len(actionable),
        hubspot=hubspot_result,
        **counts,
    )


if __name__ == "__main__":
    main()
