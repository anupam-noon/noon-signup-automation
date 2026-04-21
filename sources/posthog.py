"""Enrich signups with PostHog data from the `waitlist_submitted` event.

Uses the same shape as the manual PostHog HogQL query used for analysis:
- Session metrics come from the `session.*` fields (already aggregated)
- Geo comes from person properties ($set.*)
- IP from the event itself
"""
import requests
from config import cfg


def _query(hogql: str) -> list[list]:
    """Run a HogQL query. Returns the `results` array."""
    url = f"{cfg.posthog_host}/api/projects/{cfg.posthog_project_id}/query/"
    body = {"query": {"kind": "HogQLQuery", "query": hogql}}
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {cfg.posthog_token}"},
        json=body,
        timeout=120,
    )
    r.raise_for_status()
    return r.json().get("results", [])


def enrich_by_email(emails: list[str]) -> dict[str, dict]:
    """Return {email_lowercase: {ip, country, city, pageviews, autocaptures, session_sec}}.

    Pulls the *latest* waitlist_submitted event per email.
    Skips emails with no event.
    """
    if not emails:
        return {}
    normalised = sorted({e.strip().lower() for e in emails if e})
    out: dict[str, dict] = {}
    BATCH = 500
    for i in range(0, len(normalised), BATCH):
        out.update(_enrich_chunk(normalised[i:i + BATCH]))
    return out


def _enrich_chunk(emails: list[str]) -> dict[str, dict]:
    in_list = ",".join(f"'{e.replace(chr(39), chr(39) * 2)}'" for e in emails)
    # Grab the latest waitlist_submitted per email; no time filter since
    # the event is definitionally tied to the signup moment.
    hogql = f"""
    SELECT
        lower(properties.$set.email)                AS email,
        argMax(properties.$ip, timestamp)           AS ip,
        argMax(properties.$set."$geoip_country_name", timestamp) AS country,
        argMax(properties.$set."$geoip_city_name",    timestamp) AS city,
        argMax(session.$pageview_count,    timestamp) AS pageviews,
        argMax(session.$autocapture_count, timestamp) AS autocaptures,
        argMax(session.$session_duration,  timestamp) AS session_sec
    FROM events
    WHERE event = 'waitlist_submitted'
      AND lower(properties.$set.email) IN ({in_list})
    GROUP BY email
    LIMIT 1000000
    """
    out: dict[str, dict] = {}
    for row in _query(hogql):
        email, ip, country, city, pv, ac, sec = row
        if not email:
            continue
        out[email] = {
            "ip": ip,
            "country": country,
            "city": city,
            "pageviews": pv if pv is not None else 0,
            "autocaptures": ac if ac is not None else 0,
            "session_sec": sec if sec is not None else 0,
        }
    return out


if __name__ == "__main__":
    import sys
    sample = sys.argv[1:] or ["anupam@noon.studio"]
    print(enrich_by_email(sample))
