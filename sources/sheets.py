"""Pull rows from the Google Sheet via the Apps Script web-app webhook."""
from __future__ import annotations
import requests
from config import cfg


def fetch_rows(limit: int | None = None) -> list[dict]:
    """Return all rows from the sheet as a list of dicts keyed by header.

    If `limit` is passed, only the first N rows are returned (useful for testing).
    """
    params = {"token": cfg.sheets_webhook_secret}
    if limit is not None:
        params["limit"] = str(limit)
    r = requests.get(cfg.sheets_webhook_url, params=params, timeout=300)
    r.raise_for_status()
    if r.text.strip() == "unauthorized":
        raise RuntimeError("sheets webhook: unauthorized (check SHEETS_WEBHOOK_SECRET)")
    return r.json()


if __name__ == "__main__":
    # smoke test: python -m sources.sheets
    rows = fetch_rows(limit=5)
    print(f"fetched {len(rows)} rows")
    if rows:
        print("columns:", list(rows[0].keys()))
        print("first row:", rows[0])
