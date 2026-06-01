# signup-pipeline

Daily 8 PM IST automation:

1. **Pull** new signups from Google Sheet (via Apps Script webhook)
2. **Enrich** with PostHog session data (IP, pageviews, session duration)
3. **Score** each row: Green / Yellow / Fake (using rules ported from `build_master.py`)
4. **Push** Greens + Yellows to HubSpot as new contacts with `send_email_date` stamped
5. HubSpot workflow picks up the stamp and sends the confirmation email

## Files

```
signup-pipeline/
├── README.md                 ← this file
├── requirements.txt          ← Python deps
├── config.py                 ← reads env vars
├── rules.py                  ← green/yellow/fake scoring
├── sources/
│   ├── sheets.py             ← pulls from Apps Script webhook
│   └── posthog.py            ← PostHog enrichment
├── sinks/
│   └── hubspot.py            ← idempotent contact upsert
├── pipeline.py               ← orchestrator
├── state/
│   └── watermark.json        ← last-processed row marker (gitignored)
├── tests/
│   └── test_rules.py         ← regression tests on scoring
└── .github/workflows/
    └── daily.yml             ← 8 PM IST cron
```

## Running locally

```bash
# one-time
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# copy .env.example to .env and fill in credentials
cp .env.example .env

# dry run (no HubSpot writes)
DRY_RUN=1 python pipeline.py

# live run
python pipeline.py
```

## Running on schedule

GitHub Actions runs `pipeline.py` daily at 8 PM IST (14:30 UTC). See `.github/workflows/daily.yml`.

## Environment variables

| Var | Where from |
|---|---|
| `SHEETS_WEBHOOK_URL` | Apps Script `/exec` URL |
| `SHEETS_WEBHOOK_SECRET` | token string |
| `POSTHOG_HOST` | `https://us.posthog.com` |
| `POSTHOG_TOKEN` | PostHog personal API key |
| `POSTHOG_PROJECT_ID` | `362355` |
| `HUBSPOT_TOKEN` | private app access token |
| `DRY_RUN` | `1` to skip HubSpot writes (optional) |

Set locally in `.env`. Set in GitHub via repo Settings → Secrets and variables → Actions.

## Token ownership

| Token | Tied to | Migration-safe? |
|---|---|---|
| `HUBSPOT_TOKEN` | HubSpot **Private App** in the Noon portal — portal-scoped, not user-scoped | Yes, unless the user who created the Private App is deactivated |
| `POSTHOG_TOKEN` | PostHog **personal API key** — tied to one PostHog user | Only if that user's login email is migrated (same user, new email). New user = regenerate. |
| `POSTHOG_PROJECT_ID` | Noon PostHog project (not a secret) | Yes |
| `SHEETS_WEBHOOK_URL` / `SHEETS_WEBHOOK_SECRET` | Apps Script deployed under the Google Sheet owner | Apps Script keeps working but only the original Google account can edit |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook (Noon workspace) | Yes — workspace-scoped, not user-scoped |

GitHub Actions secrets live under the `anupam-noon/noon-signup-automation` repo and are independent of the maintainer's Google login.

## Rotating a token

1. Generate a new value at the source (HubSpot Private App / PostHog personal key / etc.)
2. Update local `.env`
3. Update the GitHub Actions secret:
   ```bash
   gh secret set HUBSPOT_TOKEN --repo anupam-noon/noon-signup-automation
   ```
4. Trigger a manual workflow run (Actions tab → "Daily signup pipeline" → Run workflow) to verify
