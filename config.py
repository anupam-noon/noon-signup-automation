"""Centralised env-var loading. Import `cfg` from here."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    sheets_webhook_url: str
    sheets_webhook_secret: str
    posthog_host: str
    posthog_token: str
    posthog_project_id: str
    hubspot_token: str
    dry_run: bool


def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"missing env var: {name}")
    return v


cfg = Config(
    sheets_webhook_url=_require("SHEETS_WEBHOOK_URL"),
    sheets_webhook_secret=_require("SHEETS_WEBHOOK_SECRET"),
    posthog_host=os.getenv("POSTHOG_HOST", "https://us.posthog.com"),
    posthog_token=_require("POSTHOG_TOKEN"),
    posthog_project_id=_require("POSTHOG_PROJECT_ID"),
    hubspot_token=_require("HUBSPOT_TOKEN"),
    dry_run=os.getenv("DRY_RUN", "") in ("1", "true", "True", "yes"),
)
