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
    limit_upload: int  # 0 = no limit
    email_queue_list_id: str   # HubSpot list ID for "Multi Email Queue"; empty disables auto-queue
    email_queue_threshold: int # batches strictly below this auto-queue; at/above → manual review


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
    limit_upload=int(os.getenv("LIMIT_UPLOAD", "0") or "0"),
    email_queue_list_id=os.getenv("HUBSPOT_QUEUE_LIST_ID", "").strip(),
    email_queue_threshold=int(os.getenv("EMAIL_QUEUE_THRESHOLD", "500") or "500"),
)
