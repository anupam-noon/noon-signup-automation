"""Diagnostic: what event names does your PostHog actually record?

Run: python debug_posthog_events.py
"""
from sources.posthog import _query

hogql = """
SELECT event, count() AS n
FROM events
WHERE timestamp > now() - INTERVAL 30 DAY
GROUP BY event
ORDER BY n DESC
LIMIT 50
"""

for row in _query(hogql):
    print(f"  {row[1]:>10}  {row[0]}")
