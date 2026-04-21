"""Quality scoring — direct port of waitlist_scorer_v2.py.

8 modules (M1-M8). Verdict logic:
    STRONG signals (single Red -> final Red):
        M1 Email Quality
        M2 Name Quality
        M3 IP + Subnet
        M8 Email-Name Consistency (Red = no-vowel username in free email)

    WEAK signals (Yellow/Green only):
        M4 Session Duration, M5 Engagement,
        M6 Geo Resolution, M7 Location Risk,
        M8 (Yellow only for no-match)

    3+ Yellows across all metrics -> Red
    0 Reds AND <=2 Yellows -> Green
    otherwise -> Yellow

For daily incremental pipeline usage, M3 (IP/subnet) needs the global IP
counters. Call `build_ip_context(rows)` first, pass the returned dict to
`score_row`.
"""
import re
from collections import Counter, defaultdict
from pathlib import Path

# ----------------------------------------------------------------------------
# Constants — lifted verbatim from waitlist_scorer_v2.py
# ----------------------------------------------------------------------------
TEAM_IPS = {"182.156.5.2", "1.6.182.170", "52.119.82.138", "106.51.82.104"}
WHITELIST_SUBNETS = {"172.225.219", "172.225.220"}  # WeWork Bengaluru

DATACENTER_CITIES = {
    "ashburn", "boardman", "council bluffs", "the dalles",
    "frankfurt am main", "frankfurt", "dublin",
    "seoul", "tokyo", "sydney", "são paulo",
}

SAFE_COUNTRIES = {
    "india", "united states", "canada", "australia", "new zealand",
    "japan", "south korea", "singapore", "israel",
    "united kingdom", "germany", "france", "the netherlands", "ukraine",
    "spain", "poland", "finland", "sweden", "italy", "portugal",
    "ireland", "denmark", "norway", "switzerland", "austria", "belgium",
    "czech republic", "romania", "hungary", "greece", "serbia", "croatia",
    "bulgaria", "lithuania", "latvia", "estonia", "slovakia", "slovenia",
    "luxembourg", "iceland", "malta", "cyprus", "türkiye", "turkey",
    "north macedonia", "montenegro", "albania", "moldova",
    "bosnia and herzegovina",
}

GMAIL_TYPOS = {
    "gmail.con", "gmai.com", "gmaip.com", "gmmail.com",
    "gma.com", "gmial.com", "gailim.com", "gmil.com",
    "gamil.com", "gnail.com", "gmail.om", "gmail.co",
}

KNOWN_FAKE_DOMAINS = {
    "gailim.com", "xintaitong.com", "rehearsalk.com", "test.com",
    "bbi.xintaitong.com", "vvo.rehearsalk.com",
    "youngestsd.com", "zzr.youngestsd.com",
    "lingeringp.com", "vve.lingeringp.com",
}

FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "me.com", "live.com", "protonmail.com",
    "proton.me", "pm.me", "hey.com", "qq.com", "mail.ru",
    "163.com", "yandex.com", "aol.com",
}

FREE_SUSPICIOUS_TLDS = {".my.id", ".tk", ".ml", ".ga", ".cf", ".gq"}

# ----------------------------------------------------------------------------
# Load disposable-domains list (one-time)
# ----------------------------------------------------------------------------
_DISPOSABLE_PATH = Path(__file__).parent / "disposable_domains.txt"
try:
    DISPOSABLE_DOMAINS = {
        line.strip().lower()
        for line in _DISPOSABLE_PATH.read_text().splitlines()
        if line.strip()
    }
except FileNotFoundError:
    DISPOSABLE_DOMAINS = set()


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def is_free_email(email: str) -> bool:
    if not email or "@" not in email:
        return True
    return email.split("@")[-1].strip().lower() in FREE_EMAIL_DOMAINS


# ----------------------------------------------------------------------------
# IP context — precomputed across the whole batch before scoring
# ----------------------------------------------------------------------------
def build_ip_context(rows: list[dict]) -> dict:
    """Build {ip_counter, subnet_data, subnet16_data} from the batch.

    Expects rows to have keys: ip_address, email, country
    Duplicates and team IPs are excluded from counters (matches v2).
    """
    # dedupe by email
    seen = set()
    active = []
    for r in rows:
        e = (r.get("email") or "").strip().lower()
        if not e or e in seen:
            continue
        seen.add(e)
        if (r.get("ip_address") or "").strip() in TEAM_IPS:
            continue
        active.append(r)

    ip_counter = Counter((r.get("ip_address") or "").strip() for r in active)

    subnet_data = defaultdict(lambda: {"count": 0, "ips": set(), "emails": set()})
    subnet16_data = defaultdict(
        lambda: {"count": 0, "ips": set(), "emails": set(), "countries": set()}
    )
    for r in active:
        ip = (r.get("ip_address") or "").strip()
        if not ip or "." not in ip:
            continue
        email = (r.get("email") or "").strip().lower()
        country = (r.get("country") or "").strip().lower()

        subnet = ".".join(ip.split(".")[:3])
        subnet_data[subnet]["count"] += 1
        subnet_data[subnet]["ips"].add(ip)
        subnet_data[subnet]["emails"].add(email)

        s16 = ".".join(ip.split(".")[:2])
        subnet16_data[s16]["count"] += 1
        subnet16_data[s16]["ips"].add(ip)
        subnet16_data[s16]["emails"].add(email)
        subnet16_data[s16]["countries"].add(country)

    return {
        "ip_counter": ip_counter,
        "subnet_data": subnet_data,
        "subnet16_data": subnet16_data,
    }


# ----------------------------------------------------------------------------
# Metric functions — verbatim from waitlist_scorer_v2.py
# ----------------------------------------------------------------------------
def score_email_quality(email: str) -> str:
    """M1: Email Quality — Red/Green only."""
    if not email or "@" not in email:
        return "Red"
    domain = email.split("@")[-1].strip().lower()

    if domain in DISPOSABLE_DOMAINS:
        return "Red"
    if domain in KNOWN_FAKE_DOMAINS:
        return "Red"
    if domain in GMAIL_TYPOS:
        return "Red"
    parts = domain.split(".")
    if len(parts) > 2:
        parent = ".".join(parts[-2:])
        if parent in KNOWN_FAKE_DOMAINS or parent in DISPOSABLE_DOMAINS:
            return "Red"
    for tld in FREE_SUSPICIOUS_TLDS:
        if domain.endswith(tld):
            return "Red"
    return "Green"


def score_name_quality(first_name: str, last_name: str) -> str:
    """M2: Name Quality."""
    fn = (first_name or "").strip()
    ln = (last_name or "").strip()
    fn_l = fn.lower()
    ln_l = ln.lower()

    if not fn and not ln:
        return "Red"
    # Bot tell: pasted the email into the last_name (or first_name) field.
    if "@" in fn or "@" in ln:
        return "Red"
    if fn_l and ln_l and fn_l == ln_l:
        return "Red"
    if fn and fn.replace(" ", "").isdigit():
        return "Red"
    if ln and ln.replace(" ", "").isdigit():
        return "Red"
    vowels = set("aeiouAEIOU")
    combined = fn + ln
    alpha = re.findall(r"[a-zA-Z]", combined)
    if alpha and not any(c in vowels for c in alpha):
        return "Red"
    if re.search(r"(.)\1{3,}", combined):
        return "Red"

    # Bot tell: digits in names. Real people don't put numbers in their
    # first/last name fields; bot form-fillers frequently do
    # (e.g. "OrbitLinkX41", "Boost333", "CanyonXX059").
    if fn and re.search(r"\d", fn):
        return "Red"
    if ln and re.search(r"\d", ln):
        return "Red"

    fn_short = fn and len(fn.strip()) <= 2
    ln_short = ln and len(ln.strip()) <= 2
    if fn_short and ln_short:
        return "Yellow"
    return "Green"


def score_ip_subnet(ip: str, city: str, email: str, is_launch_day: bool, ctx: dict) -> str:
    """M3: IP + Subnet Frequency."""
    ip = (ip or "").strip()
    if not ip:
        return "Yellow"
    if ip in TEAM_IPS:
        return "Green"

    subnet = ".".join(ip.split(".")[:3]) if "." in ip else ""
    if subnet in WHITELIST_SUBNETS:
        return "Green"

    ip_counter = ctx["ip_counter"]
    subnet_data = ctx["subnet_data"]
    subnet16_data = ctx["subnet16_data"]

    count = ip_counter.get(ip, 0)
    subnet_count = subnet_data[subnet]["count"] if subnet else 0

    if is_launch_day:
        return "Green"

    if count >= 3:
        return "Red"

    if subnet_count >= 3:
        emails = subnet_data[subnet].get("emails", set())
        total = len([e for e in emails if e])
        free = sum(1 for e in emails if e and is_free_email(e))
        free_pct = (free / total * 100) if total else 0
        if free_pct > 80:
            return "Red" if is_free_email(email) else "Green"
        if not is_free_email(email):
            return "Green"
        return "Yellow"

    s16 = ".".join(ip.split(".")[:2]) if "." in ip else ""
    if s16:
        s16d = subnet16_data[s16]
        s16_count = s16d["count"]
        s16_ips = len(s16d["ips"])
        countries = s16d["countries"] - {""}
        is_safe = any(c in SAFE_COUNTRIES for c in countries)
        if s16_count >= 3 and s16_ips >= 3 and not is_safe:
            emails = s16d.get("emails", set())
            total = len([e for e in emails if e])
            free = sum(1 for e in emails if e and is_free_email(e))
            free_pct = (free / total * 100) if total else 0
            if free_pct > 80:
                return "Red" if is_free_email(email) else "Green"

    if count == 2:
        return "Yellow"
    return "Green"


def score_session_duration(duration_str) -> str:
    """M4: Session Duration — Yellow/Green only.

    Threshold: <15s → Yellow. Signup forms don't take long, so we only
    flag *very* brief visits. (v2 used 25s; tuned down for this product.)
    """
    try:
        d = float(duration_str or 0)
    except Exception:
        d = 0
    return "Yellow" if d < 15 else "Green"


def score_engagement(pageview_str, autocapture_str) -> str:
    """M5: Engagement — Yellow/Green only."""
    try:
        pv = int(float(pageview_str or 0))
    except Exception:
        pv = 0
    try:
        ac = int(float(autocapture_str or 0))
    except Exception:
        ac = 0

    if pv == 0:
        return "Yellow"
    if pv > 50:
        return "Yellow"
    if ac <= 2:
        return "Yellow"
    if pv == 1 and ac <= 10:
        return "Yellow"
    return "Green"


def score_geo_resolution(country: str, city: str) -> str:
    """M6: Geo Resolution."""
    country_l = (country or "").strip().lower()
    city_l = (city or "").strip().lower()
    if not city_l or not country_l:
        return "Yellow"
    return "Green"


def score_location_risk(country: str, city: str) -> str:
    """M7: Location Risk."""
    country_l = (country or "").strip().lower()
    city_l = (city or "").strip().lower()
    if city_l in DATACENTER_CITIES:
        return "Yellow"
    if country_l and country_l not in SAFE_COUNTRIES:
        return "Yellow"
    return "Green"


def score_email_name_consistency(email: str, first_name: str, last_name: str) -> str:
    """M8: Email-Name Consistency — only applies to free email domains."""
    if not email or "@" not in email:
        return "Yellow"
    if not is_free_email(email):
        return "Green"

    username = email.split("@")[0].strip().lower()
    fn = (first_name or "").strip().lower()
    ln = (last_name or "").strip().lower()
    if not username:
        return "Yellow"

    alpha_only = re.sub(r"[^a-z]", "", username)
    if alpha_only and not re.search(r"[aeiou]", alpha_only):
        return "Red"

    if fn and len(fn) >= 3 and fn in username:
        return "Green"
    if ln and len(ln) >= 3 and ln in username:
        return "Green"
    if alpha_only and len(alpha_only) >= 3:
        if fn and alpha_only in fn:
            return "Green"
        if ln and alpha_only in ln:
            return "Green"
    if fn and len(fn) >= 3 and fn[:3] in username:
        return "Green"
    if ln and len(ln) >= 3 and ln[:3] in username:
        return "Green"
    return "Yellow"


# ----------------------------------------------------------------------------
# Final verdict
# ----------------------------------------------------------------------------
def final_verdict(m1, m2, m3, m4, m6, m7, m8, m5=None) -> str:
    """Roll up module scores into a final verdict.

    M5 (engagement) was removed — a single autocapture/pageview count is
    too noisy for a one-page signup form. Kept as an optional kwarg so
    callers passing 8 args don't break during migration.
    """
    all_scores = [m1, m2, m3, m4, m6, m7, m8]
    yellow_count = sum(1 for s in all_scores if s == "Yellow")
    red_count = sum(1 for s in all_scores if s == "Red")

    # Strong-signal Reds
    if m1 == "Red" or m2 == "Red" or m3 == "Red" or m8 == "Red":
        return "Red"
    if yellow_count >= 3:
        return "Red"
    if red_count == 0 and yellow_count <= 2:
        return "Green"
    return "Yellow"


# ----------------------------------------------------------------------------
# Row-level wrapper used by pipeline.py
# ----------------------------------------------------------------------------
def score_row(row: dict, ctx: dict) -> tuple[str, dict]:
    """Return (verdict, {M1..M8}).

    row needs: email, first_name, last_name, ip_address, city, country,
               session_duration, pageview_count, autocapture_count, timestamp
    """
    email = (row.get("email") or "").strip().lower()
    is_launch_day = "2026-04-02" in str(row.get("timestamp") or "")

    m1 = score_email_quality(email)
    m2 = score_name_quality(row.get("first_name", ""), row.get("last_name", ""))
    m3 = score_ip_subnet(row.get("ip_address", ""), row.get("city", ""),
                         email, is_launch_day, ctx)
    m4 = score_session_duration(row.get("session_duration", ""))
    # M5 (engagement) removed — pageview/autocapture noise on a one-page
    # signup form was causing legitimate Red false positives.
    m6 = score_geo_resolution(row.get("country", ""), row.get("city", ""))
    m7 = score_location_risk(row.get("country", ""), row.get("city", ""))
    m8 = score_email_name_consistency(email, row.get("first_name", ""),
                                       row.get("last_name", ""))

    verdict = final_verdict(m1, m2, m3, m4, m6, m7, m8)
    return verdict, {
        "M1": m1, "M2": m2, "M3": m3, "M4": m4,
        "M6": m6, "M7": m7, "M8": m8,
    }
