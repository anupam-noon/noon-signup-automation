"""Regression tests — direct port of waitlist_scorer_v2.py behaviour.

These tests exercise each module function and the final_verdict rollup.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from rules import (
    score_email_quality, score_name_quality, score_ip_subnet,
    score_session_duration, score_engagement, score_geo_resolution,
    score_location_risk, score_email_name_consistency, final_verdict,
    build_ip_context, score_row,
)


# ---- M1: email quality ----
def test_m1_disposable_is_red():
    assert score_email_quality("x@mailinator.com") == "Red" or \
           score_email_quality("x@uhodl.space") == "Red"

def test_m1_gmail_typo_is_red():
    assert score_email_quality("a@gmail.con") == "Red"

def test_m1_clean_is_green():
    assert score_email_quality("jane@acme.com") == "Green"


# ---- M2: name quality ----
def test_m2_both_empty_is_red():
    assert score_name_quality("", "") == "Red"

def test_m2_same_fn_ln_is_red():
    assert score_name_quality("John", "John") == "Red"

def test_m2_no_vowels_is_red():
    assert score_name_quality("Xkjhqz", "Bvnmwqrt") == "Red"

def test_m2_short_both_is_yellow():
    assert score_name_quality("A", "B") == "Yellow"

def test_m2_normal_is_green():
    assert score_name_quality("Jane", "Doe") == "Green"

def test_m2_at_in_name_is_red():
    # Bot pasted email into last_name field
    assert score_name_quality("OrbitLinkX41", "foo@bar.com") == "Red"
    assert score_name_quality("foo@bar.com", "Doe") == "Red"

def test_m2_digits_in_name_is_red():
    assert score_name_quality("OrbitLinkX41", "Boost333") == "Red"
    assert score_name_quality("Jane", "Doe99") == "Red"


# ---- M3: IP / subnet ----
def _ctx(rows):
    return build_ip_context(rows)

def test_m3_same_ip_3x_is_red():
    rows = [
        {"email": "a@x.com", "ip_address": "1.2.3.4", "country": "india"},
        {"email": "b@x.com", "ip_address": "1.2.3.4", "country": "india"},
        {"email": "c@x.com", "ip_address": "1.2.3.4", "country": "india"},
    ]
    ctx = _ctx(rows)
    assert score_ip_subnet("1.2.3.4", "", "a@x.com", False, ctx) == "Red"

def test_m3_same_ip_2x_is_yellow():
    rows = [
        {"email": "a@x.com", "ip_address": "1.2.3.4", "country": "india"},
        {"email": "b@x.com", "ip_address": "1.2.3.4", "country": "india"},
    ]
    ctx = _ctx(rows)
    assert score_ip_subnet("1.2.3.4", "", "a@x.com", False, ctx) == "Yellow"

def test_m3_team_ip_is_green():
    ctx = _ctx([])
    assert score_ip_subnet("182.156.5.2", "", "a@acme.com", False, ctx) == "Green"

def test_m3_launch_day_is_green():
    rows = [{"email": f"a{i}@x.com", "ip_address": "5.5.5.5", "country": "india"} for i in range(5)]
    ctx = _ctx(rows)
    assert score_ip_subnet("5.5.5.5", "", "a0@x.com", True, ctx) == "Green"


# ---- M4: session (threshold: <15s → Yellow) ----
def test_m4_very_short_is_yellow():
    assert score_session_duration(10) == "Yellow"

def test_m4_at_threshold_is_green():
    assert score_session_duration(15) == "Green"

def test_m4_long_is_green():
    assert score_session_duration(60) == "Green"


# ---- M5 removed — signup form engagement too noisy to be useful ----


# ---- M6/M7 ----
def test_m6_missing_country_is_yellow():
    assert score_geo_resolution("", "Mumbai") == "Yellow"

def test_m7_unsafe_country_is_yellow():
    assert score_location_risk("Vietnam", "Hanoi") == "Yellow"

def test_m7_safe_country_is_green():
    assert score_location_risk("India", "Mumbai") == "Green"


# ---- M8 ----
def test_m8_corporate_is_green():
    assert score_email_name_consistency("jane@acme.com", "Any", "Name") == "Green"

def test_m8_no_vowel_free_username_is_red():
    assert score_email_name_consistency("xjzqkl@gmail.com", "Jane", "Doe") == "Red"

def test_m8_name_in_username_is_green():
    assert score_email_name_consistency("jane.doe@gmail.com", "Jane", "Doe") == "Green"

def test_m8_mismatch_is_yellow():
    assert score_email_name_consistency("random123@gmail.com", "Priya", "Sharma") == "Yellow"


# ---- final_verdict (7 modules: M1,M2,M3,M4,M6,M7,M8) ----
def test_verdict_any_m1_red_is_red():
    assert final_verdict("Red", "Green", "Green", "Green", "Green", "Green", "Green") == "Red"

def test_verdict_3_yellows_is_red():
    assert final_verdict("Green", "Green", "Green", "Yellow", "Yellow", "Yellow", "Green") == "Red"

def test_verdict_clean_is_green():
    assert final_verdict("Green", "Green", "Green", "Green", "Green", "Green", "Green") == "Green"

def test_verdict_2_yellows_is_green():
    assert final_verdict("Green", "Green", "Green", "Yellow", "Yellow", "Green", "Green") == "Green"


# ---- end-to-end via score_row ----
def test_score_row_clean_green():
    rows = [{
        "email": "jane.doe@acme.com", "first_name": "Jane", "last_name": "Doe",
        "ip_address": "203.0.113.5", "city": "Mumbai", "country": "India",
        "session_duration": 60, "pageview_count": 5, "autocapture_count": 15,
        "timestamp": "2026-04-15",
    }]
    ctx = build_ip_context(rows)
    v, m = score_row(rows[0], ctx)
    assert v == "Green", (v, m)


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ok   {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
