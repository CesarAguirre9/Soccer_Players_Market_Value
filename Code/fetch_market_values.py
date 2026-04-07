"""
Fetch market value history and player profile data from Transfermarkt for all seasons.

Row-level resumability: for each player, only fetches what is still missing. A player
whose Market Value History cell is already non-null is skipped; same for profile fields.
This means the script is safe to interrupt and restart at any point.

Rate limiting: a configurable delay (REQUEST_DELAY_SECONDS) is inserted between every
API call. The default of 3 seconds keeps the request rate well below what triggers
Transfermarkt's temporary blocks (~1 request per 3 s vs. the burst rate that caused
the original block). Increase this if you see renewed blocking.

Retry logic: each API call is retried up to MAX_RETRIES times with exponential
backoff before being recorded as None (which keeps the row eligible for the next run).

Periodic saves: the output file is written every SAVE_EVERY rows so that progress
is not lost if the script is killed mid-run.

Output per year: Data/UEFA Stats {year}_with_market_values.xlsx
Added / filled columns:
  - Market Value History  (full dict from TransfermarktPlayerMarketValue)
  - Player Country        (first citizenship entry from player profile)
  - Date of Birth         (dateOfBirth string from player profile)

Usage:
    py Code/fetch_market_values.py                # all years 2018-2025
    py Code/fetch_market_values.py --years 2019 2020 2021  # specific years
"""

import ast
import re
import sys
import time
import random
import argparse
import pandas as pd
from collections import Counter
from pathlib import Path
from itertools import cycle

sys.path.insert(0, r'c:\Code_Learning\repos\transfermarkt-api')

from bs4 import BeautifulSoup
from app.services.base import _get_session

# Alternate between .us and .com on every request — each domain sees half the traffic,
# allowing a shorter per-request delay without increasing load on either server.
# NOTE: URL is a dataclass field with a baked-in default, so class-level patching does not
# affect new instances. We pass URLs explicitly at instantiation time instead.
_domain_cycle = cycle(['transfermarkt.us', 'transfermarkt.com'])
_current_domain = 'transfermarkt.us'  # updated by _sleep() before every request

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'

# ---------- tuning knobs ----------
REQUEST_DELAY_SECONDS = 3.0   # base pause between every API call
DELAY_JITTER          = 1.5   # added random jitter: actual delay = base + U(0, jitter)
MAX_RETRIES           = 3     # attempts per player before giving up
RETRY_BASE_DELAY      = 5    # seconds; doubles each retry (10, 20, 40 …)
SAVE_EVERY            = 20    # write to disk after this many players
# ----------------------------------

MV_COLUMN       = 'Market Value History'
PROFILE_COLUMNS = ['Player Country', 'Date of Birth']

# Wikidata sometimes returns long official country names; map them to the short
# form used everywhere else in this dataset.
_COUNTRY_NORM = {
    "Kingdom of the Netherlands":            "Netherlands",
    "People's Republic of China":            "China",
    "Republic of Korea":                     "South Korea",
    "Democratic People's Republic of Korea": "North Korea",
    "Russian Federation":                    "Russia",
    "United States of America":              "United States",
    "Bosnia and Herzegovina":                "Bosnia-Herzegovina",
    "Republic of Ireland":                   "Ireland",
    "Slovak Republic":                       "Slovakia",
    "Czech Republic":                        "Czech Republic",
    "Federal Republic of Nigeria":           "Nigeria",
    "Republic of Cameroon":                  "Cameroon",
    "Islamic Republic of Iran":              "Iran",
    "Republic of Ivory Coast":               "Ivory Coast",
    "Côte d'Ivoire":                         "Ivory Coast",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sleep():
    """Rotate domain then pause before the next request."""
    global _current_domain
    _current_domain = next(_domain_cycle)
    time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, DELAY_JITTER))


def _player_id_str(raw) -> str | None:
    if pd.isna(raw):
        return None
    try:
        return str(int(float(raw)))
    except (ValueError, TypeError):
        return None


def _fetch_with_retry(fetch_fn, *args) -> object | None:
    """Call fetch_fn(*args), retrying on failure with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        _sleep()
        result = fetch_fn(*args)
        if result is not None:
            return result
        if attempt < MAX_RETRIES - 1:
            wait = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"      Retry {attempt + 1}/{MAX_RETRIES - 1} in {wait}s …")
            time.sleep(wait)
    return None


def _fmt_error(e: Exception) -> str:
    """Return a readable string for any exception, including FastAPI HTTPException."""
    status = getattr(e, 'status_code', '')
    detail = getattr(e, 'detail', None) or str(e) or type(e).__name__
    return f"{status} {detail}".strip()


def _parse_date_simple(s: str):
    """Parse a date string from MV history entries (for birth year derivation)."""
    from datetime import datetime
    for fmt in ('%m/%d/%Y', '%b %d, %Y', '%B %d, %Y'):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except (ValueError, AttributeError):
            pass
    return None


def _parse_ceapi_history(data) -> list:
    """Convert ceapi JSON response into the standard marketValueHistory format."""
    if not isinstance(data, dict):
        return []
    entries = data.get('list', [])
    club_image = None
    result = []
    for entry in entries:
        wappen = entry.get('wappen') or club_image
        if entry.get('wappen'):
            club_image = entry['wappen']
        m = re.search(r'(\d+)', wappen or '')
        result.append({
            'date':     entry.get('datum_mw', ''),
            'age':      str(entry.get('age', '')),
            'clubName': entry.get('verein', ''),
            'clubID':   m.group(1) if m else None,
            'value':    entry.get('mw', ''),
        })
    return result


def _derive_birth_date(mv_raw) -> str | None:
    """
    Derive an approximate birth date from MV history as an ISO string "YYYY-MM-DD".

    Each snapshot has 'date' and 'age' (integer years). When consecutive snapshots
    show age ticking from N to N+1, the birthday falls between those two dates —
    we use the later snapshot's month as the approximate birth month (accuracy
    typically ±1–3 months, far better than year-only).

    Falls back to July 1 of the derived birth year when no age transition is found
    (mid-year assumption minimises average error to ±6 months).
    """
    if mv_raw is None:
        return None
    if isinstance(mv_raw, dict):
        history = mv_raw.get('marketValueHistory', [])
    elif isinstance(mv_raw, float):
        return None  # NaN
    else:
        s = re.sub(r'datetime\.datetime\([^)]+\)', 'None', str(mv_raw).strip())
        try:
            data = ast.literal_eval(s)
            history = data.get('marketValueHistory', []) if isinstance(data, dict) else []
        except Exception:
            return None

    dated = []
    for entry in history:
        d = _parse_date_simple(str(entry.get('date', '')))
        age_s = str(entry.get('age', ''))
        if d and age_s.isdigit():
            dated.append((d, int(age_s)))

    if not dated:
        return None

    dated.sort(key=lambda x: x[0])

    # Find first age transition (N → N+1); birthday is between those two dates.
    # Use the later snapshot's month/day as the approximate birthday.
    for i in range(len(dated) - 1):
        d_after, age_after = dated[i + 1]
        d_before, age_before = dated[i]
        if age_after == age_before + 1:
            birth_year = d_after.year - age_after
            try:
                import calendar
                last_day = calendar.monthrange(birth_year, d_after.month)[1]
                from datetime import date as _date
                return _date(birth_year, d_after.month, min(d_after.day, last_day)).isoformat()
            except (ValueError, OverflowError):
                return f"{birth_year}-{d_after.month:02d}-01"

    # No transition found — derive year by majority vote, assume mid-year.
    years = [d.year - a for d, a in dated]
    birth_year = Counter(years).most_common(1)[0][0]
    return f"{birth_year}-07-01"


def _do_fetch_mv(player_id: str, player_name: str = '') -> dict | None:
    """Fetch MV history directly from the ceapi JSON endpoint — no HTML page needed."""
    d = _current_domain
    url = f"https://www.{d}/ceapi/marketValueDevelopment/graph/{player_id}"
    try:
        resp = _get_session().get(url, timeout=15)
        if resp.status_code != 200:
            print(f"    [MV error] {resp.status_code} {resp.reason} for url: {url}")
            return None
        history = _parse_ceapi_history(resp.json())
        return {'marketValueHistory': history} if history else None
    except Exception as e:
        print(f"    [MV error] {_fmt_error(e)}")
        return None


def _do_fetch_profile(player_id: str, player_name: str = '') -> tuple[str | None, str | None]:
    """
    Fetch DOB and nationality from the Transfermarkt player profile page in a
    single HTTP request.  Returns (dob_iso, country) — either may be None if
    the page is blocked or the field is absent.

    Single attempt, no retry: the profile page blocks like the schnellsuche
    search page; retrying would extend the block window.  The caller falls back
    to _derive_birth_date() for DOB and leaves country as NaN for the next run.
    """
    d = _current_domain
    url = f"https://www.{d}/-/profil/spieler/{player_id}"
    try:
        _sleep()
        resp = _get_session().get(url, timeout=15)
        if resp.status_code != 200:
            print(f"    [Profile {player_name}] {resp.status_code}")
            return None, None
        soup = BeautifulSoup(resp.content, 'html.parser')

        # -- DOB --
        dob = None
        tag = soup.find(itemprop='birthDate')
        if tag:
            raw = tag.get_text(strip=True)
            m = re.match(r'^(.+?)\s*\(\d+\)', raw)
            dob_str = m.group(1).strip() if m else raw.strip()
            d_obj = _parse_date_simple(dob_str)
            dob = d_obj.isoformat() if d_obj else None

        # -- Nationality: first flag image on the page = primary nationality --
        country = None
        flag = soup.find('img', class_='flaggenrahmen')
        if flag:
            country = flag.get('title')

        return dob, country
    except Exception as e:
        print(f"    [Profile error] {_fmt_error(e)}")
        return None, None


# ---------------------------------------------------------------------------
# Wikidata bulk country lookup
# ---------------------------------------------------------------------------

def _fetch_countries_wikidata(player_ids: list[str]) -> dict[str, str]:
    """
    Bulk-fetch player nationalities from Wikidata via the free SPARQL endpoint,
    using Transfermarkt player IDs (property P2446) as the lookup key.

    Sends POST requests in batches of 200 to stay within URL/query size limits.
    Returns {player_id_str: country_name}.  Players absent from Wikidata are
    simply not included — the caller leaves their Country cell as NaN.
    """
    import json
    import urllib.parse
    import urllib.request

    ENDPOINT   = "https://query.wikidata.org/sparql"
    BATCH_SIZE = 200

    results: dict[str, str] = {}
    total_batches = (len(player_ids) - 1) // BATCH_SIZE + 1

    for batch_num, offset in enumerate(range(0, len(player_ids), BATCH_SIZE), start=1):
        batch   = player_ids[offset : offset + BATCH_SIZE]
        id_list = ", ".join(f'"{pid}"' for pid in batch)
        query = (
            "SELECT ?tmId ?nationalityLabel WHERE { "
            "  ?player wdt:P2446 ?tmId . "
            "  ?player wdt:P27 ?nationality . "
            f" FILTER(?tmId IN ({id_list})) "
            '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } '
            "}"
        )
        print(f"  [Wikidata] batch {batch_num}/{total_batches} ({len(batch)} IDs) …")
        try:
            body = urllib.parse.urlencode({"query": query, "format": "json"}).encode()
            req  = urllib.request.Request(
                ENDPOINT, data=body,
                headers={
                    "User-Agent":   "SoccerMVResearch/1.0 (academic)",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept":       "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
            for row in data["results"]["bindings"]:
                pid = row["tmId"]["value"]
                nat = row.get("nationalityLabel", {}).get("value", "")
                nat = _COUNTRY_NORM.get(nat, nat)
                if pid not in results and nat:   # keep first nationality per player
                    results[pid] = nat
            time.sleep(1)  # polite pause between Wikidata batches
        except Exception as e:
            print(f"  [Wikidata error] batch {batch_num}: {e}")

    return results


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------

def process_year(year: int, limit: int | None = None) -> bool:
    input_file  = DATA_DIR / f'UEFA Stats {year}_with_IDs.xlsx'
    output_file = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'

    print(f"\n{'='*60}")
    print(f"Year: {year}")
    print(f"{'='*60}")

    # ---- load working dataframe ----
    # Prefer the output file if it exists (picks up prior partial progress).
    # Fall back to _with_IDs.xlsx for years that have never been started.
    if output_file.exists():
        df = pd.read_excel(output_file)
        print(f"  Resuming from {output_file.name}  ({len(df)} rows)")
    elif input_file.exists():
        df = pd.read_excel(input_file)
        print(f"  Starting fresh from {input_file.name}  ({len(df)} rows)")
    else:
        print(f"  Neither input nor output file found. Skipping.")
        return False

    # Ensure target columns exist (may be absent on first run)
    for col in [MV_COLUMN] + PROFILE_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Cast profile columns to object dtype so pandas doesn't warn when
    # writing date/country strings into an all-NaN float64 column.
    for col in PROFILE_COLUMNS:
        if df[col].dtype != object:
            df[col] = df[col].astype(object)

    total = len(df)

    # ---- row-level fetch loop ----
    # Only fetch what is currently null; already-filled cells are left alone.
    # This is what makes every restart truly resumable at the row level.
    mv_needed      = df[MV_COLUMN].isna().sum()
    country_needed = df['Player Country'].isna().sum()
    dob_needed     = df['Date of Birth'].isna().sum()

    print(f"  Still needed: MV={mv_needed}, Country={country_needed}, DOB={dob_needed}")

    if mv_needed == 0 and country_needed == 0 and dob_needed == 0:
        print("  All data present. Nothing to do.")
        return True

    # -- Bulk country fill from Wikidata (before row loop) --
    # One SPARQL call per 200 players; no per-player HTTP overhead.
    # Players absent from Wikidata stay NaN and fall through to the
    # profile page attempt inside the row loop.
    if country_needed > 0:
        missing_ids = [
            _player_id_str(r['Player ID'])
            for _, r in df[df['Player Country'].isna()].iterrows()
            if _player_id_str(r['Player ID']) is not None
        ]
        if missing_ids:
            wikidata_map = _fetch_countries_wikidata(missing_ids)
            filled = 0
            for idx, row in df[df['Player Country'].isna()].iterrows():
                pid = _player_id_str(row.get('Player ID'))
                if pid and pid in wikidata_map:
                    df.at[idx, 'Player Country'] = wikidata_map[pid]
                    filled += 1
            print(f"  Wikidata: filled {filled} / {len(missing_ids)} countries")
            if filled:
                df.to_excel(output_file, index=False)
                print(f"  [checkpoint] saved Wikidata fill")

        # Re-check — if Wikidata filled everything, skip the row loop entirely
        country_needed = df['Player Country'].isna().sum()
        if mv_needed == 0 and country_needed == 0 and dob_needed == 0:
            print("  All data present after Wikidata fill. Nothing more to do.")
            df.to_excel(output_file, index=False)
            return True

    rows_since_save = 0

    for pos, (idx, row) in enumerate(df.iterrows(), start=1):
        if limit is not None and pos > limit:
            print(f"  [limit] Stopping after {limit} rows.")
            break
        pid  = _player_id_str(row.get('Player ID'))
        name = row.get('Player Name', '?')
        needs_any = False

        # -- market value history --
        if pd.isna(row.get(MV_COLUMN)):
            if pid is None:
                df.at[idx, MV_COLUMN] = None
            else:
                print(f"  [{pos}/{total}] MV  {name} (ID {pid})")
                result = _fetch_with_retry(_do_fetch_mv, pid, name)
                df.at[idx, MV_COLUMN] = result
                needs_any = True

        # -- profile page: DOB + Country fallback (single request, no retry) --
        # Called only when the profile page can provide something Wikidata/MV can't:
        #   • DOB is missing AND MV history is absent  (can't derive without MV)
        #   • Country is still NaN after the Wikidata bulk fill above
        # Profile page is behind AWS WAF so expect frequent 405s; failure is
        # silent — DOB falls back to MV derivation, Country stays NaN for retry.
        dob_missing     = pd.isna(row.get('Date of Birth'))
        country_missing = pd.isna(row.get('Player Country'))
        mv_present      = not pd.isna(df.at[idx, MV_COLUMN])

        profile_dob, profile_country = None, None
        need_profile = (dob_missing and not mv_present) or country_missing
        if need_profile and pid is not None:
            print(f"  [{pos}/{total}] Profile  {name} (ID {pid})")
            profile_dob, profile_country = _do_fetch_profile(pid, name)

        if dob_missing:
            birth_date = profile_dob or _derive_birth_date(df.at[idx, MV_COLUMN])
            if birth_date:
                df.at[idx, 'Date of Birth'] = birth_date
                needs_any = True

        if country_missing:
            if pid is None:
                df.at[idx, 'Player Country'] = None
            elif profile_country:
                df.at[idx, 'Player Country'] = profile_country
                needs_any = True

        if needs_any:
            rows_since_save += 1
            if rows_since_save >= SAVE_EVERY:
                df.to_excel(output_file, index=False)
                print(f"  [checkpoint] saved after {pos} players")
                rows_since_save = 0

    # Final save
    df.to_excel(output_file, index=False)
    mv_done  = df[MV_COLUMN].notna().sum()
    pc_done  = df['Player Country'].notna().sum()
    dob_done = df['Date of Birth'].notna().sum()
    print(f"\n  Done. MV={mv_done}/{total}, Country={pc_done}/{total}, DOB={dob_done}/{total}")
    print(f"  Saved: {output_file.name}")
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch Transfermarkt data for all seasons.")
    parser.add_argument('--years', nargs='+', type=int, default=list(range(2018, 2026)),
                        help='Years to process (default: 2018-2025)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Stop after processing this many rows per year (for testing)')
    args = parser.parse_args()

    print("Transfermarkt data fetcher (resumable, rate-limited)")
    print(f"Request delay: {REQUEST_DELAY_SECONDS}s + up to {DELAY_JITTER}s jitter")
    print(f"Max retries: {MAX_RETRIES}  |  Save every: {SAVE_EVERY} players")
    print(f"Years: {args.years}\n")

    results = {}
    for year in args.years:
        try:
            results[year] = process_year(year, limit=args.limit)
        except Exception as e:
            print(f"ERROR on year {year}: {e}")
            results[year] = False

    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    for year, ok in results.items():
        print(f"  {year}: {'OK' if ok else 'FAILED'}")


if __name__ == "__main__":
    main()
