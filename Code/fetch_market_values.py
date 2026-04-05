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

import argparse
import random
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, r'c:\Code_Learning\repos\transfermarkt-api')

from app.services.players.market_value import TransfermarktPlayerMarketValue
from app.services.players.player_profile import TransfermarktPlayerProfile

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'

# ---------- tuning knobs ----------
REQUEST_DELAY_SECONDS = 3.0   # base pause between every API call
DELAY_JITTER          = 1.0   # added random jitter: actual delay = base + U(0, jitter)
MAX_RETRIES           = 3     # attempts per player before giving up
RETRY_BASE_DELAY      = 10    # seconds; doubles each retry (10, 20, 40 …)
SAVE_EVERY            = 20    # write to disk after this many players
# ----------------------------------

MV_COLUMN       = 'Market Value History'
PROFILE_COLUMNS = ['Player Country', 'Date of Birth']


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sleep():
    """Polite pause between requests."""
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


def _do_fetch_mv(player_id: str) -> dict | None:
    try:
        return TransfermarktPlayerMarketValue(player_id=player_id).get_player_market_value()
    except Exception as e:
        print(f"    [MV error] {e}")
        return None


def _do_fetch_profile(player_id: str) -> dict | None:
    try:
        return TransfermarktPlayerProfile(player_id=player_id).get_player_profile()
    except Exception as e:
        print(f"    [Profile error] {e}")
        return None


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------

def process_year(year: int) -> bool:
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

    rows_since_save = 0

    for pos, (idx, row) in enumerate(df.iterrows(), start=1):
        pid  = _player_id_str(row.get('Player ID'))
        name = row.get('Player Name', '?')
        needs_any = False

        # -- market value history --
        if pd.isna(row.get(MV_COLUMN)):
            if pid is None:
                df.at[idx, MV_COLUMN] = None
            else:
                print(f"  [{pos}/{total}] MV  {name} (ID {pid})")
                result = _fetch_with_retry(_do_fetch_mv, pid)
                df.at[idx, MV_COLUMN] = result
                needs_any = True

        # -- player profile (country + DOB) --
        if pd.isna(row.get('Player Country')) or pd.isna(row.get('Date of Birth')):
            if pid is None:
                df.at[idx, 'Player Country'] = None
                df.at[idx, 'Date of Birth']  = None
            else:
                print(f"  [{pos}/{total}] Profile  {name} (ID {pid})")
                profile = _fetch_with_retry(_do_fetch_profile, pid)
                if profile:
                    citizenship = profile.get('citizenship') or []
                    df.at[idx, 'Player Country'] = citizenship[0] if citizenship else None
                    df.at[idx, 'Date of Birth']  = profile.get('dateOfBirth')
                else:
                    df.at[idx, 'Player Country'] = None
                    df.at[idx, 'Date of Birth']  = None
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
    args = parser.parse_args()

    print("Transfermarkt data fetcher (resumable, rate-limited)")
    print(f"Request delay: {REQUEST_DELAY_SECONDS}s + up to {DELAY_JITTER}s jitter")
    print(f"Max retries: {MAX_RETRIES}  |  Save every: {SAVE_EVERY} players")
    print(f"Years: {args.years}\n")

    results = {}
    for year in args.years:
        try:
            results[year] = process_year(year)
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
