"""
Compile the final dataset from all UEFA Stats {year}_with_market_values.xlsx files.

For each year 2018-2025, reads the enriched stats file, then:
- Parses the Market Value History string back into a Python dict.
- Calculates MV_start = average market value in the season's starting year.
- Calculates MV_end   = average market value in the season's ending year.
- Computes the player's age at the start of the CL season (August 1, start_year).
- Looks up Team Country from club_country_map.json.
- Formats the Season label (e.g. "21/22").

Output: Data/Final_Dataset.xlsx  — matches the schema of Data/Data.xlsx.

Prerequisites:
  1. Run fetch_market_values.py  (creates/updates all _with_market_values.xlsx files)
  2. Run build_club_country_map.py  (creates club_country_map.json)
"""

import ast
import json
import re
import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'
OUTPUT_FILE = DATA_DIR / 'Final_Dataset.xlsx'
CLUB_MAP_FILE = DATA_DIR / 'club_country_map.json'


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_mv_history_string(raw) -> list:
    """
    Convert a Market Value History cell (stored as a string repr of a Python dict)
    back to a list of snapshot dicts.

    The cell may contain datetime.datetime(...) literals which ast.literal_eval
    cannot handle, so we replace them with None first.
    """
    if pd.isna(raw) or raw is None:
        return []
    s = str(raw).strip()
    if not s or s in ('None', 'nan'):
        return []
    # Replace datetime.datetime(...) with None
    s = re.sub(r'datetime\.datetime\([^)]+\)', 'None', s)
    try:
        data = ast.literal_eval(s)
    except Exception:
        return []
    if isinstance(data, dict):
        return data.get('marketValueHistory') or []
    return []


def _parse_date(date_str: str) -> date | None:
    """
    Parse a market-value snapshot date.
    Observed formats:
      - "Jul 15, 2020"   (newer Transfermarkt API)
      - "04/10/2004"     (older API, DD/MM/YYYY)
    """
    if not date_str:
        return None
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            from datetime import datetime as dt
            return dt.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_value_euros(value_str: str) -> float | None:
    """
    Convert a market value string to a float in millions of euros.
    Examples: "€50.00m" → 50.0, "€400k" → 0.4, "€1.50m" → 1.5
    """
    if not value_str:
        return None
    m = re.search(r'([\d.,]+)\s*([mk]?)', value_str, re.IGNORECASE)
    if not m:
        return None
    num = float(m.group(1).replace(',', ''))
    suffix = m.group(2).lower()
    if suffix == 'm':
        return num
    elif suffix == 'k':
        return num / 1000
    return num


def calc_mv_for_year(history: list, target_year: int) -> float | None:
    """
    Average all market value snapshots whose date falls in target_year.
    Returns None if no snapshots found for that year.
    """
    values = []
    for entry in history:
        d = _parse_date(entry.get('date', ''))
        if d and d.year == target_year:
            v = _parse_value_euros(entry.get('value', ''))
            if v is not None:
                values.append(v)
    return float(np.mean(values)) if values else None


def calc_age_at_season_start(dob_str: str | None, start_year: int) -> int | None:
    """
    Calculate the player's age on August 1 of start_year (the CL season start).
    """
    if not dob_str or pd.isna(dob_str):
        return None
    dob = _parse_date(str(dob_str))
    if dob is None:
        return None
    season_start = date(start_year, 8, 1)
    return (season_start - dob).days // 365


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------

def process_year(year: int, club_map: dict) -> pd.DataFrame | None:
    """
    Build a partial DataFrame for one CL season (identified by the ending year).

    year 2018 → season "17/18", start_year=2017, end_year=2018
    year 2022 → season "21/22", start_year=2021, end_year=2022
    """
    mv_file = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'
    if not mv_file.exists():
        print(f"  [{year}] File not found: {mv_file.name}  Skipping.")
        return None

    df = pd.read_excel(mv_file)
    print(f"  [{year}] Loaded {len(df)} rows from {mv_file.name}")

    start_year = year - 1
    end_year   = year
    season_label = f"{start_year % 100:02d}/{end_year % 100:02d}"

    mv_start_col = f"MV{start_year % 100:02d}"
    mv_end_col   = f"MV{end_year % 100:02d}"

    mv_starts = []
    mv_ends   = []
    ages      = []

    for _, row in df.iterrows():
        history = _parse_mv_history_string(row.get('Market Value History'))
        mv_starts.append(calc_mv_for_year(history, start_year))
        mv_ends.append(calc_mv_for_year(history, end_year))
        ages.append(calc_age_at_season_start(row.get('Date of Birth'), start_year))

    # Build the output frame with standardised column names
    out = pd.DataFrame()
    out['Season']         = season_label
    out['Player']         = df.get('Player Name', df.get('Player_Name'))
    out['Player Country'] = df.get('Player Country')
    out['Position']       = df.get('Position')
    out['Age']            = ages
    out['Team']           = df.get('Team')
    out['Team Country']   = df['Team'].map(club_map) if 'Team' in df.columns else None
    out['Minutes']        = df.get('Minutes Played', df.get('Minutes_played'))
    out['Matches']        = df.get('Matches')
    out['Goals']          = df.get('Goals')
    out['Assists']        = df.get('Assists')
    out['Distance']       = df.get('Distance Covered', df.get('Distance_covered'))
    out[mv_start_col]     = mv_starts
    out[mv_end_col]       = mv_ends

    # Drop rows where both MV columns are missing (no usable market value data)
    has_mv = out[mv_start_col].notna() | out[mv_end_col].notna()
    dropped = (~has_mv).sum()
    if dropped:
        print(f"    Dropping {dropped} rows with no market value data.")
    out = out[has_mv].reset_index(drop=True)

    print(f"    {len(out)} rows kept for season {season_label}.")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Load club → country mapping
    if not CLUB_MAP_FILE.exists():
        print(f"ERROR: {CLUB_MAP_FILE.name} not found.")
        print("Run build_club_country_map.py first.")
        return

    with open(CLUB_MAP_FILE, 'r', encoding='utf-8') as fh:
        club_map = json.load(fh)
    print(f"Loaded club map: {len(club_map)} entries\n")

    years = list(range(2018, 2026))
    frames = []

    for year in years:
        partial = process_year(year, club_map)
        if partial is not None and not partial.empty:
            frames.append(partial)

    if not frames:
        print("\nNo data to compile.")
        return

    final = pd.concat(frames, ignore_index=True)

    # Add an index column (rank within each season by minutes played, descending)
    final['Rank'] = (
        final.groupby('Season')['Minutes']
        .rank(method='first', ascending=False)
        .astype(int)
    )

    print(f"\nFinal dataset: {len(final)} rows across {final['Season'].nunique()} seasons.")
    final.to_excel(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE.name}")

    # Quick sanity check
    print("\nRows per season:")
    print(final.groupby('Season').size().to_string())

    missing_mv = final.iloc[:, 12].isna().sum()  # MV_start column
    print(f"\nRows missing MV_start: {missing_mv} / {len(final)}")


if __name__ == "__main__":
    main()
