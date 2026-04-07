"""
Read-only audit of all UEFA Stats {year}_with_market_values.xlsx files.

Checks (no API calls, no writes to source files):
  1. Completeness    — rows missing MV history, DOB, or Player Country
  2. DOB plausibility — age at CL final outside [15, 42]
  3. Cross-year consistency — same Player ID with conflicting DOB or Country across years
  4. Match confidence — rows flagged NEEDS_REVIEW in _with_IDs.xlsx files

Output:
  Prints a summary to the console.
  Saves Data/Verification_Report.xlsx with one sheet per check (only sheets with
  flagged rows are written — clean checks are omitted).

Usage:
    py Code/verify_dataset.py
    py Code/verify_dataset.py --years 2019 2022   # specific years only
"""

import argparse
import ast
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'
REPORT_FILE = DATA_DIR / 'Verification_Report.xlsx'

ALL_YEARS = list(range(2018, 2026))

AGE_MIN = 15
AGE_MAX = 42

CL_FINAL_DATES = {
    2018: date(2018, 5, 26),
    2019: date(2019, 6,  1),
    2020: date(2020, 8, 23),
    2021: date(2021, 5, 29),
    2022: date(2022, 5, 28),
    2023: date(2023, 6, 10),
    2024: date(2024, 6,  1),
    2025: date(2025, 5, 31),
}


# ---------------------------------------------------------------------------
# Helpers (duplicated from compile_dataset.py to keep this script standalone)
# ---------------------------------------------------------------------------

def _parse_date(date_str: str) -> date | None:
    if not date_str:
        return None
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d'):
        try:
            from datetime import datetime as dt
            return dt.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _calc_age(dob_raw, end_year: int) -> int | None:
    ref = CL_FINAL_DATES.get(end_year)
    if ref is None:
        return None
    if isinstance(dob_raw, (int, float)) and not pd.isna(dob_raw):
        return ref.year - int(dob_raw)
    if not dob_raw or pd.isna(dob_raw):
        return None
    dob = _parse_date(str(dob_raw))
    if dob is None:
        try:
            return ref.year - int(str(dob_raw).strip())
        except ValueError:
            return None
    return (ref - dob).days // 365


def _normalise_dob(raw) -> str:
    """Return a canonical DOB string for cross-year comparison."""
    if pd.isna(raw) or raw is None:
        return ''
    d = _parse_date(str(raw))
    if d:
        return d.isoformat()
    # integer birth year
    try:
        return str(int(float(str(raw))))
    except (ValueError, TypeError):
        return str(raw).strip()


def _player_id_str(raw) -> str | None:
    if pd.isna(raw):
        return None
    try:
        return str(int(float(raw)))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_mv_files(years: list[int]) -> dict[int, pd.DataFrame]:
    frames = {}
    for year in years:
        path = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'
        if path.exists():
            df = pd.read_excel(path)
            frames[year] = df
        else:
            print(f"  [skip] {path.name} not found")
    return frames


def load_ids_files(years: list[int]) -> dict[int, pd.DataFrame]:
    frames = {}
    for year in years:
        path = DATA_DIR / f'UEFA Stats {year}_with_IDs.xlsx'
        if path.exists():
            df = pd.read_excel(path)
            if 'Match_Confidence' in df.columns:
                frames[year] = df
    return frames


# ---------------------------------------------------------------------------
# Check 1: Completeness
# ---------------------------------------------------------------------------

def check_completeness(frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for year, df in frames.items():
        name_col = 'Player_Name' if 'Player_Name' in df.columns else 'Player Name'
        for _, row in df.iterrows():
            missing = []
            if pd.isna(row.get('Market Value History')):
                missing.append('MV History')
            if pd.isna(row.get('Date of Birth')):
                missing.append('DOB')
            if pd.isna(row.get('Player Country')):
                missing.append('Country')
            if missing:
                rows.append({
                    'Year':        year,
                    'Player Name': row.get(name_col, ''),
                    'Team':        row.get('Team', ''),
                    'Player ID':   _player_id_str(row.get('Player ID')),
                    'Missing':     ', '.join(missing),
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Check 2: DOB plausibility
# ---------------------------------------------------------------------------

def check_dob_plausibility(frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for year, df in frames.items():
        name_col = 'Player_Name' if 'Player_Name' in df.columns else 'Player Name'
        for _, row in df.iterrows():
            dob_raw = row.get('Date of Birth')
            if pd.isna(dob_raw):
                continue
            age = _calc_age(dob_raw, year)
            if age is None or not (AGE_MIN <= age <= AGE_MAX):
                rows.append({
                    'Year':         year,
                    'Player Name':  row.get(name_col, ''),
                    'Team':         row.get('Team', ''),
                    'Player ID':    _player_id_str(row.get('Player ID')),
                    'DOB':          dob_raw,
                    'Age at Final': age,
                    'Issue':        'age out of range' if age is not None else 'age uncomputable',
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Check 3: Cross-year consistency
# ---------------------------------------------------------------------------

def check_cross_year_consistency(frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """
    For each Player ID that appears in multiple year files, check that DOB and
    Player Country are the same across all appearances.
    """
    # Build per-player-ID records: {pid: [(year, dob_norm, country), ...]}
    from collections import defaultdict
    records: dict[str, list] = defaultdict(list)

    for year, df in frames.items():
        name_col = 'Player_Name' if 'Player_Name' in df.columns else 'Player Name'
        for _, row in df.iterrows():
            pid = _player_id_str(row.get('Player ID'))
            if pid is None:
                continue
            records[pid].append({
                'year':    year,
                'name':    row.get(name_col, ''),
                'dob':     _normalise_dob(row.get('Date of Birth')),
                'country': str(row.get('Player Country', '') or '').strip(),
            })

    rows = []
    for pid, appearances in records.items():
        if len(appearances) < 2:
            continue
        dobs     = {a['dob']     for a in appearances if a['dob']}
        countries = {a['country'] for a in appearances if a['country']}
        issues = []
        if len(dobs) > 1:
            issues.append(f"DOB conflict: {dobs}")
        if len(countries) > 1:
            issues.append(f"Country conflict: {countries}")
        if issues:
            years_seen = sorted({a['year'] for a in appearances})
            rows.append({
                'Player ID':   pid,
                'Player Name': appearances[0]['name'],
                'Years':       str(years_seen),
                'Issue':       ' | '.join(issues),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Check 4: Match confidence
# ---------------------------------------------------------------------------

def check_match_confidence(ids_frames: dict[int, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for year, df in ids_frames.items():
        name_col = 'Player_Name' if 'Player_Name' in df.columns else 'Player Name'
        mask = df['Match_Confidence'] == 'NEEDS_REVIEW'
        for _, row in df[mask].iterrows():
            rows.append({
                'Year':        year,
                'Player Name': row.get(name_col, ''),
                'Team':        row.get('Team', ''),
                'Player ID':   _player_id_str(row.get('Player ID')),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_section(title: str, df: pd.DataFrame, max_rows: int = 20):
    total = len(df)
    status = 'CLEAN' if total == 0 else f'{total} issue(s)'
    print(f"\n{'-' * 60}")
    print(f"  {title}  [{status}]")
    print(f"{'-' * 60}")
    if total == 0:
        print("  No issues found.")
    else:
        print(df.head(max_rows).to_string(index=False))
        if total > max_rows:
            print(f"  ... and {total - max_rows} more (see Verification_Report.xlsx)")


def main():
    parser = argparse.ArgumentParser(description="Audit _with_market_values.xlsx files.")
    parser.add_argument('--years', type=int, nargs='+', default=ALL_YEARS)
    args = parser.parse_args()

    print(f"Verifying years: {args.years}\n")

    mv_frames  = load_mv_files(args.years)
    ids_frames = load_ids_files(args.years)

    if not mv_frames:
        print("No _with_market_values.xlsx files found. Nothing to verify.")
        sys.exit(0)

    print(f"Loaded {len(mv_frames)} year file(s): {sorted(mv_frames)}")

    c1 = check_completeness(mv_frames)
    c2 = check_dob_plausibility(mv_frames)
    c3 = check_cross_year_consistency(mv_frames)
    c4 = check_match_confidence(ids_frames)

    print_section("Check 1: Missing data (MV / DOB / Country)", c1)
    print_section("Check 2: DOB plausibility (age 15–42 at CL final)", c2)
    print_section("Check 3: Cross-year DOB / Country conflicts", c3)
    print_section("Check 4: NEEDS_REVIEW player IDs", c4)

    # Summary line
    total_issues = len(c1) + len(c2) + len(c3) + len(c4)
    print(f"\n{'=' * 60}")
    print(f"  Total issues: {total_issues}  (missing={len(c1)}, age={len(c2)}, conflicts={len(c3)}, review={len(c4)})")
    print(f"{'=' * 60}\n")

    # Save report only when there are issues
    sheets = {
        'Missing Data':       c1,
        'DOB Plausibility':   c2,
        'Cross-Year Conflicts': c3,
        'Needs Review IDs':   c4,
    }
    non_empty = {k: v for k, v in sheets.items() if len(v) > 0}
    if non_empty:
        with pd.ExcelWriter(REPORT_FILE, engine='openpyxl') as writer:
            for sheet_name, df in non_empty.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"  Report saved: {REPORT_FILE}")
    else:
        print("  All checks clean — no report file written.")


if __name__ == '__main__':
    main()
