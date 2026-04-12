"""
Report completion status of _with_market_values.xlsx files and update CLAUDE.md.

Reads each file, counts non-null values for Market Value History, Player Country,
and Date of Birth, prints a summary table, and rewrites the "Data File Status"
section in CLAUDE.md with current counts and today's date.

Existing Notes column entries are preserved across updates.

Usage:
    py -3.11 Code/report_status.py                    # all years 2018-2025
    py -3.11 Code/report_status.py --years 2019 2023  # specific years
    py -3.11 Code/report_status.py --no-update        # print only, skip CLAUDE.md
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

DATA_DIR  = Path(__file__).resolve().parent.parent / 'Data'
CLAUDE_MD = Path(__file__).resolve().parent.parent / 'CLAUDE.md'

MV_COL      = 'Market Value History'
COUNTRY_COL = 'Player Country'
DOB_COL     = 'Date of Birth'

ALL_YEARS = list(range(2018, 2026))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt(filled: int, total: int, *, unicode: bool = False) -> str:
    """Return fill status as a string.

    unicode=True uses ✓/— (for CLAUDE.md); unicode=False uses OK/- (for console).
    """
    if total == 0 or filled == 0:
        return '\u2014' if unicode else '-'
    if filled == total:
        return '\u2713' if unicode else 'OK'
    return f'{filled}/{total}'


def _load_stats(year: int) -> dict | None:
    """
    Read the _with_market_values.xlsx for a year and return fill counts.
    Returns None if the file does not exist.
    """
    path = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'
    if not path.exists():
        return None
    df = pd.read_excel(path)
    total = len(df)
    return {
        'total':   total,
        'mv':      int(df[MV_COL].notna().sum())      if MV_COL      in df.columns else 0,
        'country': int(df[COUNTRY_COL].notna().sum()) if COUNTRY_COL in df.columns else 0,
        'dob':     int(df[DOB_COL].notna().sum())     if DOB_COL     in df.columns else 0,
    }


# ---------------------------------------------------------------------------
# CLAUDE.md update
# ---------------------------------------------------------------------------

def _parse_existing_notes(text: str) -> dict[int, str]:
    """Extract the Notes column from the existing Data File Status table."""
    notes: dict[int, str] = {}
    for line in text.splitlines():
        m = re.match(r'\|\s*(\d{4})\s*\|[^|]*\|[^|]*\|[^|]*\|([^|]*)\|', line)
        if m:
            notes[int(m.group(1))] = m.group(2).strip()
    return notes


def _build_table(all_stats: dict[int, dict | None], existing_notes: dict[int, str]) -> str:
    """Render the full Markdown table for all years 2018-2025."""
    header = (
        '| Year | MV History | Country | DOB     | Notes                                               |\n'
        '| ---- | ---------- | ------- | ------- | --------------------------------------------------- |'
    )
    rows = [header]
    for year in ALL_YEARS:
        s    = all_stats.get(year)
        note = existing_notes.get(year, '')
        if s is None:
            mv_s = country_s = dob_s = '\u2014'
        else:
            t = s['total']
            mv_s      = _fmt(s['mv'],      t, unicode=True)
            country_s = _fmt(s['country'], t, unicode=True)
            dob_s     = _fmt(s['dob'],     t, unicode=True)
        rows.append(
            f'| {year} | {mv_s:<10} | {country_s:<7} | {dob_s:<7} | {note:<51} |'
        )
    return '\n'.join(rows)


def update_claude_md(all_stats: dict[int, dict | None]) -> None:
    text = CLAUDE_MD.read_text(encoding='utf-8')
    existing_notes = _parse_existing_notes(text)
    new_table      = _build_table(all_stats, existing_notes)
    today          = date.today().isoformat()

    # Replace the section header (updating the date) and the whole table body.
    # The section ends just before the blank line + '---' separator that follows.
    pattern = (
        r'(## Data File Status )\(as of [^\)]+\)'   # group 1: literal prefix
        r'(\n\n)'                                    # group 2: blank line
        r'\| Year \|.*?(?=\n---)'                   # table rows (non-greedy)
    )
    replacement = rf'\g<1>(as of {today})\g<2>{new_table}'
    new_text, n = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)

    if n == 0:
        print('  [CLAUDE.md] WARNING: table section not found — file not updated.')
        return

    CLAUDE_MD.write_text(new_text, encoding='utf-8')
    print(f'  [CLAUDE.md] Data File Status table updated (as of {today}).')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Report _with_market_values.xlsx completion and update CLAUDE.md.'
    )
    parser.add_argument(
        '--years', nargs='+', type=int, default=ALL_YEARS,
        help='Years to report (default: 2018-2025)'
    )
    parser.add_argument(
        '--no-update', action='store_true',
        help='Print report only; do not modify CLAUDE.md'
    )
    args = parser.parse_args()

    # --- console report ---
    print(f"\n{'='*70}")
    print(f"{'Year':<6} {'MV History':<16} {'Country':<12} {'DOB':<12} {'Rows'}")
    print(f"{'='*70}")

    reported_stats: dict[int, dict | None] = {}
    for year in sorted(args.years):
        s = _load_stats(year)
        reported_stats[year] = s
        if s is None:
            print(f"{year:<6} {'(no file)'}")
        else:
            t = s['total']
            print(
                f"{year:<6} {_fmt(s['mv'], t):<16} "
                f"{_fmt(s['country'], t):<12} "
                f"{_fmt(s['dob'], t):<12} "
                f"{t}"
            )

    print(f"{'='*70}\n")

    # --- CLAUDE.md update ---
    if args.no_update:
        print('  [--no-update] CLAUDE.md not modified.')
        return

    # Build full stats for all years so the table is always complete.
    all_stats: dict[int, dict | None] = {}
    for year in ALL_YEARS:
        if year in reported_stats:
            all_stats[year] = reported_stats[year]
        else:
            all_stats[year] = _load_stats(year)

    update_claude_md(all_stats)


if __name__ == '__main__':
    main()
