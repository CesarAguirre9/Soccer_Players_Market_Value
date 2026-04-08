"""
Remove rows with zero or missing 'Minutes Played' from all
UEFA Stats {year}_with_market_values.xlsx files.

Players who recorded no minutes contributed nothing to the CL season
and would distort market-value regressions, so they are dropped here
rather than inside compile_dataset.py.

Usage:
    py -3.11 Code/cleanup.py                     # all available years
    py -3.11 Code/cleanup.py --years 2019 2022   # specific years
    py -3.11 Code/cleanup.py --dry-run           # report only, no writes
"""

import argparse
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'
MINUTES_COL = 'Minutes Played'


def clean_year(year: int, dry_run: bool = False) -> bool:
    path = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'
    if not path.exists():
        print(f"  [{year}] File not found — skipping.")
        return False

    df = pd.read_excel(path)
    before = len(df)

    if MINUTES_COL not in df.columns:
        print(f"  [{year}] Column '{MINUTES_COL}' not found — skipping.")
        return False

    # Coerce to numeric first so string sentinels like "-" become NaN
    minutes = pd.to_numeric(df[MINUTES_COL], errors='coerce')

    # Drop rows where minutes are missing or zero
    mask_keep = minutes.notna() & (minutes != 0)
    df_clean = df[mask_keep].reset_index(drop=True)
    dropped = before - len(df_clean)

    if dropped == 0:
        print(f"  [{year}] Nothing to drop ({before} rows, all have minutes > 0).")
        return True

    print(f"  [{year}] {before} rows -> {len(df_clean)} rows  (dropped {dropped})")

    if not dry_run:
        df_clean.to_excel(path, index=False)
        print(f"         Saved: {path.name}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Drop zero/missing minutes rows from _with_market_values files.")
    parser.add_argument('--years', nargs='+', type=int,
                        help='Years to process (default: all found in Data/)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report what would be dropped without writing files')
    args = parser.parse_args()

    if args.years:
        years = args.years
    else:
        years = sorted(
            int(p.stem.split('_')[0].split()[-1])
            for p in DATA_DIR.glob('UEFA Stats *_with_market_values.xlsx')
        )

    if not years:
        print("No _with_market_values.xlsx files found in Data/.")
        return

    mode = '[DRY RUN] ' if args.dry_run else ''
    print(f"{mode}Cleaning years: {years}\n")

    for year in years:
        clean_year(year, dry_run=args.dry_run)

    if args.dry_run:
        print("\nDry run complete - no files were modified.")


if __name__ == '__main__':
    main()
