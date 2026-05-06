"""
Run the per-year data pipeline in order:
  1. validate_player_ids  — apply MANUALLY_UPDATED corrections, skip already-resolved
  2. fetch_market_values  — fill MV history, Country, DOB for any missing rows
  3. add_mv_columns       — write MV{year} and MV{year-1} columns

All three scripts are resumable: re-running is safe and only processes what is
still missing or uncorrected.

Usage:
    py -3.11 Code/run_pipeline.py --years 2018
    py -3.11 Code/run_pipeline.py --years 2018 2023
    py -3.11 Code/run_pipeline.py --years 2018 --skip-fetch   # ID corrections only
"""

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent


def run_step(script: str, years: list[int], extra_args: list[str] | None = None) -> None:
    years_str = [str(y) for y in years]
    cmd = [sys.executable, str(SCRIPTS_DIR / script), '--years'] + years_str
    if extra_args:
        cmd += extra_args
    print(f"\n{'='*60}")
    print(f"STEP: {script}  (years: {', '.join(years_str)})")
    print(f"{'='*60}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\nERROR: {script} exited with code {result.returncode}. Stopping pipeline.")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run the per-year data pipeline.")
    parser.add_argument('--years', nargs='+', type=int, required=True,
                        help='Season end-years to process (e.g. 2018 2023)')
    parser.add_argument('--skip-validate', action='store_true',
                        help='Skip validate_player_ids step')
    parser.add_argument('--skip-fetch', action='store_true',
                        help='Skip fetch_market_values step (ID corrections only)')
    parser.add_argument('--skip-mv-cols', action='store_true',
                        help='Skip add_mv_columns step')
    args = parser.parse_args()

    years_label = ', '.join(str(y) for y in args.years)
    print(f"\nPipeline starting for: {years_label}")

    if not args.skip_validate:
        run_step('validate_player_ids.py', args.years)

    if not args.skip_fetch:
        run_step('fetch_market_values.py', args.years)

    if not args.skip_mv_cols:
        run_step('add_mv_columns.py', args.years)

    print(f"\n{'='*60}")
    print(f"Pipeline complete for: {years_label}")
    print(f"{'='*60}")
    print("\nNext steps (full dataset):")
    print("  py -3.11 Code/verify_dataset.py")
    print("  py -3.11 Code/build_club_country_map.py")
    print("  py -3.11 Code/compile_dataset.py")


if __name__ == '__main__':
    main()
