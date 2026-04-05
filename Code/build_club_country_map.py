"""
Build a mapping of club names (as they appear in the UEFA stats files) to their countries.

Process:
1. Extract all unique team names from all UEFA Stats {year}_with_IDs.xlsx files.
2. For each team name, search Transfermarkt to find the club and its country.
3. Save the mapping to Data/club_country_map.json.
4. Report any clubs that could not be resolved (for manual entry).

Run this once before compile_dataset.py. If a club is missing or wrong in the JSON,
edit club_country_map.json manually.
"""

import sys
import json
import pandas as pd
from pathlib import Path

sys.path.insert(0, r'c:\Code_Learning\repos\transfermarkt-api')

from app.services.clubs.search import TransfermarktClubSearch

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'
OUTPUT_FILE = DATA_DIR / 'club_country_map.json'


def search_club_country(team_name: str) -> str | None:
    """Search Transfermarkt for a club and return its country."""
    try:
        results = TransfermarktClubSearch(query=team_name).search_clubs().get('results', [])
        if results:
            country = results[0].get('country')
            print(f"  '{team_name}' → '{results[0].get('name')}' ({country})")
            return country
        print(f"  '{team_name}' → no results found")
        return None
    except Exception as e:
        print(f"  '{team_name}' → error: {e}")
        return None


def get_all_unique_teams() -> list[str]:
    """Collect all unique team names from all _with_IDs.xlsx files."""
    teams = set()
    for year in range(2018, 2026):
        f = DATA_DIR / f'UEFA Stats {year}_with_IDs.xlsx'
        if f.exists():
            df = pd.read_excel(f, usecols=['Team'])
            teams.update(df['Team'].dropna().astype(str).unique())
    return sorted(teams)


def main():
    # Load existing mapping if it exists (allows incremental updates)
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as fh:
            mapping = json.load(fh)
        print(f"Loaded existing mapping with {len(mapping)} entries from {OUTPUT_FILE.name}\n")
    else:
        mapping = {}

    teams = get_all_unique_teams()
    print(f"Found {len(teams)} unique team names across all years.\n")

    unresolved = []
    for team in teams:
        if team in mapping:
            print(f"  '{team}' → already mapped to '{mapping[team]}' (skipping)")
            continue

        country = search_club_country(team)
        if country:
            mapping[team] = country
        else:
            unresolved.append(team)

    # Save updated mapping
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as fh:
        json.dump(mapping, fh, indent=2, ensure_ascii=False)

    print(f"\nMapping saved to {OUTPUT_FILE.name}  ({len(mapping)} entries)")

    if unresolved:
        print(f"\nCould not resolve {len(unresolved)} clubs — please add manually to {OUTPUT_FILE.name}:")
        for t in unresolved:
            print(f"  \"{t}\": \"<country>\"")
    else:
        print("\nAll clubs resolved successfully.")


if __name__ == "__main__":
    main()
