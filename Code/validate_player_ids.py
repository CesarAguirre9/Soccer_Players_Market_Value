"""
Validate and auto-correct player IDs in UEFA Stats _with_IDs.xlsx files.

For each player-year:
  1. Look up market value history for the assigned ID
     (from existing _with_market_values.xlsx if available — free; else fetch from API)
  2. Check if any snapshot dated in [year-1, year] has a clubName that fuzzy-matches
     the expected Team column (threshold 65%)
  3. On mismatch: search top 5 Transfermarkt candidates, re-check each
  4. Auto-correct if a better match is found; flag NEEDS_REVIEW if not
  5. Null out the Market Value History cell in _with_market_values.xlsx for any
     row whose ID changed (so fetch_market_values.py re-fetches it)
  6. Append all changes to Data/Corrections_Log.xlsx

Usage:
    py Code/validate_player_ids.py                      # all years 2018-2025
    py Code/validate_player_ids.py --years 2018 2023    # specific years only
"""

import argparse
import ast
import re
import sys
import difflib
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, r'c:\Code_Learning\repos\transfermarkt-api')

from app.services.players.market_value import TransfermarktPlayerMarketValue
from app.services.players.search import TransfermarktPlayerSearch

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'
LOG_FILE = DATA_DIR / 'Corrections_Log.xlsx'

MATCH_THRESHOLD = 0.65   # fuzzy similarity floor for club name matching
TOP_N_SEARCH    = 5      # candidates to check when a mismatch is found

# Known UEFA stats abbreviations → Transfermarkt full names
TEAM_NORMALIZATION: dict[str, str] = {
    "B. Dortmund":       "Borussia Dortmund",
    "Dortmund":          "Borussia Dortmund",
    "Bayern":            "FC Bayern München",
    "Bayern Munchen":    "FC Bayern München",
    "Man City":          "Manchester City",
    "Man United":        "Manchester United",
    "Man Utd":           "Manchester United",
    "Paris":             "Paris Saint-Germain",
    "Milan":             "AC Milan",
    "Inter":             "Inter Milan",
    "Atletico de Madrid":"Atlético de Madrid",
    "Atleti":            "Atlético de Madrid",
    "Crvena Zvezda":     "Red Star Belgrade",
    "Crvena zvezda":     "Red Star Belgrade",
    "GNK Dinamo":        "Dinamo Zagreb",
    "Shakhtar":          "Shakhtar Donetsk",
    "Leipzig":           "RB Leipzig",
    "Monchengladbach":   "Borussia Mönchengladbach",
    "Slavia Praha":      "SK Slavia Praha",
    "Sparta Praha":      "AC Sparta Praha",
    "Spartak Moskva":    "Spartak Moscow",
    "Lokomotiv Moskva":  "Lokomotiv Moscow",
    "CSKA Moskva":       "CSKA Moscow",
    "S. Bratislava":     "ŠK Slovan Bratislava",
    "Malmo":             "Malmö FF",
    "Dynamo Kyiv":       "Dynamo Kyiv",
    "FC Porto":          "FC Porto",
    "Leverkusen":        "Bayer 04 Leverkusen",
    "Napoli":            "SSC Napoli",
    "Lazio":             "SS Lazio",
    "Roma":              "AS Roma",
    "Wolfsburg":         "VfL Wolfsburg",
    "Schalke":           "FC Schalke 04",
    "Hoffenheim":        "TSG Hoffenheim",
    "Stuttgart":         "VfB Stuttgart",
    "Eintracht Frankfurt": "Eintracht Frankfurt",
    "Union Berlin":      "1.FC Union Berlin",
    "Young Boys":        "BSC Young Boys",
    "Salzburg":          "FC Red Bull Salzburg",
    "Feyenoord":         "Feyenoord Rotterdam",
}


# ---------------------------------------------------------------------------
# Helpers — market value history parsing (mirrors compile_dataset.py)
# ---------------------------------------------------------------------------

def _parse_mv_history_string(raw) -> list:
    if pd.isna(raw) or raw is None:
        return []
    s = str(raw).strip()
    if s in ('None', 'nan', ''):
        return []
    s = re.sub(r'datetime\.datetime\([^)]+\)', 'None', s)
    try:
        data = ast.literal_eval(s)
    except Exception:
        return []
    if isinstance(data, dict):
        return data.get('marketValueHistory') or []
    return []


def _parse_date(date_str: str):
    if not date_str:
        return None
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _normalize_team(team: str) -> str:
    return TEAM_NORMALIZATION.get(team, team)


def _club_similarity(club_a: str, club_b: str) -> float:
    """Return a 0-1 similarity between two club name strings."""
    a = _normalize_team(club_a).lower()
    b = _normalize_team(club_b).lower()
    return difflib.SequenceMatcher(None, a, b).ratio()


def _id_str(raw) -> str | None:
    if pd.isna(raw):
        return None
    try:
        return str(int(float(raw)))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def fetch_mv_history(player_id: str) -> list:
    try:
        data = TransfermarktPlayerMarketValue(player_id=player_id).get_player_market_value()
        return data.get('marketValueHistory') or []
    except Exception as e:
        print(f"      [MV fetch error] ID {player_id}: {e}")
        return []


def search_candidates(player_name: str, n: int = TOP_N_SEARCH) -> list[dict]:
    """Return up to n Transfermarkt search results for player_name."""
    try:
        results = TransfermarktPlayerSearch(query=player_name).search_players().get('results', [])
        return results[:n]
    except Exception as e:
        print(f"      [Search error] '{player_name}': {e}")
        return []


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------

def best_club_match_score(history: list, team: str, start_year: int, end_year: int) -> float:
    """
    Return the highest club-similarity score found in the [start_year, end_year] window.
    Returns 0 if no snapshots exist in that window.
    """
    best = 0.0
    for entry in history:
        d = _parse_date(entry.get('date', ''))
        if d and start_year <= d.year <= end_year:
            score = _club_similarity(entry.get('clubName', ''), team)
            if score > best:
                best = score
    return best


def validate_player(player_name: str, team: str, player_id: str,
                    year: int, existing_history: list | None) -> dict:
    """
    Validate one player's assigned ID.

    Returns a result dict:
        status         : 'VALID' | 'AUTO_CORRECTED' | 'NEEDS_REVIEW'
        old_id         : the ID that was checked
        new_id         : corrected ID (same as old_id if VALID/NEEDS_REVIEW)
        score          : best club-match score found
        alt_club       : best matching club name from history
        confidence     : 'HIGH' | 'MEDIUM' | 'LOW'
    """
    start_year = year - 1
    end_year   = year

    # Step 1: get history (free if pre-loaded, else fetch)
    if existing_history is not None:
        history = existing_history
    else:
        print(f"    Fetching MV history for {player_name} (ID: {player_id})...")
        history = fetch_mv_history(player_id)

    # Step 2: validate current ID
    score = best_club_match_score(history, team, start_year, end_year)

    if score >= MATCH_THRESHOLD:
        return {
            'status': 'VALID', 'old_id': player_id, 'new_id': player_id,
            'score': score, 'confidence': 'HIGH' if score >= 0.85 else 'MEDIUM',
        }

    # Step 3: mismatch — search for alternatives
    print(f"    MISMATCH: {player_name} ({team} {year}) ID={player_id}  club_score={score:.2f}")
    candidates = search_candidates(player_name)

    best_candidate_id    = None
    best_candidate_score = 0.0

    for cand in candidates:
        cand_id = cand.get('id')
        if not cand_id or cand_id == player_id:
            continue
        print(f"      Checking candidate: {cand.get('name')} (ID: {cand_id}, club: {cand.get('club',{}).get('name','')})")
        cand_history = fetch_mv_history(cand_id)
        cand_score   = best_club_match_score(cand_history, team, start_year, end_year)
        print(f"        club match score: {cand_score:.2f}")
        if cand_score > best_candidate_score:
            best_candidate_score = cand_score
            best_candidate_id    = cand_id

    if best_candidate_score >= MATCH_THRESHOLD:
        print(f"    → AUTO_CORRECTED: {player_id} → {best_candidate_id}  (score {best_candidate_score:.2f})")
        return {
            'status': 'AUTO_CORRECTED', 'old_id': player_id, 'new_id': best_candidate_id,
            'score': best_candidate_score,
            'confidence': 'HIGH' if best_candidate_score >= 0.85 else 'MEDIUM',
        }

    print(f"    → NEEDS_REVIEW: no good match found in top {TOP_N_SEARCH}")
    return {
        'status': 'NEEDS_REVIEW', 'old_id': player_id, 'new_id': player_id,
        'score': score, 'confidence': 'LOW',
    }


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------

def load_existing_mv_history(mv_file: Path) -> dict[tuple, list] | None:
    """
    Load existing market value histories keyed by (Player Name, Team).
    Returns None if file doesn't exist.
    """
    if not mv_file.exists():
        return None
    df = pd.read_excel(mv_file)
    if 'Market Value History' not in df.columns:
        return None
    mapping = {}
    for _, row in df.iterrows():
        key = (str(row.get('Player Name', '')), str(row.get('Team', '')))
        mapping[key] = _parse_mv_history_string(row.get('Market Value History'))
    return mapping


def process_year(year: int, log_rows: list) -> bool:
    ids_file = DATA_DIR / f'UEFA Stats {year}_with_IDs.xlsx'
    mv_file  = DATA_DIR / f'UEFA Stats {year}_with_market_values.xlsx'

    if not ids_file.exists():
        print(f"  [{year}] _with_IDs.xlsx not found. Skipping.")
        return False

    df_ids = pd.read_excel(ids_file)
    print(f"\n{'='*60}")
    print(f"Year: {year}  ({len(df_ids)} players)")
    print(f"{'='*60}")

    # Load existing MV histories if available (avoids API calls for validated rows)
    mv_cache = load_existing_mv_history(mv_file)
    if mv_cache:
        print(f"  Loaded MV history cache from {mv_file.name}  ({len(mv_cache)} entries)")
    else:
        print(f"  No existing MV file — will fetch histories from API as needed")

    # Load market values df for nulling corrected rows later
    df_mv = pd.read_excel(mv_file) if mv_file.exists() else None

    corrections_this_year = 0

    for i, row in df_ids.iterrows():
        player_name = str(row.get('Player Name', ''))
        team        = str(row.get('Team', ''))
        player_id   = _id_str(row.get('Player ID'))

        if player_id is None:
            continue  # no ID assigned — out of scope for this script

        # Try to find pre-loaded history
        existing_hist = mv_cache.get((player_name, team)) if mv_cache else None

        result = validate_player(player_name, team, player_id, year, existing_hist)

        # Record in log
        log_rows.append({
            'Year':       year,
            'Player Name': player_name,
            'Team':        team,
            'Old ID':      result['old_id'],
            'New ID':      result['new_id'],
            'Status':      result['status'],
            'Confidence':  result['confidence'],
            'Club Score':  round(result['score'], 3),
            'Timestamp':   datetime.now().isoformat(),
        })

        if result['status'] == 'AUTO_CORRECTED':
            corrections_this_year += 1
            # Update ID in df_ids
            df_ids.at[i, 'Player ID'] = int(result['new_id'])

            # Null out MV history in df_mv for this row (will be re-fetched)
            if df_mv is not None:
                mask = (df_mv['Player Name'] == player_name) & (df_mv['Team'] == team)
                if mask.any() and 'Market Value History' in df_mv.columns:
                    df_mv.loc[mask, 'Market Value History'] = None

    # Save updated files if any corrections were made
    if corrections_this_year > 0:
        print(f"\n  {corrections_this_year} correction(s) applied — saving updated files...")
        df_ids.to_excel(ids_file, index=False)
        print(f"  Saved: {ids_file.name}")
        if df_mv is not None:
            df_mv.to_excel(mv_file, index=False)
            print(f"  Saved: {mv_file.name}  (nulled MV rows will be re-fetched)")
    else:
        print(f"\n  No corrections needed for {year}.")

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate and correct player IDs.")
    parser.add_argument('--years', nargs='+', type=int, default=list(range(2018, 2026)),
                        help='Years to process (default: 2018-2025)')
    args = parser.parse_args()

    print(f"Player ID Validator")
    print(f"Years: {args.years}\n")

    log_rows = []

    for year in args.years:
        process_year(year, log_rows)

    # Write / append corrections log
    if log_rows:
        df_log = pd.DataFrame(log_rows)

        if LOG_FILE.exists():
            df_existing = pd.read_excel(LOG_FILE)
            df_log = pd.concat([df_existing, df_log], ignore_index=True)

        df_log.to_excel(LOG_FILE, index=False)
        print(f"\nCorrections log saved: {LOG_FILE.name}  ({len(log_rows)} entries)")

        # Summary
        status_counts = pd.Series([r['Status'] for r in log_rows]).value_counts()
        print("\nSummary:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
    else:
        print("\nNo entries to log.")


if __name__ == "__main__":
    main()
