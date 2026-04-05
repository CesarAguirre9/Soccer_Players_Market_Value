"""
Assign validated Transfermarkt player IDs to UEFA Stats files.

Filters out players with 0 / empty Minutes Played, then searches Transfermarkt
for each player's ID using a tiered validation strategy (cheapest signal first):

  Tier 1 — Position filter (free)
    Skip search candidates whose Transfermarkt position doesn't match the UEFA
    position category (Goalkeeper / Defender / Midfielder / Forward).

  Tier 2 — Current-club match (free)
    If a candidate's current club fuzzy-matches the player's Team, return HIGH
    confidence immediately. Most reliable for recent seasons (2024-25).

  Tier 3 — MV history club check (1 API call per remaining candidate)
    Fetch market value history and check whether any snapshot in the season's
    year window has a clubName that matches the expected Team. This is the
    definitive signal for older seasons where players may have since retired or
    transferred: a player who was retired BEFORE the season won't have the
    correct club in their history, while a player who retired AFTER will.

NOTE: A blanket "skip if retired / no market value" filter is intentionally
      absent. Players active in older seasons (2018-2020) may now be retired
      and show 0 market value, so filtering on current retirement status
      produces false negatives. The MV history club check handles this cleanly.

Output adds two columns to _with_IDs.xlsx:
  Player ID        -- Transfermarkt ID (or NaN if not found)
  Match_Confidence -- HIGH | MEDIUM | NEEDS_REVIEW

Usage:
    py Code/process_uefa_stats.py               # default: years 2024-2025
    py Code/process_uefa_stats.py --year 2026   # single year
"""

import argparse
import sys
import difflib
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, r'c:\Code_Learning\repos\transfermarkt-api')

from app.services.players.market_value import TransfermarktPlayerMarketValue
from app.services.players.search import TransfermarktPlayerSearch

DATA_DIR = Path(__file__).resolve().parent.parent / 'Data'

TOP_N                = 5     # max search results to evaluate
CLUB_MATCH_THRESHOLD = 0.65  # fuzzy similarity floor

# Map broad UEFA position labels to keywords found in Transfermarkt positions
POSITION_KEYWORDS: dict[str, list[str]] = {
    "Goalkeeper": ["goalkeeper"],
    "Defender":   ["back", "defender", "sweeper"],
    "Midfielder": ["midfield"],
    "Forward":    ["forward", "wing", "attack", "striker", "winger"],
}

# Known UEFA stats abbreviations -> Transfermarkt full names (aids fuzzy matching)
TEAM_NORMALIZATION: dict[str, str] = {
    "B. Dortmund":        "Borussia Dortmund",
    "Dortmund":           "Borussia Dortmund",
    "Bayern":             "FC Bayern Munchen",
    "Bayern Munchen":     "FC Bayern Munchen",
    "Man City":           "Manchester City",
    "Man United":         "Manchester United",
    "Man Utd":            "Manchester United",
    "Paris":              "Paris Saint-Germain",
    "Milan":              "AC Milan",
    "Inter":              "Inter Milan",
    "Atletico de Madrid": "Atletico de Madrid",
    "Atleti":             "Atletico de Madrid",
    "Crvena Zvezda":      "Red Star Belgrade",
    "Crvena zvezda":      "Red Star Belgrade",
    "GNK Dinamo":         "Dinamo Zagreb",
    "Shakhtar":           "Shakhtar Donetsk",
    "Leipzig":            "RB Leipzig",
    "Monchengladbach":    "Borussia Monchengladbach",
    "Slavia Praha":       "SK Slavia Praha",
    "Sparta Praha":       "AC Sparta Praha",
    "Spartak Moskva":     "Spartak Moscow",
    "Lokomotiv Moskva":   "Lokomotiv Moscow",
    "CSKA Moskva":        "CSKA Moscow",
    "S. Bratislava":      "Slovan Bratislava",
    "Malmo":              "Malmo FF",
    "Dynamo Kyiv":        "Dynamo Kyiv",
    "Leverkusen":         "Bayer 04 Leverkusen",
    "Schalke":            "FC Schalke 04",
    "Hoffenheim":         "TSG Hoffenheim",
    "Stuttgart":          "VfB Stuttgart",
    "Union Berlin":       "1.FC Union Berlin",
    "Young Boys":         "BSC Young Boys",
    "Salzburg":           "FC Red Bull Salzburg",
    "Feyenoord":          "Feyenoord Rotterdam",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(team: str) -> str:
    return TEAM_NORMALIZATION.get(team, team)


def _club_sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, _normalize(a).lower(), _normalize(b).lower()).ratio()


def _position_ok(transfermarkt_pos: str, uefa_pos: str) -> bool:
    """True if the Transfermarkt position is compatible with the UEFA category."""
    if not transfermarkt_pos or not uefa_pos:
        return True
    keywords = POSITION_KEYWORDS.get(uefa_pos, [])
    return any(kw in transfermarkt_pos.lower() for kw in keywords)


def _parse_date(s: str):
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _mv_club_score(history: list, team: str, start_year: int, end_year: int) -> float:
    """Best fuzzy club-match score across MV snapshots in [start_year, end_year]."""
    best = 0.0
    for entry in history:
        d = _parse_date(entry.get('date', ''))
        if d and start_year <= d.year <= end_year:
            s = _club_sim(entry.get('clubName', ''), team)
            if s > best:
                best = s
    return best


def _fetch_mv_history(player_id: str) -> list:
    try:
        data = TransfermarktPlayerMarketValue(player_id=player_id).get_player_market_value()
        return data.get('marketValueHistory') or []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Core ID lookup
# ---------------------------------------------------------------------------

def get_player_id(player_name: str, team: str, position: str,
                  season_year: int) -> tuple[str | None, str]:
    """
    Search Transfermarkt for the correct player ID.
    Returns (player_id, confidence) where confidence is HIGH / MEDIUM / NEEDS_REVIEW.
    """
    if pd.isna(player_name) or not str(player_name).strip():
        return None, 'NEEDS_REVIEW'

    print(f"  Searching: {player_name!r}  [{team}, {position}]")

    try:
        results = TransfermarktPlayerSearch(query=player_name).search_players().get('results', [])
    except Exception as e:
        print(f"    Search error: {e}")
        return None, 'NEEDS_REVIEW'

    if not results:
        print(f"    No results found.")
        return None, 'NEEDS_REVIEW'

    start_year = season_year - 1

    # ------------------------------------------------------------------
    # Tier 1: position filter
    # ------------------------------------------------------------------
    filtered = [r for r in results[:TOP_N] if _position_ok(r.get('position', ''), position)]
    if not filtered:
        filtered = results[:1]

    # ------------------------------------------------------------------
    # Tier 2: current-club match (free — best for recent seasons)
    # ------------------------------------------------------------------
    for r in filtered:
        club = (r.get('club') or {}).get('name', '')
        if club and _club_sim(club, team) >= CLUB_MATCH_THRESHOLD:
            print(f"    -> HIGH (current club match): {r.get('name')} ID={r.get('id')} club={club}")
            return r['id'], 'HIGH'

    if len(filtered) == 1:
        r = filtered[0]
        print(f"    -> MEDIUM (only candidate after position filter): {r.get('name')} ID={r.get('id')}")
        return r['id'], 'MEDIUM'

    # ------------------------------------------------------------------
    # Tier 3: MV history club check — run for all remaining candidates
    # This handles older seasons correctly: a player who retired AFTER
    # the season will show the correct club; one who retired BEFORE won't.
    # ------------------------------------------------------------------
    best_id    = None
    best_score = 0.0

    for r in filtered:
        cid = r.get('id')
        if not cid:
            continue
        print(f"    Checking MV history: {r.get('name')} ID={cid}")
        history = _fetch_mv_history(cid)
        score   = _mv_club_score(history, team, start_year, season_year)
        print(f"      club score: {score:.2f}")
        if score > best_score:
            best_score = score
            best_id    = cid

    if best_id and best_score >= CLUB_MATCH_THRESHOLD:
        conf = 'HIGH' if best_score >= 0.85 else 'MEDIUM'
        print(f"    -> {conf} (MV history, score {best_score:.2f}): ID={best_id}")
        return best_id, conf

    # Fallback: first position-filtered candidate
    fallback = filtered[0]
    print(f"    -> NEEDS_REVIEW: ID={fallback.get('id')} ({fallback.get('name')})")
    return fallback.get('id'), 'NEEDS_REVIEW'


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------

def process_uefa_file(year: int) -> bool:
    input_file  = DATA_DIR / f'UEFA Stats {year}.xlsx'
    output_file = DATA_DIR / f'UEFA Stats {year}_with_IDs.xlsx'

    print(f"\n{'='*60}")
    print(f"Processing: {input_file.name}")
    print(f"{'='*60}")

    if not input_file.exists():
        print(f"  File not found. Skipping.")
        return False

    df = pd.read_excel(input_file)
    initial = len(df)

    # Filter 0 / empty minutes — handle both column name conventions
    minutes_col = 'Minutes_played' if 'Minutes_played' in df.columns else 'Minutes Played'
    if minutes_col in df.columns:
        df = df[df[minutes_col].notna() & (df[minutes_col] != 0) & (df[minutes_col] != '0')]
    print(f"  {initial} rows loaded; {initial - len(df)} removed (0 minutes); {len(df)} remaining")

    name_col = 'Player_Name' if 'Player_Name' in df.columns else 'Player Name'

    ids, confidences = [], []
    for _, row in df.iterrows():
        pid, conf = get_player_id(
            player_name=str(row.get(name_col, '')),
            team=str(row.get('Team', '')),
            position=str(row.get('Position', '')),
            season_year=year,
        )
        ids.append(pid)
        confidences.append(conf)

    df['Player ID']        = ids
    df['Match_Confidence'] = confidences

    found    = sum(1 for x in ids if x is not None)
    reviewed = confidences.count('NEEDS_REVIEW')
    print(f"\n  Found: {found}/{len(df)}  |  NEEDS_REVIEW: {reviewed}")

    df.to_excel(output_file, index=False)
    print(f"  Saved: {output_file.name}")
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Assign validated Transfermarkt IDs to UEFA Stats files."
    )
    parser.add_argument('--year', type=int, default=None,
                        help='Single year to process. Omit to run 2024-2025.')
    args = parser.parse_args()

    years = [args.year] if args.year else [2024, 2025]
    print("UEFA Stats -- Player ID Processor (tiered validation)\n")
    for year in years:
        process_uefa_file(year)
    print("\nDone.")


if __name__ == "__main__":
    main()
