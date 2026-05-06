"""
Validate and auto-correct player IDs in UEFA Stats _with_IDs.xlsx files.

For each player-year three signals are checked against the assigned ID:
  1. Club name   — a snapshot in [year-1, year] must fuzzy-match the Team column (≥65%)
  2. Activity    — the player must have at least one MV snapshot in [year-1, year]
  3. Position    — the player's Transfermarkt position (broad category) must match
                   the UEFA Position column (Goalkeeper/Defender/Midfielder/Forward)

A player passes only when ALL three signals agree. On any failure the top-N
Transfermarkt search candidates are evaluated with the same three signals and the
best-scoring active, position-compatible candidate is auto-corrected.

Steps:
  1. Look up market value history (from _with_market_values.xlsx cache if available,
     else fetch from ceapi)
  2. Search Transfermarkt by player name → get candidates + current ID's TM position
  3. Check all three signals for the current ID
  4. On mismatch: re-check each candidate; auto-correct if a better match found
  5. Null out Market Value History for any row whose ID changed (triggers re-fetch)
  6. Append all results to Data/Corrections_Log.xlsx

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

# Statuses that mean "already resolved — skip re-validation".
# Set NEEDS_REVIEW → MANUALLY_VERIFIED in the log after you confirm an ID is correct.
SKIP_STATUSES = {'VALID', 'AUTO_CORRECTED', 'MANUALLY_VERIFIED', 'MANUALLY_UPDATED', 'CROSS_YEAR_CORRECTED'}

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

# Transfermarkt granular position → broad category used by UEFA data.
# UEFA already stores Goalkeeper / Defender / Midfielder / Forward directly.
TM_POSITION_MAP: dict[str, str] = {
    # Goalkeepers
    "Goalkeeper":          "Goalkeeper",
    # Defenders
    "Centre-Back":         "Defender",
    "Left-Back":           "Defender",
    "Right-Back":          "Defender",
    "Left Back":           "Defender",
    "Right Back":          "Defender",
    "Centre Back":         "Defender",
    "Sweeper":             "Defender",
    "Defender":            "Defender",
    # Midfielders
    "Defensive Midfield":  "Midfielder",
    "Central Midfield":    "Midfielder",
    "Attacking Midfield":  "Midfielder",
    "Left Midfield":       "Midfielder",
    "Right Midfield":      "Midfielder",
    "Midfielder":          "Midfielder",
    # Forwards
    "Centre-Forward":      "Forward",
    "Left Winger":         "Forward",
    "Right Winger":        "Forward",
    "Second Striker":      "Forward",
    "Forward":             "Forward",
    "Attack":              "Forward",
}

_UEFA_BROAD = {"Goalkeeper", "Defender", "Midfielder", "Forward"}


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


def _broad_position(pos_str: str | None) -> str | None:
    """Map any position string (UEFA or TM) to Goalkeeper/Defender/Midfielder/Forward."""
    if not pos_str:
        return None
    norm = str(pos_str).strip()
    if norm in _UEFA_BROAD:
        return norm
    return TM_POSITION_MAP.get(norm)


def _positions_compatible(tm_pos: str | None, uefa_pos: str | None) -> bool | None:
    """
    Compare TM and UEFA position broad categories.
    Returns True (match), False (clear mismatch), or None (either side unknown → skip).
    """
    broad_tm   = _broad_position(tm_pos)
    broad_uefa = _broad_position(uefa_pos)
    if broad_tm is None or broad_uefa is None:
        return None
    return broad_tm == broad_uefa


def _has_activity(history: list, start_year: int, end_year: int) -> bool:
    """True if at least one MV snapshot falls within [start_year, end_year]."""
    for entry in history:
        d = _parse_date(entry.get('date', ''))
        if d and start_year <= d.year <= end_year:
            return True
    return False


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
                    year: int, existing_history: list | None,
                    uefa_position: str | None = None) -> dict:
    """
    Validate one player's assigned ID against three signals:
      1. Club name   — MV snapshot in [year-1, year] fuzzy-matches Team (≥65%)
      2. Activity    — at least one MV snapshot exists in [year-1, year]
      3. Position    — TM broad category matches UEFA Position column

    Returns a result dict:
        status         : 'VALID' | 'AUTO_CORRECTED' | 'NEEDS_REVIEW'
        old_id / new_id
        score          : best club-match score
        confidence     : 'HIGH' | 'MEDIUM' | 'LOW'
        activity_ok    : bool
        position_check : True | False | None  (None = position unknown, check skipped)
        tm_position    : raw TM position string for the accepted ID
        uefa_position  : the UEFA Position column value passed in
    """
    start_year = year - 1
    end_year   = year

    # Step 1: get MV history (free if pre-loaded, else fetch)
    if existing_history is not None:
        history = existing_history
    else:
        print(f"    Fetching MV history for {player_name} (ID: {player_id})...")
        history = fetch_mv_history(player_id)

    # Step 2: search by name — gives us candidates AND the current ID's TM position
    candidates  = search_candidates(player_name)
    tm_position = next(
        (c.get('position') for c in candidates if str(c.get('id', '')) == player_id),
        None,
    )

    # Step 3: evaluate all three signals for the current ID
    club_score  = best_club_match_score(history, team, start_year, end_year)
    activity_ok = _has_activity(history, start_year, end_year)
    pos_check   = _positions_compatible(tm_position, uefa_position)

    all_pass = (
        club_score  >= MATCH_THRESHOLD
        and activity_ok
        and pos_check is not False
    )

    if all_pass:
        return {
            'status': 'VALID', 'old_id': player_id, 'new_id': player_id,
            'score': club_score,
            'confidence':     'HIGH' if club_score >= 0.85 else 'MEDIUM',
            'activity_ok':    activity_ok,
            'position_check': pos_check,
            'tm_position':    tm_position,
            'uefa_position':  uefa_position,
        }

    # Step 4: mismatch — log why, then evaluate candidates
    fail_reasons = []
    if club_score < MATCH_THRESHOLD:
        fail_reasons.append(f"club_score={club_score:.2f}")
    if not activity_ok:
        fail_reasons.append("no_activity_in_window")
    if pos_check is False:
        fail_reasons.append(
            f"position_mismatch(UEFA={_broad_position(uefa_position)}"
            f",TM={_broad_position(tm_position)})"
        )
    print(f"    MISMATCH: {player_name} ({team} {year}) ID={player_id}"
          f"  [{', '.join(fail_reasons)}]")

    best_candidate_id    = None
    best_candidate_score = 0.0
    best_cand_activity   = False
    best_cand_pos_check  = None
    best_cand_tm_pos     = None

    for cand in candidates:
        cand_id = cand.get('id')
        if not cand_id or cand_id == player_id:
            continue
        cand_tm_pos = cand.get('position')
        cand_pos_check = _positions_compatible(cand_tm_pos, uefa_position)

        print(f"      Checking candidate: {cand.get('name')} "
              f"(ID: {cand_id}, club: {cand.get('club', {}).get('name', '')}, "
              f"pos: {cand_tm_pos})")

        cand_history  = fetch_mv_history(cand_id)
        cand_score    = best_club_match_score(cand_history, team, start_year, end_year)
        cand_activity = _has_activity(cand_history, start_year, end_year)

        if not cand_activity or cand_pos_check is False:
            print(f"        skipped: activity={cand_activity}, pos_check={cand_pos_check}")
            continue

        print(f"        club_score={cand_score:.2f}, activity={cand_activity}, "
              f"pos_check={cand_pos_check}")

        if cand_score > best_candidate_score:
            best_candidate_score = cand_score
            best_candidate_id    = cand_id
            best_cand_activity   = cand_activity
            best_cand_pos_check  = cand_pos_check
            best_cand_tm_pos     = cand_tm_pos

    if best_candidate_score >= MATCH_THRESHOLD:
        print(f"    → AUTO_CORRECTED: {player_id} → {best_candidate_id}"
              f"  (score {best_candidate_score:.2f})")
        return {
            'status': 'AUTO_CORRECTED', 'old_id': player_id, 'new_id': best_candidate_id,
            'score':          best_candidate_score,
            'confidence':     'HIGH' if best_candidate_score >= 0.85 else 'MEDIUM',
            'activity_ok':    best_cand_activity,
            'position_check': best_cand_pos_check,
            'tm_position':    best_cand_tm_pos,
            'uefa_position':  uefa_position,
        }

    print(f"    → NEEDS_REVIEW: no good match found in top {TOP_N_SEARCH}")
    return {
        'status': 'NEEDS_REVIEW', 'old_id': player_id, 'new_id': player_id,
        'score':          club_score,
        'confidence':     'LOW',
        'activity_ok':    activity_ok,
        'position_check': pos_check,
        'tm_position':    tm_position,
        'uefa_position':  uefa_position,
    }


# ---------------------------------------------------------------------------
# Skip-set: players already resolved in a prior run or manually verified
# ---------------------------------------------------------------------------

def load_skip_set(log_file: Path) -> set[tuple]:
    """
    Return (year, player_name, team) tuples whose most recent log entry has a
    terminal status (VALID, AUTO_CORRECTED, MANUALLY_VERIFIED).
    To mark a NEEDS_REVIEW player as confirmed-correct, change its status in
    Corrections_Log.xlsx to MANUALLY_VERIFIED — this script will then skip it.
    """
    if not log_file.exists():
        return set()
    df = pd.read_excel(log_file)
    if not {'Year', 'Player Name', 'Team', 'Status'}.issubset(df.columns):
        return set()
    # Use the most recent entry per (year, player, team) in case of duplicates
    if 'Timestamp' in df.columns:
        df = df.sort_values('Timestamp').groupby(
            ['Year', 'Player Name', 'Team'], sort=False
        ).last().reset_index()
    skip = set()
    for _, row in df.iterrows():
        if row.get('Status') in SKIP_STATUSES:
            skip.add((int(row['Year']), str(row['Player Name']), str(row['Team'])))
    return skip


def load_manual_updates(log_file: Path) -> dict[tuple, str]:
    """
    Return {(year, player_name, team): new_id} for all MANUALLY_UPDATED entries.
    Set New ID to the correct Transfermarkt ID and Status to MANUALLY_UPDATED in
    Corrections_Log.xlsx — the script will write that ID into _with_IDs.xlsx and
    null the MV history so it gets re-fetched.
    """
    if not log_file.exists():
        return {}
    df = pd.read_excel(log_file)
    if not {'Year', 'Player Name', 'Team', 'Status', 'New ID'}.issubset(df.columns):
        return {}
    if 'Timestamp' in df.columns:
        df = df.sort_values('Timestamp').groupby(
            ['Year', 'Player Name', 'Team'], sort=False
        ).last().reset_index()
    updates = {}
    for _, row in df.iterrows():
        if row.get('Status') == 'MANUALLY_UPDATED':
            new_id = _id_str(row.get('New ID'))
            if new_id:
                updates[(int(row['Year']), str(row['Player Name']), str(row['Team']))] = new_id
    return updates


def build_cross_year_maps(log_file: Path, current_year: int) -> tuple[dict, dict]:
    """
    Scan resolved corrections from other years to build two lookup dicts.

    id_name_map: {(old_id_str, player_name_lower) -> new_id_str}
        Keyed by BOTH the wrong ID and the player name.  Using only the ID would
        risk applying a correction to a different player who happens to share the
        same wrong auto-assigned ID (e.g. two different "Fernandez" players both
        getting ID 648195).  The name requirement ensures the correction only
        propagates to the same person appearing in another season.

    name_map: {player_name_lower -> new_id_str}
        Fallback used only when the player name uniquely resolves to exactly one
        correct ID across all other years — filters out common names like "Silva"
        or "Moreno" that refer to multiple different players.

    Both maps feed into the NEEDS_REVIEW rescue step in process_year(), where the
    candidate is validated with a club-score + activity check before being applied.

    Sources: AUTO_CORRECTED, MANUALLY_UPDATED, and CROSS_YEAR_CORRECTED entries
    where Old ID != New ID.  CROSS_YEAR_CORRECTED from prior runs propagates
    corrections to additional years on subsequent invocations.
    """
    if not log_file.exists():
        return {}, {}
    df = pd.read_excel(log_file)
    if not {'Year', 'Player Name', 'Status', 'Old ID', 'New ID'}.issubset(df.columns):
        return {}, {}

    if 'Timestamp' in df.columns:
        df = df.sort_values('Timestamp').groupby(
            ['Year', 'Player Name', 'Team'], sort=False
        ).last().reset_index()

    correction_statuses = {'AUTO_CORRECTED', 'MANUALLY_UPDATED', 'CROSS_YEAR_CORRECTED'}
    src = df[(df['Year'] != current_year) & (df['Status'].isin(correction_statuses))]

    id_name_map: dict[tuple, str] = {}
    name_ids:    dict[str, set]   = {}

    for _, row in src.iterrows():
        old_id = _id_str(row.get('Old ID'))
        new_id = _id_str(row.get('New ID'))
        if not old_id or not new_id or old_id == new_id:
            continue
        name = str(row.get('Player Name', '')).lower().strip()
        if name:
            id_name_map[(old_id, name)] = new_id
            name_ids.setdefault(name, set()).add(new_id)

    # Only keep names that resolve unambiguously to a single correct ID
    name_map = {name: next(iter(ids)) for name, ids in name_ids.items() if len(ids) == 1}
    return id_name_map, name_map


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


def process_year(year: int, log_rows: list, skip_set: set, manual_updates: dict) -> bool:
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

    # Apply manual ID updates from Corrections_Log (MANUALLY_UPDATED status)
    for i, row in df_ids.iterrows():
        pname  = str(row.get('Player Name', ''))
        team_r = str(row.get('Team', ''))
        new_id = manual_updates.get((year, pname, team_r))
        if new_id is None:
            continue
        old_id = _id_str(row.get('Player ID'))
        if old_id == new_id:
            continue
        df_ids.at[i, 'Player ID'] = int(new_id)
        corrections_this_year += 1
        print(f"  MANUALLY_UPDATED: {pname} ({team_r}): {old_id} -> {new_id}")
        if df_mv is not None:
            mask = (df_mv['Player Name'] == pname) & (df_mv['Team'] == team_r)
            if mask.any() and 'Market Value History' in df_mv.columns:
                df_mv.loc[mask, 'Market Value History'] = None

    # Load cross-year candidates; used in the NEEDS_REVIEW rescue step below.
    cross_year_id_name_map, cross_year_name_map = build_cross_year_maps(LOG_FILE, year)
    if cross_year_id_name_map or cross_year_name_map:
        print(f"  Cross-year map: {len(cross_year_id_name_map)} (id,name) pair(s), "
              f"{len(cross_year_name_map)} unique-name pair(s)")

    for i, row in df_ids.iterrows():
        player_name = str(row.get('Player Name', ''))
        team        = str(row.get('Team', ''))
        player_id   = _id_str(row.get('Player ID'))

        if player_id is None:
            continue  # no ID assigned — out of scope for this script

        if (year, player_name, team) in skip_set:
            print(f"    SKIPPED (already resolved): {player_name} ({team})")
            continue

        uefa_position = str(row.get('Position', '') or '').strip() or None

        # Try to find pre-loaded history
        existing_hist = mv_cache.get((player_name, team)) if mv_cache else None

        result = validate_player(
            player_name, team, player_id, year, existing_hist,
            uefa_position=uefa_position,
        )

        # Cross-year rescue: if validation couldn't resolve the player, try a corrected
        # ID from another year's corrections log.  The candidate is validated with a
        # club-score + activity check before being applied, preventing false positives
        # from players who share the same name or happened to receive the same wrong ID.
        if result['status'] == 'NEEDS_REVIEW':
            name_lower   = player_name.lower().strip()
            cross_cand   = cross_year_id_name_map.get((player_id, name_lower))
            if cross_cand is None:
                cross_cand = cross_year_name_map.get(name_lower)

            if cross_cand and cross_cand != player_id:
                print(f"    Cross-year candidate {cross_cand} — validating...")
                cand_hist     = fetch_mv_history(cross_cand)
                cand_score    = best_club_match_score(cand_hist, team, year - 1, year)
                cand_activity = _has_activity(cand_hist, year - 1, year)

                if cand_activity and cand_score >= MATCH_THRESHOLD:
                    print(f"    -> CROSS_YEAR_CORRECTED: {player_id} -> {cross_cand}"
                          f"  (score {cand_score:.2f})")
                    result = {
                        'status':         'CROSS_YEAR_CORRECTED',
                        'old_id':         player_id,
                        'new_id':         cross_cand,
                        'score':          cand_score,
                        'confidence':     'HIGH' if cand_score >= 0.85 else 'MEDIUM',
                        'activity_ok':    cand_activity,
                        'position_check': result['position_check'],
                        'tm_position':    None,
                        'uefa_position':  uefa_position,
                    }
                else:
                    print(f"    Cross-year candidate failed: score={cand_score:.2f}, "
                          f"activity={cand_activity}")

        pos_check_val = result['position_check']
        pos_check_str = 'SKIPPED' if pos_check_val is None else ('PASS' if pos_check_val else 'FAIL')

        # Record in log
        log_rows.append({
            'Year':           year,
            'Player Name':    player_name,
            'Team':           team,
            'Old ID':         result['old_id'],
            'New ID':         result['new_id'],
            'Status':         result['status'],
            'Confidence':     result['confidence'],
            'Club Score':     round(result['score'], 3),
            'Activity OK':    result['activity_ok'],
            'Position Check': pos_check_str,
            'UEFA Position':  result['uefa_position'],
            'TM Position':    result['tm_position'],
            'Timestamp':      datetime.now().isoformat(),
        })

        if result['status'] in ('AUTO_CORRECTED', 'CROSS_YEAR_CORRECTED'):
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

    skip_set       = load_skip_set(LOG_FILE)
    manual_updates = load_manual_updates(LOG_FILE)
    print(f"Skip set loaded:     {len(skip_set)} already-resolved player-year entries.")
    print(f"Manual updates:      {len(manual_updates)} MANUALLY_UPDATED entries to apply.\n")

    log_rows = []

    for year in args.years:
        process_year(year, log_rows, skip_set, manual_updates)

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
