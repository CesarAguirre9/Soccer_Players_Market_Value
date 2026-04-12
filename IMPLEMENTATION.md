# IMPLEMENTATION.md — Key Implementation Details

## Season Year Convention
File year = year the season **ends**. Season 17/18 → `UEFA Stats 2018.xlsx`.
- `MV_start` = average Transfermarkt snapshots in year `file_year - 1`
- `MV_end`   = average Transfermarkt snapshots in year `file_year`

Both calculated in `compile_dataset.py` via `calc_mv_for_year(history, year)`.

---

## Player ID Validation Strategy (`process_uefa_stats.py`)
Three tiers, cheapest first:
1. **Position filter** (free) — skip candidates with wrong position category
2. **Current-club match** (free) — HIGH confidence for recent seasons
3. **MV history club check** (1 API call per remaining candidate) — definitive for all seasons

Retirement filter intentionally absent: players from 2018–2020 may be retired with zero current MV.
The MV history check handles this — their history shows the correct club for the season window.

**Known data quality issue:** `_with_market_values.xlsx` for **2018 and 2023** have wrong IDs (~64 in 2018).
Fix: `py -3.11 Code/validate_player_ids.py --years 2018 2023`.

---

## Market Value History Parsing
Stored as Python dict string in Excel. Contains `datetime.datetime(...)` literals that
`ast.literal_eval` can't handle. Use:

```python
import re, ast
s = re.sub(r'datetime\.datetime\([^)]+\)', 'None', raw_string)
data = ast.literal_eval(s)
history = data.get('marketValueHistory', [])
```

---

## Club Name Fuzzy Matching
UEFA stats use abbreviated names ("B. Dortmund", "Man City"). `TEAM_NORMALIZATION` dict in
`process_uefa_stats.py` and `validate_player_ids.py` maps these to full Transfermarkt names.
Combined with `difflib.SequenceMatcher` at threshold **0.65**.
