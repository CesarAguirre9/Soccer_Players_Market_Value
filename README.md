# Soccer Players Market Value — Champions League Performance Study

## Research Question

> **Does a player's performance in the UEFA Champions League have a measurable impact on their market value?**

This repository contains the full end-to-end data engineering pipeline for a quantitative economics study examining the relationship between individual player performance statistics in the UEFA Champions League and changes in their Transfermarkt market valuations across seasons.

---

## Project Overview

Market value in professional football is a proxy for player quality used in transfer negotiations, contract discussions, and academic labour-economics research. This project builds a novel panel dataset by merging two independent data sources:

1. **UEFA.com** — per-player performance statistics (minutes played, goals, assists, distance covered, top speed, matches) scraped directly from the official UEFA Champions League statistics portal.
2. **Transfermarkt** — time-series market valuation history for each player, accessed via a local instance of the open-source [transfermarkt-api](https://github.com/felipeall/transfermarkt-api).

The final dataset tracks each player's performance in a given CL season alongside their market value at the **start** and **end** of that season, enabling difference-in-differences and panel regression analyses.

---

## Skills Demonstrated

| Area                          | Details                                                                                                                                   |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Web Scraping**              | Selenium WebDriver for dynamic JavaScript-rendered pages; BeautifulSoup for HTML parsing; automated scroll-to-load pagination             |
| **API Integration**           | Programmatic access to a RESTful web-scraping API (Transfermarkt); request error handling and rate-limit management                       |
| **Data Pipeline Design**      | Multi-stage ETL pipeline with checkpointing, resumability, and idempotent steps                                                           |
| **Data Validation**           | Fuzzy string matching (`difflib`) for entity resolution across two data sources; automated ID cross-validation using historical club data |
| **Data Wrangling**            | `pandas` for multi-file merging, type coercion, missing-value handling, and multi-format date parsing                                     |
| **Python**                    | Modular scripts with `argparse` CLI, `pathlib`, `ast.literal_eval` for structured string parsing, dataclasses                             |
| **Academic Research Support** | Dataset construction methodology designed for panel econometrics (two market-value observations per player per season)                    |

---

## Data Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE 1 — UEFA.com                                                │
│  Selenium scraper → per-player stats for each CL season            │
│  Output: Data/UEFA Stats {year}.xlsx                                │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Assign Player IDs  (Code/process_uefa_stats.py)          │
│  Transfermarkt player search → validated top-N matching             │
│  Validation: position filter → current-club filter → MV history    │
│  Output: UEFA Stats {year}_with_IDs.xlsx  + Match_Confidence column │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Validate & Correct IDs  (Code/validate_player_ids.py)    │
│  Cross-checks assigned IDs against Transfermarkt MV club history   │
│  Auto-corrects mismatches; writes Corrections_Log.xlsx audit trail  │
│  Output: corrected _with_IDs.xlsx files                             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE 2 — Transfermarkt                                           │
│  STEP 3 — Fetch Market Values  (Code/fetch_market_values.py)       │
│  Per-player market value history + profile (nationality, DOB)       │
│  Resumable: skips already-fetched players                           │
│  Output: UEFA Stats {year}_with_market_values.xlsx                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Build Club Country Map  (Code/build_club_country_map.py) │
│  Resolves 95 unique club names → country via Transfermarkt search  │
│  Output: Data/club_country_map.json  (cached, editable)             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 5 — Compile Final Dataset  (Code/compile_dataset.py)         │
│  Parses MV history → calculates MV_start / MV_end per season       │
│  Computes age at season start, formats season label                 │
│  Output: Data/Final_Dataset.xlsx                                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Final Dataset Schema

| Column           | Description                                                           |
| ---------------- | --------------------------------------------------------------------- |
| `Season`         | CL season label (e.g. `21/22`)                                        |
| `Player`         | Player name                                                           |
| `Player Country` | Nationality                                                           |
| `Position`       | Goalkeeper / Defender / Midfielder / Forward                          |
| `Age`            | Age at season start (August 1)                                        |
| `Team`           | Club in the CL that season                                            |
| `Team Country`   | Club's home country                                                   |
| `Minutes`        | Total minutes played                                                  |
| `Matches`        | Matches played                                                        |
| `Goals`          | Goals scored                                                          |
| `Assists`        | Assists                                                               |
| `Distance`       | Total distance covered (km)                                           |
| `MV_start`       | Average Transfermarkt market value (€M) in the season's starting year |
| `MV_end`         | Average Transfermarkt market value (€M) in the season's ending year   |

**Coverage:** 8 CL seasons (2017/18 – 2024/25) · ~700–900 players per season · ~6,000 player-season observations

---

## Repository Structure

```
Soccer_Players_Market_Value/
│
├── Code/                          # All pipeline scripts
│   ├── process_uefa_stats.py      # Step 1 – assign + validate player IDs
│   ├── validate_player_ids.py     # Step 2 – retroactive ID correction
│   ├── fetch_market_values.py     # Step 3 – fetch Transfermarkt MV history
│   ├── build_club_country_map.py  # Step 4 – club → country lookup table
│   └── compile_dataset.py         # Step 5 – compile final dataset
│
├── Data/
│   ├── UEFA Stats {year}.xlsx                    # Raw scraped stats
│   ├── UEFA Stats {year}_with_IDs.xlsx           # + player IDs
│   ├── UEFA Stats {year}_with_market_values.xlsx # + MV history + profile
│   ├── club_country_map.json                     # Club → country mapping
│   ├── Corrections_Log.xlsx                      # ID correction audit trail
│   ├── Unique_Players_with_Empty_IDs.xlsx        # Players needing manual ID
│   └── Final_Dataset.xlsx                        # Final compiled dataset
│
├── Webscraping_VF_04.05.2024.ipynb  # UEFA.com Selenium scraping notebook
├── CLAUDE.md                         # AI assistant context file
└── README.md
```

---

## How to Run

**Prerequisites:**

- Python 3.11+
- `pandas`, `openpyxl`, `numpy`, `selenium`, `beautifulsoup4`, `unidecode`
- Local clone of [transfermarkt-api](https://github.com/felipeall/transfermarkt-api)

**Step 0 — Scrape a new season** _(one-time per season)_
Open `Webscraping_VF_04.05.2024.ipynb`, set `year = <YYYY>`, and run all cells.

**Step 1 — Assign player IDs**

```bash
py Code/process_uefa_stats.py --year 2026
```

**Step 2 — Validate & correct IDs**

```bash
py Code/validate_player_ids.py --years 2026
# Review Data/Corrections_Log.xlsx for any NEEDS_REVIEW flags
```

**Step 3 — Fetch market values & player profiles** _(long-running, ~hours)_

```bash
py Code/fetch_market_values.py
```

**Step 4 — Build club country map** _(run once; re-run to add new clubs)_

```bash
py Code/build_club_country_map.py
```

**Step 5 — Compile final dataset**

```bash
py Code/compile_dataset.py
```

---

## Technical Notes

- **ID validation** uses fuzzy club-name matching (`difflib.SequenceMatcher`) between Transfermarkt market value history entries and the expected club from the UEFA stats, resolving entity-resolution challenges from abbreviated team names and retired/transferred players.
- **Resumability** — `fetch_market_values.py` skips already-fetched columns, making long runs safe to interrupt and restart.
- **Audit trail** — every automated ID correction is written to `Data/Corrections_Log.xlsx` with the old ID, new ID, confidence score, and timestamp.

---

## Data Sources

- [UEFA Champions League Statistics](https://www.uefa.com/uefachampionsleague/history/seasons/)
- [Transfermarkt](https://www.transfermarkt.com) via [transfermarkt-api](https://github.com/felipeall/transfermarkt-api)
