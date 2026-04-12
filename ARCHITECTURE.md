# ARCHITECTURE.md — Script Internals

## fetch_market_values.py

```
process_year(year):
  1. Load _with_market_values.xlsx (or _with_IDs.xlsx on first run)
  2. Ensure columns exist; cast profile columns to object dtype (avoids FutureWarning)
  3. BULK WIKIDATA FILL (before row loop):
       - Collect all player IDs missing Country OR DOB
       - POST SPARQL query in batches of 200
       - Fill both columns; checkpoint-save
       - If everything filled: skip row loop entirely
  3b. SELENIUM WAF BOOTSTRAP (optional, not yet built):
       - If Country/DOB still missing after Wikidata, call _bootstrap_waf_cookie()
       - Re-call if 405s resume (cookie expired, TTL ~30 min–few hours)
  4. ROW LOOP (per player):
       - MV History missing → ceapi fetch with retry
       - DOB missing → derive from MV history age transitions (±1–3 months)
       - Country missing → profile page fallback (needs WAF cookie)
  5. Final save + summary
```

**Key settings:**
```python
REQUEST_DELAY_SECONDS = 3.0   # base delay between ceapi calls
DELAY_JITTER          = 1.5   # actual delay = 3.0–4.5s
MAX_RETRIES           = 3
RETRY_BASE_DELAY      = 5     # doubles each retry: 5s, 10s, 20s
SAVE_EVERY            = 20    # checkpoint every N players
```

---

## verify_dataset.py — Audit Checks

```bash
py -3.11 Code/verify_dataset.py                     # all years
py -3.11 Code/verify_dataset.py --years 2019 2020   # specific years
```

Four checks:
1. **Completeness** — rows missing MV History, DOB, or Player Country
2. **DOB plausibility** — age at CL final outside [15, 42]
3. **Cross-year consistency** — same Player ID with conflicting DOB or Country
4. **Match confidence** — rows flagged NEEDS_REVIEW in `_with_IDs.xlsx`

Output: console + `Data/Verification_Report.xlsx` (only sheets with issues written).

**Planned (not yet built):** Cross-check MV history `clubName` vs UEFA `Team` for the season window.
Flag rows where best club-match score < 0.5 even for HIGH-confidence assignments (catches silent wrong IDs like the Mane→Dembele case).

---

## compile_dataset.py — Age Calculation

Age computed at the **CL final date** for each season:

```python
CL_FINAL_DATES = {
    2018: date(2018, 5, 26),  # Kiev
    2019: date(2019, 6,  1),  # Madrid
    2020: date(2020, 8, 23),  # Lisbon (COVID)
    2021: date(2021, 5, 29),  # Porto
    2022: date(2022, 5, 28),  # Paris
    2023: date(2023, 6, 10),  # Istanbul
    2024: date(2024, 6,  1),  # London
    2025: date(2025, 5, 31),  # Munich
}
```

`calc_age_at_cl_final(dob_raw, end_year)` handles:
- ISO string `"1992-10-02"` (Wikidata — exact)
- Integer birth year `1992` (MV derivation — ±1 year accuracy)
