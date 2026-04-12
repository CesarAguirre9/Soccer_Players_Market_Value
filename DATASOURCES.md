# DATASOURCES.md — Data Sources Reference

## 1. Transfermarkt ceapi (Market Value History)
**URL:** `https://www.transfermarkt.{domain}/ceapi/marketValueDevelopment/graph/{player_id}`
**Used for:** `Market Value History` column only.
**Why it works:** JSON endpoint Transfermarkt uses for their own embedded widgets — can't block without breaking their site. Domain cycles between `.us` and `.com`.
**Blocking:** IP-based. If blocked: stop immediately, wait overnight. Test recovery with `--limit 3`. Mobile hotspot unblocks instantly.

---

## 2. Wikidata SPARQL (Country + DOB)
**URL:** `https://query.wikidata.org/sparql`
**Used for:** `Player Country` (P27), `Date of Birth` (P569), keyed by Transfermarkt ID (P2446).
**How:** Bulk SPARQL POST, 200 players per batch, runs before the row loop — fills both columns in seconds.
**Coverage:** ~693/698 countries, ~478/692 DOBs for 2019 in one batch.
**No blocking:** Free public endpoint; 1s polite pause between batches.
**DOB format:** ISO datetime `"1992-10-02T00:00:00Z"` — stripped to `"1992-10-02"`.

---

## 3. DOB Derivation from MV History (fallback)
`_derive_birth_date()` estimates DOB from age transitions in MV history snapshots (accuracy ±1–3 months).
Runs automatically in the row loop for players Wikidata misses. Covers virtually all remaining cases.

---

## 4. Transfermarkt HTML Pages — Blocked by AWS WAF
**Affected:** `/profil/spieler/`, `/schnellsuche/`, `/marktwertverlauf/`
**Root cause:** AWS WAF silent JS challenge. Real browsers auto-solve it; `curl_cffi` mimics TLS but can't execute JS → never gets the cookie → HTTP 405.
**Status:** Country + DOB now replaced by Wikidata + MV derivation. `_do_fetch_profile()` remains as last-resort fallback but expects 405.

---

## 5. Selenium WAF Cookie Bootstrap (planned)
**When:** After `fetch_market_values.py`, run `verify_dataset.py` to find remaining gaps.
If < 30 missing → look up manually. If larger (especially historical expansion) → use this.

**How it works:**
1. Selenium drives Chrome to `transfermarkt.com`
2. Chrome auto-solves the WAF JS challenge (invisible, no user input)
3. Extract WAF cookie from Selenium session
4. Inject into the `curl_cffi` session
5. Cookie TTL ~30 min–few hours; re-bootstrap on expiry

**One solve unlocks many requests** — not per-player.

**Implementation sketch (add to `fetch_market_values.py`):**
```python
def _bootstrap_waf_cookie():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    import time

    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.transfermarkt.com/")
        time.sleep(4)   # allow WAF JS challenge to complete
        session = _get_session()
        for cookie in driver.get_cookies():
            session.cookies.set(
                cookie['name'], cookie['value'],
                domain=cookie.get('domain', '.transfermarkt.com')
            )
        print("  [WAF bootstrap] cookies injected from Selenium session")
    finally:
        driver.quit()
```

**Where to call:** before row loop, after Wikidata fill, when Country/DOB gaps remain.
**Priority:** Low for 2018–2025 (Wikidata ~99% coverage). Build during historical expansion.
