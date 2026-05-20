# Community Supply Chain Intelligence (CSI) — Global Platform

**Real-time energy & water shock early warning for vulnerable communities.**

Monitors upstream supply chain signals and translates them into localized,
actionable intelligence for communities in ODA-eligible countries.

**Live dashboard:** [global.criticalto.ca](https://global.criticalto.ca) 
**Sister platform:** [data.criticalto.ca](https://data.criticalto.ca) — Toronto Infrastructure Intelligence

---

## What it watches

| Sector | Signal | Source | Frequency |
|---|---|---|---|
| Energy | Brent crude + LNG spot | Stooq / EIA | Daily |
| Chokepoint | Vessel density — Hormuz, Bab-el-Mandeb, Suez, Malacca | AISstream.io / GDELT proxy | 6-hourly |
| Food | FAO Food Price Index (FFPI + 5 sub-indices) | FAOSTAT | Monthly |
| Freight | Baltic Dry Index + Freightos FBX | Stooq / Freightos | Daily |
| Water | GloFAS river anomaly signal | EU Copernicus | Weekly |
| Geopolitical | Supply shock / chokepoint / food crisis news volume | GDELT DOC 2.0 | 6-hourly |
| WASH | Global safe water access population | WHO/UNICEF JMP | Annual |
| Currency | ODA currency stress vs USD (BDT, KES, ETB, BOB, PEN, MXN, CLP, NPR) | Open Exchange Rates / Stooq | Daily |

---

## Alert thresholds

| Indicator | WATCH | ALERT |
|---|---|---|
| Brent crude | >$80/bbl | >$95/bbl |
| FAO FFPI | >115 pts | >130 pts |
| Baltic Dry Index | >1,500 | >2,500 |
| FBX container rate | >$2,000/FEU | >$3,500/FEU |
| GDELT supply shock volume | >0.05% | >0.15% |
| ODA currencies | Region-specific | Region-specific |

---

## Setup

### 1. Create the GitHub repo

```
Repo name: csi-global 
Visibility: Public 
```

### 2. Upload files

Upload `csi_scraper.py` and `index.html` to the repo root.
Create `.github/workflows/csi-scraper.yml`.

### 3. Add API keys as GitHub Secrets

 **Settings → Secrets and variables → Actions → New repository secret**:

| Secret name | Where to get it | Cost |
|---|---|---|
| `EIA_API_KEY` | [eia.gov/opendata/register.php](https://www.eia.gov/opendata/register.php) | Free |
| `OPENEXCHANGERATES_APP_ID` | [openexchangerates.org/signup/free](https://openexchangerates.org/signup/free) | Free (500 req/mo) |
| `AISSTREAM_API_KEY` | [aisstream.io](https://aisstream.io) | Free tier |

All three are optional — the scraper runs without them using fallback sources,
but AISstream gives you real vessel counts instead of GDELT news proxies.

### 4. Enable GitHub Pages

**Settings → Pages → Source: Deploy from branch → main → / (root)**

### 5. Point your domain

Add a CNAME file to the repo with `global.criticalto.ca`,
then add a CNAME record in your DNS pointing to `sargjones.github.io`.

---

## Running locally

```bash
pip install requests beautifulsoup4
python csi_scraper.py              # full run
python csi_scraper.py --sector energy   # single sector
python csi_scraper.py --dry-run    # print only, no file writes
```

---

## Output format

`csi_data_latest.json` — always-current snapshot  
`csi_data_YYYYMMDD.json` — daily archive

```json
{
  "meta": {
    "platform": "Community Supply Chain Intelligence (CSI)",
    "generated_utc": "2026-05-07T01:00:00Z",
    "platform_status": "watch",
    "counts": { "ok": 12, "watch": 4, "alert": 1, "error": 0, "manual": 3 },
    "alert_summary": [...]
  },
  "indicators": [
    {
      "indicator": "Brent Crude Price",
      "value": 97.40,
      "unit": "USD/bbl",
      "source": "Stooq",
      "status": "alert",
      "notes": "CRITICAL — supply shock threshold...",
      "sector": "energy",
      "region": "global",
      "ts": "2026-05-07T01:00:00Z"
    }
  ]
}
```

---

## Architecture

```
csi_scraper.py          <- fetch functions, threshold logic, JSON output
csi_data_latest.json    <- always-current data (read by dashboard)
csi_data_YYYYMMDD.json  <- daily archive
index.html              <- dashboard (reads csi_data_latest.json via fetch)
.github/workflows/
  csi-scraper.yml       <- GitHub Actions schedule (every 6h)
```

---

## Pilot regions

**Phase 1 targets (grant period 2027):**
- South Asia — Bangladesh (Dhaka focus) — BDT currency, LNG/LPG cooking fuel dependency
- East Africa — Kenya (Nairobi focus) — KES currency, charcoal/kerosene, water stress
- Andean LatAm — Bolivia + Peru — BOB/PEN currency, fertilizer dependency, food sovereignty

---

## Related

- **Critical TO** (Toronto): [criticalto.ca](https://criticalto.ca) — sister platform, same architecture
- **GRP TECH4Resilience application**: May 2026 — funding for pilot community delivery layer
- **WFP Innovation Accelerator**: June/July 2026 intake target

---

*Built by Sarah Jones G — Brantford, Ontario, Canada*  
*Part of the Community Supply Chain Intelligence humanitarian technology initiative.*
