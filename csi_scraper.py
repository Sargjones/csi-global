"""
Community Supply Chain Intelligence (CSI) — Global Scraper v1.0
================================================================
Watches upstream supply chain signals across 8 categories and
produces a JSON file consumed by the global dashboard.

SIGNALS
-------
  1. Energy      — Brent crude (Stooq), LNG spot (EIA)
  2. Chokepoints — Vessel density in Hormuz / Bab-el-Mandeb / Suez (AISstream.io)
  3. Food        — FAO Food Price Index sub-indices (FAOSTAT CSV)
  4. Freight     — Freightos Baltic Index FBX proxy (scraped)
  5. Water       — GloFAS river anomaly signal (Copernicus/EU)
  6. Geopolitical— GDELT DOC 2.0 API (supply shock keyword volume)
  7. WASH        — WHO/UNICEF JMP improved water access (static annual, manual update)
  8. Currency    — ODA currency stress vs USD (Open Exchange Rates free tier)

OUTPUT
------
  csi_data_latest.json          — always-overwritten latest
  csi_data_YYYYMMDD.json        — daily snapshot

SETUP
-----
  pip install requests beautifulsoup4
  
  Optional env vars (set as GitHub Actions secrets):
    EIA_API_KEY          — free at https://www.eia.gov/opendata/register.php
    OPENEXCHANGERATES_APP_ID  — free at https://openexchangerates.org/signup/free

  AISstream.io:
    AISSTREAM_API_KEY    — free at https://aisstream.io  (WebSocket; 
                           we use their REST snapshot endpoint here)

USAGE
-----
  python csi_scraper.py           # full run
  python csi_scraper.py --sector energy
  python csi_scraper.py --dry-run

THRESHOLDS (alert levels)
--------------------------
  GREEN  = normal operating range
  AMBER  = elevated — watch
  RED    = critical — alert warranted
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Session ──────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "CSI-Scraper/1.0 Community Supply Chain Intelligence "
        "(contact: your-email@example.com)"
    ),
    "Accept": "application/json, text/html, text/csv, */*",
})
TIMEOUT = 30

# ── API keys from environment ─────────────────────────────────────────────────

EIA_KEY          = os.environ.get("EIA_API_KEY", "")
OER_APP_ID       = os.environ.get("OPENEXCHANGERATES_APP_ID", "")
AISSTREAM_KEY    = os.environ.get("AISSTREAM_API_KEY", "")

# ── Response helpers ──────────────────────────────────────────────────────────

def _ok(indicator, value, unit, source, status="ok", notes="", sector="", region="global"):
    return {
        "indicator": indicator,
        "value": value,
        "unit": unit,
        "source": source,
        "status": status,
        "notes": notes,
        "sector": sector,
        "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _err(indicator, message, source, sector="", region="global"):
    return {
        "indicator": indicator,
        "value": None,
        "unit": "",
        "source": source,
        "status": "error",
        "notes": f"ERROR: {message}",
        "sector": sector,
        "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _manual(indicator, value, unit, source, notes="", sector="", region="global"):
    return {
        "indicator": indicator,
        "value": value,
        "unit": unit,
        "source": source,
        "status": "manual",
        "notes": notes,
        "sector": sector,
        "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _threshold_energy(brent_usd):
    """Brent thresholds: <80 GREEN, 80-95 AMBER, >95 RED"""
    if brent_usd is None:
        return "unknown"
    if brent_usd < 80:
        return "ok"
    if brent_usd < 95:
        return "watch"
    return "alert"

def _threshold_food(ffpi):
    """FAO FFPI thresholds: <115 GREEN, 115-130 AMBER, >130 RED"""
    if ffpi is None:
        return "unknown"
    if ffpi < 115:
        return "ok"
    if ffpi < 130:
        return "watch"
    return "alert"

def _threshold_freight(fbx):
    """FBX thresholds: <2000 GREEN, 2000-3500 AMBER, >3500 RED (USD/FEU)"""
    if fbx is None:
        return "unknown"
    if fbx < 2000:
        return "ok"
    if fbx < 3500:
        return "watch"
    return "alert"

def _threshold_gdelt(pct):
    """GDELT volume pct thresholds: <0.05 GREEN, 0.05-0.15 AMBER, >0.15 RED"""
    if pct is None:
        return "unknown"
    if pct < 0.05:
        return "ok"
    if pct < 0.15:
        return "watch"
    return "alert"

# ── 1. ENERGY — Brent Crude ───────────────────────────────────────────────────

def fetch_brent_crude():
    """Brent spot price from Stooq (same source as Critical TO Toronto)."""
    indicator = "Brent Crude Price"
    source    = "Stooq"
    sector    = "energy"
    try:
        # Tickers to try in order — Stooq occasionally changes futures codes
        # lcox.f = ICE Brent, cl.f = WTI (close proxy), brent.f = spot
        url  = "https://stooq.com/q/d/l/?s=lcox.f&i=d"
        # If 403/404, also try: s=brent.f or s=cb.f
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
        if not lines:
            return _err(indicator, "No data rows", source, sector)
        last  = lines[-1].split(",")
        price = float(last[4])  # Close
        status = _threshold_energy(price)
        notes = (
            f"Brent at ${price:.2f}/bbl. "
            + ("Normal range." if status == "ok" else
               "Elevated — household energy costs rising globally." if status == "watch" else
               "CRITICAL — supply shock threshold. LPG/fuel price spikes likely in import-dependent communities within 4–6 weeks.")
        )
        return _ok(indicator, price, "USD/bbl", source, status, notes, sector)
    except Exception as exc:
        return _err(indicator, str(exc), source, sector)


def fetch_lng_spot():
    """LNG spot price (Henry Hub proxy) from EIA free API, or Stooq fallback."""
    indicator = "LNG / Natural Gas Spot"
    source    = "EIA"
    sector    = "energy"
    # Try EIA first if key exists
    if EIA_KEY:
        try:
            url = (
                f"https://api.eia.gov/v2/natural-gas/pri/fut/data/"
                f"?api_key={EIA_KEY}&frequency=daily&data[0]=value"
                f"&facets[series][]=RNGC1&sort[0][column]=period"
                f"&sort[0][direction]=desc&length=1"
            )
            resp = SESSION.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            val  = float(data["response"]["data"][0]["value"])
            notes = f"Henry Hub natural gas at ${val:.2f}/MMBtu."
            return _ok(indicator, val, "USD/MMBtu", source, "ok", notes, sector)
        except Exception as exc:
            pass  # fall through to Stooq

    # Stooq fallback — NG continuous futures
    try:
        url  = "https://stooq.com/q/d/l/?s=ngx.f&i=d"
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
        if not lines:
            return _err(indicator, "No data rows", "Stooq", sector)
        last  = lines[-1].split(",")
        price = float(last[4])
        notes = f"NG futures at ${price:.2f}/MMBtu (Stooq). Set EIA_API_KEY for direct LNG data."
        return _ok(indicator, price, "USD/MMBtu", "Stooq", "ok", notes, sector)
    except Exception as exc:
        return _err(indicator, str(exc), "Stooq", sector)


# ── 2. CHOKEPOINTS — AISstream / density proxy ────────────────────────────────

# Chokepoint bounding boxes (lat_min, lat_max, lon_min, lon_max)
CHOKEPOINTS = {
    "Strait of Hormuz": (25.5, 27.0, 55.5, 57.5),
    "Bab-el-Mandeb":   (11.5, 13.5, 42.5, 44.5),
    "Suez Canal":      (29.5, 32.5, 32.0, 33.5),
    "Strait of Malacca": (1.0, 6.0, 100.0, 104.5),
}

def fetch_chokepoint_status():
    """
    Queries AISstream.io REST snapshot API for vessel counts inside each
    chokepoint bounding box. Returns alert status based on vessel density
    anomaly (very low density = disruption signal).
    
    Falls back to GDELT keyword search for chokepoint news if AIS unavailable.
    """
    results = []
    
    for name, (lat_min, lat_max, lon_min, lon_max) in CHOKEPOINTS.items():
        indicator = f"Chokepoint — {name}"
        source    = "AISstream.io"
        sector    = "chokepoint"
        
        if AISSTREAM_KEY:
            try:
                # AISstream REST snapshot for bounding box vessel count
                url = "https://stream.aisstream.io/v0/vessels"
                params = {
                    "BoundingBoxes": f"[[{lat_min},{lon_min}],[{lat_max},{lon_max}]]",
                    "FilterMessageTypes": ["PositionReport"],
                }
                headers = {"Authorization": f"Bearer {AISSTREAM_KEY}"}
                resp = SESSION.get(url, params=params, headers=headers, timeout=TIMEOUT)
                resp.raise_for_status()
                data  = resp.json()
                count = len(data.get("vessels", []))
                
                # Rough normal baseline counts (tankers + cargo in transit)
                # These baselines are approximate — refine after first weeks of data
                baselines = {
                    "Strait of Hormuz":    80,
                    "Bab-el-Mandeb":       60,
                    "Suez Canal":          45,
                    "Strait of Malacca":   120,
                }
                baseline = baselines.get(name, 50)
                ratio    = count / baseline if baseline else 1.0
                
                if ratio > 0.8:
                    status = "ok"
                    notes  = f"{count} vessels detected (normal baseline ~{baseline}). Traffic flowing."
                elif ratio > 0.5:
                    status = "watch"
                    notes  = f"{count} vessels detected vs baseline ~{baseline}. Reduced traffic — monitor."
                else:
                    status = "alert"
                    notes  = f"CRITICAL: only {count} vessels vs baseline ~{baseline}. Significant disruption signal."
                
                results.append(_ok(indicator, count, "vessels", source, status, notes, sector))
                continue
                
            except Exception as exc:
                # Fall through to GDELT news proxy
                pass
        
        # Fallback: GDELT news volume for chokepoint disruption keywords
        try:
            keyword = f'"{name}" disruption OR closure OR attack OR blockade'
            url = (
                f"https://api.gdeltproject.org/api/v2/doc/doc"
                f"?query={requests.utils.quote(keyword)}"
                f"&mode=artlist&maxrecords=10&format=json&timespan=7d"
            )
            resp  = SESSION.get(url, timeout=TIMEOUT)
            data  = resp.json() if resp.ok else {}
            count = len(data.get("articles", []))
            
            if count == 0:
                status = "ok"
                notes  = f"No significant disruption news for {name} in past 7 days. (AIS key not set — set AISSTREAM_API_KEY for vessel counts.)"
            elif count < 5:
                status = "watch"
                notes  = f"{count} disruption-related news items in past 7 days for {name}. Monitor."
            else:
                status = "alert"
                notes  = f"ELEVATED: {count} disruption-related news items in past 7 days for {name}. Verify with vessel tracking."
            
            results.append(_ok(indicator, count, "news items (7d)", "GDELT proxy", status, notes, sector))
            
        except Exception as exc:
            results.append(_err(indicator, str(exc), "GDELT proxy", sector))
    
    return results


# ── 3. FOOD — FAO Food Price Index ────────────────────────────────────────────

def fetch_fao_food_price_index():
    """
    FAO Food Price Index from FAOSTAT bulk CSV download.
    Returns overall FFPI + 5 sub-indices (cereals, dairy, meat, oils, sugar).
    Published monthly on the first Thursday.
    """
    results  = []
    sector   = "food"
    source   = "FAO / FAOSTAT"
    
    try:
        # FAOSTAT prices domain — FFPI series
        # Direct CSV endpoint for the World Food Situation food price index
        url  = "https://www.fao.org/fishery/static/Data/FoodPriceIndex.xlsx"
        # Fallback: parse from the FAO World Food Situation page
        # The FFPI page publishes a downloadable Excel — we parse the HTML for the latest figure
        
        page_url = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
        resp     = SESSION.get(page_url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup     = BeautifulSoup(resp.text, "html.parser")
        
        # Find the FFPI value — FAO publishes it prominently on this page
        ffpi_val = None
        month_str = ""
        
        # Look for pattern like "128.5" in the page text near "FFPI" or "Food Price Index"
        text = soup.get_text(" ", strip=True)
        # Pattern: "averaged NNN.N points in MONTH YYYY"
        m = re.search(
            r"FFPI[^\d]*averaged\s+([\d,]+\.?\d*)\s+points\s+in\s+(\w+\s+\d{4})",
            text, re.IGNORECASE
        )
        if not m:
            # Try broader pattern
            m = re.search(
                r"averaged\s+([\d,]+\.?\d*)\s+points\s+in\s+(\w+\s+\d{4})",
                text, re.IGNORECASE
            )
        
        if m:
            ffpi_val  = float(m.group(1).replace(",", ""))
            month_str = m.group(2)
        
        if ffpi_val is None:
            # Try to get value from any number near "128" range in the page
            # Last resort — look for the index table values
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    for cell in cells:
                        if "Food Price" in cell.get_text():
                            # Get the next numeric cell
                            for sibling in cells:
                                try:
                                    v = float(sibling.get_text(strip=True).replace(",", ""))
                                    if 50 < v < 300:  # Plausible FFPI range
                                        ffpi_val = v
                                        break
                                except ValueError:
                                    continue
        
        if ffpi_val:
            status = _threshold_food(ffpi_val)
            notes = (
                f"FAO FFPI: {ffpi_val} points ({month_str}). Base period 2014–2016 = 100. "
                + ("Normal food price environment." if status == "ok" else
                   "Food prices elevated — household food budgets under pressure in import-dependent communities." if status == "watch" else
                   "CRITICAL food price environment — acute food security risk in low-income import-dependent communities.")
            )
            results.append(_ok("FAO Food Price Index (FFPI)", ffpi_val, "index (2014-16=100)", source, status, notes, sector))
        else:
            results.append(_err("FAO Food Price Index (FFPI)", "Could not parse FFPI value from FAO page", source, sector))
    
    except Exception as exc:
        results.append(_err("FAO Food Price Index (FFPI)", str(exc), source, sector))
    
    # Sub-indices — from FAOSTAT API
    sub_indices = {
        "Cereals": "2905",
        "Vegetable Oils": "2906",
        "Dairy": "2907",
        "Meat": "2908",
        "Sugar": "2909",
    }
    
    for name, item_code in sub_indices.items():
        try:
            # FAOSTAT JSON API — price indices domain
            url = (
                f"https://fenixservices.fao.org/faostat/api/v1/en/data/FP"
                f"?area=1&item={item_code}&element=2909&year=2026&type=chart&output_type=json"
            )
            resp = SESSION.get(url, timeout=TIMEOUT)
            if resp.ok:
                data = resp.json()
                rows = data.get("data", [])
                if rows:
                    val = float(rows[-1].get("Value", 0))
                    results.append(_ok(
                        f"FAO {name} Price Index",
                        val, "index (2014-16=100)", "FAOSTAT",
                        "ok",
                        f"{name} sub-index: {val:.1f}",
                        sector
                    ))
                else:
                    results.append(_manual(
                        f"FAO {name} Price Index",
                        None, "index (2014-16=100)", "FAOSTAT",
                        "FAOSTAT API returned no rows — retrieve from fao.org/worldfoodsituation manually",
                        sector
                    ))
            else:
                results.append(_manual(
                    f"FAO {name} Price Index",
                    None, "index (2014-16=100)", "FAOSTAT",
                    f"FAOSTAT API {resp.status_code} — retrieve from fao.org/worldfoodsituation manually",
                    sector
                ))
        except Exception as exc:
            results.append(_manual(
                f"FAO {name} Price Index",
                None, "index (2014-16=100)", "FAOSTAT",
                f"Exception: {exc}",
                sector
            ))
    
    return results


# ── 4. FREIGHT — Freightos Baltic Index proxy ─────────────────────────────────

def fetch_freight_rates():
    """
    Freightos Baltic Index (FBX) — global container freight rate proxy.
    Scrapes the public FBX composite from Freightos.
    Falls back to Baltic Dry Index from Stooq.
    """
    results = []
    sector  = "freight"
    
    # Try Baltic Dry Index from Stooq (reliable free source)
    try:
        url  = "https://stooq.com/q/d/l/?s=bdi&i=d"
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
        if lines:
            last = lines[-1].split(",")
            bdi  = float(last[4])
            if bdi < 1500:
                status = "ok"
                notes  = f"Baltic Dry Index: {bdi:.0f}. Low freight rates — shipping capacity available."
            elif bdi < 2500:
                status = "watch"
                notes  = f"Baltic Dry Index: {bdi:.0f}. Elevated freight rates — supply chain cost pressure building."
            else:
                status = "alert"
                notes  = f"ELEVATED: Baltic Dry Index: {bdi:.0f}. High freight rates — significant supply chain cost pressure."
            results.append(_ok("Baltic Dry Index (BDI)", bdi, "index", "Stooq / Baltic Exchange", status, notes, sector))
    except Exception as exc:
        results.append(_err("Baltic Dry Index (BDI)", str(exc), "Stooq", sector))
    
    # Try Freightos FBX page scrape
    try:
        url  = "https://fbx.freightos.com/"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        
        # Look for FBX composite value (typically in $NNN or $N,NNN format)
        m = re.search(r'\$\s*([\d,]+)', text)
        if m:
            fbx = float(m.group(1).replace(",", ""))
            if 200 < fbx < 30000:  # sanity check — plausible FEU range
                status = _threshold_freight(fbx)
                notes  = (
                    f"Freightos Baltic Index (FBX): ${fbx:,.0f}/FEU. "
                    + ("Normal shipping costs." if status == "ok" else
                       "Elevated container freight costs." if status == "watch" else
                       "CRITICAL freight costs — supply chain disruption materializing in consumer prices.")
                )
                results.append(_ok("Freightos Baltic Index (FBX)", fbx, "USD/FEU", "Freightos", status, notes, sector))
    except Exception:
        pass  # BDI already captured above
    
    if not results:
        results.append(_err("Freight Rate Index", "All freight rate sources failed", "Stooq / Freightos", sector))
    
    return results


# ── 5. WATER — GloFAS river anomaly / USGS ───────────────────────────────────

def fetch_water_stress():
    """
    Global water stress proxy using GloFAS (EU Copernicus) seasonal outlook 
    and USGS water resources data for context.
    
    GloFAS publishes a seasonal hydrological outlook as a PDF/HTML — we 
    extract the summary alert level. This is primarily a manual-tier indicator
    until the GloFAS REST API is fully available.
    """
    results = []
    sector  = "water"
    
    # GloFAS seasonal outlook — check for anomaly signal
    try:
        url  = "https://www.globalfloods.eu/glofas-forecasting/"
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)[:3000]
        
        # Look for alert keywords
        alert_keywords = ["exceptional", "severe", "extreme drought", "water shortage", "crisis"]
        watch_keywords = ["below normal", "low flow", "drought", "deficit"]
        
        found_alert = any(kw in text.lower() for kw in alert_keywords)
        found_watch = any(kw in text.lower() for kw in watch_keywords)
        
        if found_alert:
            status = "alert"
            notes  = "GloFAS seasonal outlook indicates exceptional/severe water stress in one or more monitored basins. Review full outlook at globalfloods.eu."
        elif found_watch:
            status = "watch"
            notes  = "GloFAS seasonal outlook indicates below-normal river flow conditions in monitored basins."
        else:
            status = "ok"
            notes  = "GloFAS seasonal outlook — no exceptional water stress signals detected. Review globalfloods.eu for detailed basin analysis."
        
        results.append(_ok(
            "GloFAS Global Water Stress Signal",
            None, "qualitative", "GloFAS / EU Copernicus",
            status, notes, sector
        ))
    except Exception as exc:
        results.append(_err("GloFAS Global Water Stress Signal", str(exc), "GloFAS", sector))
    
    # WHO "Water Bankruptcy" context indicator — static with last-known value
    results.append(_manual(
        "Global Safe Water Access (JMP)",
        2200,  # approximate millions without safely managed drinking water
        "million people",
        "WHO/UNICEF JMP 2025",
        "Static annual indicator. ~2.2B people lack safely managed drinking water (JMP 2025). Update annually from washdata.org.",
        sector,
        "global"
    ))
    
    return results


# ── 6. GEOPOLITICAL — GDELT DOC 2.0 ──────────────────────────────────────────

GDELT_QUERIES = [
    {
        "key":     "supply_shock",
        "label":   "Supply Shock News Volume",
        "query":   '"supply chain" (disruption OR shock OR shortage) (energy OR food OR fuel)',
        "context": "Monitors global news volume mentioning supply chain disruption across energy and food sectors.",
    },
    {
        "key":     "chokepoint_news",
        "label":   "Maritime Chokepoint News Volume",
        "query":   '(Hormuz OR "Bab-el-Mandeb" OR "Suez Canal" OR "Strait of Malacca") (closure OR attack OR disruption OR blockade)',
        "context": "Monitors news coverage of maritime chokepoint disruption events.",
    },
    {
        "key":     "energy_crisis",
        "label":   "Energy Crisis News Volume",
        "query":   '("energy crisis" OR "fuel shortage" OR "LNG shortage" OR "gas shortage") developing',
        "context": "Monitors news coverage of energy crises specifically affecting developing nations.",
    },
    {
        "key":     "food_crisis",
        "label":   "Food Crisis News Volume",
        "query":   '("food crisis" OR "food shortage" OR "famine" OR "food insecurity") (Africa OR Asia OR "Latin America" OR Bangladesh OR Kenya)',
        "context": "Monitors news coverage of food crises in target pilot regions.",
    },
]

def fetch_gdelt_signals():
    """
    GDELT DOC 2.0 API — free, no auth required.
    Returns news volume (% of global coverage) for supply chain keywords.
    Updated every 15 minutes. Uses 24h timespan.
    """
    results = []
    sector  = "geopolitical"
    
    for q in GDELT_QUERIES:
        try:
            url = (
                "https://api.gdeltproject.org/api/v2/doc/doc"
                f"?query={requests.utils.quote(q['query'])}"
                "&mode=timelinevol&timespan=7d&timezoom=yes&TIMELINESMOOTH=3&format=json"
            )
            resp = SESSION.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            
            # Extract the most recent volume percentage
            timeline = data.get("timeline", [])
            if timeline and timeline[0].get("data"):
                points  = timeline[0]["data"]
                latest  = points[-1]["value"] if points else 0.0
                avg_7d  = sum(p["value"] for p in points) / len(points) if points else 0.0
                
                status = _threshold_gdelt(latest)
                trend  = "↑ rising" if len(points) > 1 and points[-1]["value"] > points[-2]["value"] else "→ stable"
                
                notes = (
                    f"{q['context']} "
                    f"Current: {latest:.4f}% of global coverage. "
                    f"7-day avg: {avg_7d:.4f}%. Trend: {trend}."
                )
                results.append(_ok(q["label"], round(latest, 5), "% global coverage", "GDELT DOC 2.0", status, notes, sector))
            else:
                results.append(_ok(q["label"], 0.0, "% global coverage", "GDELT DOC 2.0", "ok",
                                   f"No data returned for query. {q['context']}", sector))
        
        except Exception as exc:
            results.append(_err(q["label"], str(exc), "GDELT DOC 2.0", sector))
    
    return results


# ── 7. WASH — WHO/UNICEF JMP ─────────────────────────────────────────────────

def fetch_wash_indicators():
    """
    WHO/UNICEF Joint Monitoring Programme (JMP) for Water Supply, Sanitation and Hygiene.
    Annual data — this is a manual-update indicator seeded with 2023 values.
    Source: https://washdata.org/data
    """
    # Static seed values from JMP 2023 report — update annually
    wash_data = [
        {
            "indicator": "Population without safely managed drinking water",
            "value":     2200,
            "unit":      "million people",
            "notes":     "2.2B people lack safely managed drinking water (JMP 2023). Of these, 703M have no basic water service. Concentrated in Sub-Saharan Africa and South Asia. Update from washdata.org annually.",
            "region":    "global",
        },
        {
            "indicator": "Population without basic sanitation",
            "value":     3500,
            "unit":      "million people",
            "notes":     "3.5B people lack safely managed sanitation. Highest burden in South Asia (1.5B) and Sub-Saharan Africa (1.0B). Update from washdata.org annually.",
            "region":    "global",
        },
        {
            "indicator": "Population practicing open defecation",
            "value":     419,
            "unit":      "million people",
            "notes":     "419M people still practicing open defecation globally (JMP 2023). Down from 892M in 2000 but progress slowing. Primary contamination driver for groundwater.",
            "region":    "global",
        },
    ]
    
    results = []
    for item in wash_data:
        results.append(_manual(
            item["indicator"],
            item["value"],
            item["unit"],
            "WHO/UNICEF JMP 2023",
            item["notes"],
            "wash",
            item["region"],
        ))
    return results


# ── 8. CURRENCY STRESS — ODA currencies vs USD ───────────────────────────────

# Target currencies: ODA-eligible nations in pilot regions
TARGET_CURRENCIES = {
    "BDT": ("Bangladeshi Taka",     "South Asia",     130.0, 145.0),   # (watch_level, alert_level per USD)
    "KES": ("Kenyan Shilling",      "East Africa",    130.0, 155.0),
    "ETB": ("Ethiopian Birr",       "East Africa",    55.0,  70.0),
    "BOB": ("Bolivian Boliviano",   "Andean LatAm",   7.0,   8.0),
    "PEN": ("Peruvian Sol",         "Andean LatAm",   3.8,   4.3),
    "MXN": ("Mexican Peso",         "Latin America",  18.0,  22.0),
    "CLP": ("Chilean Peso",         "Latin America",  950.0, 1050.0),
    "NPR": ("Nepalese Rupee",       "South Asia",     135.0, 150.0),
}

def fetch_currency_stress():
    """
    ODA currency stress vs USD — using Open Exchange Rates free tier
    (500 requests/month — well within GitHub Actions schedule).
    Falls back to Stooq individual pairs if OER key not set.
    """
    results = []
    sector  = "currency"
    
    rates = {}
    
    if OER_APP_ID:
        try:
            url  = f"https://openexchangerates.org/api/latest.json?app_id={OER_APP_ID}&base=USD"
            resp = SESSION.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data  = resp.json()
            rates = data.get("rates", {})
        except Exception as exc:
            pass  # Fall through to Stooq
    
    for code, (name, region, watch_level, alert_level) in TARGET_CURRENCIES.items():
        rate = rates.get(code)
        
        if rate is None:
            # Try Stooq for individual pair
            try:
                pair = f"{code.lower()}usd"
                url  = f"https://stooq.com/q/d/l/?s={pair}&i=d"
                resp = SESSION.get(url, timeout=TIMEOUT)
                resp.raise_for_status()
                lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
                if lines:
                    last = lines[-1].split(",")
                    # Stooq gives currency/USD — invert to get units per USD
                    raw  = float(last[4])
                    rate = 1.0 / raw if raw > 0 else None
            except Exception:
                pass
        
        if rate is None:
            results.append(_manual(
                f"{code} / USD ({name})",
                None, f"{code} per USD", "Open Exchange Rates / Stooq",
                f"Could not fetch {code}. Set OPENEXCHANGERATES_APP_ID for reliable currency data.",
                sector, region
            ))
            continue
        
        if rate >= alert_level:
            status = "alert"
            notes  = (
                f"{name} at {rate:.2f} per USD — significantly depreciated. "
                f"Import costs (fuel, food) elevated for {region} households. "
                f"Alert threshold: {alert_level:.2f}."
            )
        elif rate >= watch_level:
            status = "watch"
            notes  = (
                f"{name} at {rate:.2f} per USD — currency under pressure. "
                f"Import cost pass-through risk for {region}. "
                f"Watch threshold: {watch_level:.2f}."
            )
        else:
            status = "ok"
            notes  = f"{name} at {rate:.2f} per USD — within normal range."
        
        results.append(_ok(f"{code} / USD ({name})", round(rate, 4), f"{code} per USD",
                           "Open Exchange Rates / Stooq", status, notes, sector, region))
    
    return results


# ── SECTOR MAP ────────────────────────────────────────────────────────────────

SECTORS = {
    "energy":      [fetch_brent_crude, fetch_lng_spot],
    "chokepoint":  [fetch_chokepoint_status],
    "food":        [fetch_fao_food_price_index],
    "freight":     [fetch_freight_rates],
    "water":       [fetch_water_stress],
    "geopolitical":[fetch_gdelt_signals],
    "wash":        [fetch_wash_indicators],
    "currency":    [fetch_currency_stress],
}

# ── RUNNER ────────────────────────────────────────────────────────────────────

def run_all(sector_filter=None):
    all_results = []
    total_ok    = 0
    total_err   = 0

    sectors_to_run = (
        {sector_filter: SECTORS[sector_filter]}
        if sector_filter and sector_filter in SECTORS
        else SECTORS
    )

    for sector_name, fetchers in sectors_to_run.items():
        print(f"\n{'='*60}")
        print(f"  SECTOR: {sector_name.upper()}")
        print(f"{'='*60}")
        
        for fetcher in fetchers:
            print(f"  → {fetcher.__name__}...")
            try:
                result = fetcher()
                # Normalise — some fetchers return list, some return single item
                items = result if isinstance(result, list) else [result]
                for item in items:
                    icon = "✅" if item["status"] == "ok" else \
                           "⚠️ " if item["status"] in ("watch", "manual") else \
                           "🚨" if item["status"] == "alert" else "❌"
                    val_str = f"{item['value']} {item['unit']}" if item['value'] is not None else "N/A"
                    print(f"     {icon} {item['indicator']}: {val_str}")
                    if item["status"] in ("error",):
                        print(f"        ↳ {item['notes'][:120]}")
                        total_err += 1
                    else:
                        total_ok += 1
                all_results.extend(items)
            except Exception as exc:
                print(f"     💥 EXCEPTION in {fetcher.__name__}: {exc}")
                total_err += 1

    print(f"\n{'='*60}")
    print(f"  COMPLETE — {total_ok} OK/watch, {total_err} errors")
    print(f"{'='*60}\n")
    return all_results


def build_output(results):
    """Assemble the final JSON payload."""
    now = datetime.now(timezone.utc)
    
    # Overall platform status
    statuses = [r["status"] for r in results]
    if "alert" in statuses:
        platform_status = "alert"
    elif "watch" in statuses:
        platform_status = "watch"
    else:
        platform_status = "ok"
    
    # Count by status
    counts = {
        "ok":      statuses.count("ok"),
        "watch":   statuses.count("watch"),
        "alert":   statuses.count("alert"),
        "error":   statuses.count("error"),
        "manual":  statuses.count("manual"),
    }
    
    # Alert items for summary
    alerts = [r for r in results if r["status"] == "alert"]
    
    return {
        "meta": {
            "platform":        "Community Supply Chain Intelligence (CSI)",
            "version":         "1.0",
            "generated_utc":   now.isoformat(),
            "platform_status": platform_status,
            "counts":          counts,
            "alert_summary":   [{"indicator": a["indicator"], "notes": a["notes"][:200]} for a in alerts],
        },
        "indicators": results,
    }


def save_output(payload, dry_run=False):
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    
    files = [
        Path(f"csi_data_{date_str}.json"),
        Path("csi_data_latest.json"),
    ]
    
    json_str = json.dumps(payload, indent=2, ensure_ascii=False)
    
    if dry_run:
        print("\n[DRY RUN] Would write:")
        for f in files:
            print(f"  {f}")
        print(json_str[:500] + "...")
        return
    
    for f in files:
        f.write_text(json_str, encoding="utf-8")
        print(f"  ✓ Written: {f}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CSI Global Scraper v1.0")
    parser.add_argument("--sector",  help=f"Run only one sector: {list(SECTORS.keys())}")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write files")
    args = parser.parse_args()
    
    if args.sector and args.sector not in SECTORS:
        print(f"Unknown sector '{args.sector}'. Valid: {list(SECTORS.keys())}")
        sys.exit(1)
    
    results = run_all(sector_filter=args.sector)
    payload = build_output(results)
    save_output(payload, dry_run=args.dry_run)
    
    # Exit with error code if any alerts fired (useful for GitHub Actions notifications)
    alert_count = payload["meta"]["counts"]["alert"]
    if alert_count > 0:
        print(f"\n⚠️  {alert_count} ALERT(s) active — review csi_data_latest.json")
        sys.exit(2)  # Non-zero but not 1 (reserved for errors) — GHA can check this


if __name__ == "__main__":
    main()
