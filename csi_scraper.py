"""
Community Supply Chain Intelligence (CSI) — Global Scraper v1.0
================================================================
Watches upstream supply chain signals across 8 categories and
produces a JSON file consumed by the global dashboard.

SIGNALS
-------
  1. Energy      — Brent crude (EIA spot RBRTE), LNG spot (EIA/Stooq fallback)
  2. Chokepoints — Vessel counts via AISstream.io WebSocket; GDELT news proxy fallback
  3. Food        — FAO FFPI scraped from FAO world food situation page
  4. Freight     — Baltic Dry Index (Baltic Exchange) + Freightos FBX (scraped)
  5. Water       — GloFAS river anomaly signal (Copernicus/EU)
  6. Geopolitical— GDELT DOC 2.0 API (supply shock keyword volume)
  7. WASH        — WHO/UNICEF JMP improved water access (static annual, manual update)
  8. Health      — WHO Disease Outbreak News RSS + manual active outbreak indicators
  9. Currency    — ODA currency stress vs USD (Open Exchange Rates free tier)

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
    """
    Brent spot price — tries sources in order:
    1. OilPriceAPI free tier (reliable, no key, timestamped)
    2. EIA free API (spot series RBRTE)
    3. EIA weekly petroleum report page scrape
    """
    indicator = "Brent Crude Price"
    sector    = "energy"

    # Source 1 — OilPriceAPI free tier (no key, 1 req/min limit, returns WTI not Brent)
    # but gives a reliable current price with timestamp
    try:
        url  = "https://api.oilpriceapi.com/prices"
        resp = SESSION.get(url, timeout=15)
        if resp.ok:
            data  = resp.json()
            price = float(data.get("data", {}).get("price", 0))
            if 40 < price < 250:
                # OilPriceAPI returns WTI; add ~3-4 USD for Brent premium
                brent_est = round(price + 3.5, 2)
                status    = _threshold_energy(brent_est)
                notes     = (
                    f"Brent crude estimated at ${brent_est:.2f}/bbl (WTI ${price:.2f} + ~$3.5 Brent premium). "
                    + ("Normal range." if status == "ok" else
                       "Elevated — energy cost pressure on import-dependent communities." if status == "watch" else
                       "CRITICAL — supply shock threshold. LPG/fuel price spikes likely in 4–6 weeks.")
                )
                return _ok(indicator, brent_est, "USD/bbl", "OilPriceAPI (WTI+premium)", status, notes, sector)
    except Exception:
        pass

    # Source 2 — EIA free API, Brent spot price (RBRTE), no key required
    try:
        url = (
            "https://api.eia.gov/v2/petroleum/pri/spt/data/"
            "?frequency=daily&data[0]=value&facets[series][]=RBRTE"
            "&sort[0][column]=period&sort[0][direction]=desc&length=1"
            + (f"&api_key={EIA_KEY}" if EIA_KEY else "")
        )
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("response", {}).get("data", [])
        if rows:
            price  = float(rows[0]["value"])
            period = rows[0].get("period", "")
            status = _threshold_energy(price)
            notes  = (
                f"Brent spot at ${price:.2f}/bbl ({period}). "
                + ("Normal range." if status == "ok" else
                   "Elevated — household energy costs rising globally." if status == "watch" else
                   "CRITICAL — supply shock threshold. LPG/fuel price spikes likely in import-dependent communities.")
            )
            return _ok(indicator, price, "USD/bbl", "EIA", status, notes, sector)
    except Exception:
        pass

    # Source 3 — EIA weekly petroleum report page scrape
    try:
        url  = "https://www.eia.gov/petroleum/weekly/"
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        m    = re.search(r'Brent[^\d]*\$?\s*([\d]+\.[\d]+)', text, re.IGNORECASE)
        if m:
            price = float(m.group(1))
            if 40 < price < 200:
                status = _threshold_energy(price)
                notes  = f"Brent at ${price:.2f}/bbl (EIA weekly report)."
                return _ok(indicator, price, "USD/bbl", "EIA Weekly", status, notes, sector)
    except Exception:
        pass

    return _err(indicator, "All Brent sources failed — OilPriceAPI, EIA, EIA weekly", "Multiple", sector)


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


# AIS ship type code → human-readable category
# Codes 80-89 = tankers, 70-79 = cargo, 60-69 = passenger, 30-39 = fishing/service
def _ais_vessel_category(ship_type):
    if ship_type is None:
        return "unknown"
    t = int(ship_type)
    if 80 <= t <= 89: return "tanker"
    if 70 <= t <= 79: return "cargo"
    if 60 <= t <= 69: return "passenger"
    if 30 <= t <= 39: return "fishing/service"
    if 50 <= t <= 59: return "special"
    if 40 <= t <= 49: return "high-speed"
    return "other"


def _aisstream_vessel_data(api_key, lat_min, lat_max, lon_min, lon_max, listen_seconds=25):
    """
    Opens a WebSocket to AISstream.io, subscribes to a bounding box,
    collects PositionReport (count/type) AND StaticDataReport (vessel name,
    destination, ship type) messages for listen_seconds.
    Returns (vessel_dict, error) where vessel_dict maps MMSI ->
    {category, name, destination, nav_status, speed}.
    """
    import threading
    try:
        import websocket
    except ImportError:
        return None, "websocket-client not installed"

    vessels = {}  # MMSI -> dict of known fields
    error   = [None]
    done    = threading.Event()

    def on_open(ws):
        subscription = {
            "APIkey": api_key,
            "BoundingBoxes": [[[lat_min, lon_min], [lat_max, lon_max]]],
            "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport",
                                   "ExtendedClassBPositionReport", "StaticAndVoyageRelatedData"],
        }
        ws.send(json.dumps(subscription))

    def on_message(ws, message):
        try:
            data  = json.loads(message)
            mtype = data.get("MessageType", "")
            meta  = data.get("MetaData", {})
            mmsi  = meta.get("MMSI") or meta.get("MMSI_String")
            if not mmsi:
                return
            mmsi = str(mmsi)
            if mmsi not in vessels:
                vessels[mmsi] = {"category": "unknown", "name": "", "destination": "", "nav_status": None, "speed": None}

            msg = data.get("Message", {})

            # Position reports — navigation status and speed
            for key in ["PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"]:
                pr = msg.get(key)
                if pr:
                    vessels[mmsi]["nav_status"] = pr.get("NavigationalStatus")
                    vessels[mmsi]["speed"]       = pr.get("Sog")
                    break

            # Static data — vessel name, destination, ship type
            svd = msg.get("StaticAndVoyageRelatedData")
            if svd:
                if svd.get("Name"):
                    vessels[mmsi]["name"]        = svd["Name"].strip()
                if svd.get("Destination"):
                    vessels[mmsi]["destination"] = svd["Destination"].strip()
                if svd.get("TypeOfShipAndCargo") is not None:
                    vessels[mmsi]["category"]    = _ais_vessel_category(svd["TypeOfShipAndCargo"])

            # ShipName from metadata (available in PositionReport metadata)
            if meta.get("ShipName") and not vessels[mmsi]["name"]:
                vessels[mmsi]["name"] = meta["ShipName"].strip()

        except Exception:
            pass

    def on_error(ws, err):
        error[0] = str(err)
        done.set()

    def on_close(ws, *args):
        done.set()

    ws = websocket.WebSocketApp(
        "wss://stream.aisstream.io/v0/stream",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 30}, daemon=True)
    t.start()
    done.wait(timeout=listen_seconds + 5)
    ws.close()

    if error[0] and not vessels:
        return None, error[0]
    return vessels, None


def fetch_chokepoint_status():
    """
    Vessel counts + type breakdown per chokepoint via AISstream.io WebSocket.
    Reports tanker/cargo/passenger breakdown, anchored count, and destinations.
    Falls back to GDELT news proxy if AISstream key not set or fails.
    """
    import time
    results = []

    # Baseline vessel counts by type per chokepoint (approximate 30-day normals)
    BASELINES = {
        "Strait of Hormuz":  {"total": 80,  "tanker": 18, "cargo": 25},
        "Bab-el-Mandeb":     {"total": 60,  "tanker": 12, "cargo": 22},
        "Suez Canal":        {"total": 45,  "tanker": 10, "cargo": 20},
        "Strait of Malacca": {"total": 120, "tanker": 20, "cargo": 55},
    }

    for name, (lat_min, lat_max, lon_min, lon_max) in CHOKEPOINTS.items():
        indicator = f"Chokepoint — {name}"
        sector    = "chokepoint"

        # ── PRIMARY: AISstream WebSocket with type breakdown ──────────────────
        if AISSTREAM_KEY:
            vessels, err = _aisstream_vessel_data(
                AISSTREAM_KEY, lat_min, lat_max, lon_min, lon_max,
                listen_seconds=25
            )
            if vessels is not None:
                count    = len(vessels)
                baseline = BASELINES.get(name, {"total": 50, "tanker": 10, "cargo": 20})

                # Count by category
                cats = {}
                for v in vessels.values():
                    c = v.get("category", "unknown")
                    cats[c] = cats.get(c, 0) + 1

                tankers   = cats.get("tanker", 0)
                cargo_cnt = cats.get("cargo", 0)
                passenger = cats.get("passenger", 0)
                other_cnt = count - tankers - cargo_cnt - passenger

                # Anchored vessels (nav_status 1 = at anchor)
                anchored = sum(1 for v in vessels.values() if v.get("nav_status") == 1)

                # Notable destinations (non-empty, first 3)
                destinations = list({
                    v["destination"] for v in vessels.values()
                    if v.get("destination") and v["destination"] not in ("", "NONE", "XXXX", ".")
                })[:3]

                ratio        = count / baseline["total"] if baseline["total"] else 1.0
                tanker_ratio = tankers / baseline["tanker"] if baseline.get("tanker") else 1.0

                if tanker_ratio < 0.3 or ratio < 0.3:
                    status   = "alert"
                    severity = "CRITICAL"
                elif tanker_ratio < 0.6 or ratio < 0.6:
                    status   = "watch"
                    severity = "REDUCED"
                else:
                    status   = "ok"
                    severity = "NORMAL"

                breakdown  = f"{tankers} tankers, {cargo_cnt} cargo, {passenger} passenger, {other_cnt} other"
                dest_str   = f" Seen destinations: {', '.join(destinations)}." if destinations else ""
                anchor_str = f" {anchored} vessels at anchor." if anchored > 0 else ""

                notes = (
                    f"{severity}: {count} vessels in {name} (25s window, baseline ~{baseline['total']}). "
                    f"Breakdown: {breakdown}. "
                    f"Tanker count {tankers} vs baseline ~{baseline.get('tanker', '?')}."
                    f"{anchor_str}{dest_str}"
                )
                results.append(_ok(indicator, count, "vessels (25s window)",
                                   "AISstream.io", status, notes, sector))
                continue
            # else fall through to GDELT

        # ── FALLBACK: GDELT news proxy ────────────────────────────────────────
        time.sleep(4)
        try:
            short_name = (name
                .replace("Strait of ", "")
                .replace("Bab-el-", "Mandeb ")
                .split()[0])
            keyword = f'{short_name} shipping'
            url = (
                f"https://api.gdeltproject.org/api/v2/doc/doc"
                f"?query={requests.utils.quote(keyword)}"
                f"&mode=artlist&maxrecords=5&format=json&timespan=3d"
            )
            resp  = SESSION.get(url, timeout=12)
            data  = resp.json() if resp.ok else {}
            count = len(data.get("articles", []))

            if count == 0:
                status = "ok"
                notes  = (f"No significant disruption news for {name} in past 3 days. "
                          f"Set AISSTREAM_API_KEY for live vessel counts and type breakdown.")
            elif count < 3:
                status = "watch"
                notes  = f"{count} shipping news items in past 3 days for {name}. Monitor."
            else:
                status = "alert"
                notes  = (f"ELEVATED: {count} shipping news items for {name} in past 3 days. "
                          f"Verify with vessel tracking.")

            results.append(_ok(indicator, count, "news items (3d)",
                               "GDELT proxy", status, notes, sector))

        except Exception as exc:
            results.append(_err(indicator, str(exc), "GDELT proxy", sector))
    
    return results


# ── 3. FOOD — FAO Food Price Index ────────────────────────────────────────────

def fetch_fao_food_price_index():
    """
    FAO Food Price Index (FFPI) + 5 sub-indices from FAOSTAT.
    FAO GIEWS country-level retail food prices for pilot communities.
    FAO fertilizer price index (urea, DAP) — 4-6 month leading food indicator.
    """
    results  = []
    sector   = "food"
    source   = "FAO / FAOSTAT"

    # ── FFPI headline (scraped from FAO world food situation page) ────────────
    try:
        page_url = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
        resp     = SESSION.get(page_url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup     = BeautifulSoup(resp.text, "html.parser")
        text     = soup.get_text(" ", strip=True)
        ffpi_val = None
        month_str= ""

        m = re.search(
            r"averaged\s+([\d,]+\.?\d*)\s+points\s+in\s+(\w+\s+\d{4})",
            text, re.IGNORECASE
        )
        if m:
            ffpi_val  = float(m.group(1).replace(",", ""))
            month_str = m.group(2)

        if ffpi_val:
            status = _threshold_food(ffpi_val)
            notes  = (
                f"FAO FFPI: {ffpi_val} points ({month_str}). Base period 2014–2016 = 100. "
                + ("Normal food price environment." if status == "ok" else
                   "Food prices elevated — household food budgets under pressure in import-dependent communities." if status == "watch" else
                   "CRITICAL food price environment — acute food security risk in low-income import-dependent communities.")
            )
            results.append(_ok("FAO Food Price Index (FFPI)", ffpi_val, "index (2014-16=100)",
                               source, status, notes, sector))
        else:
            results.append(_err("FAO Food Price Index (FFPI)", "Could not parse FFPI from FAO page", source, sector))
    except Exception as exc:
        results.append(_err("FAO Food Price Index (FFPI)", str(exc), source, sector))

    # ── FAO GIEWS retail food prices — pilot countries ────────────────────────
    # GIEWS FPMA tool: country-level retail staple food prices (monthly)
    # Pilot countries: Ethiopia (238), Nepal (175), Bolivia (25)
    GIEWS_COUNTRIES = [
        {"code": "ETH", "name": "Ethiopia",  "region": "East Africa",   "staple": "Maize",  "currency": "ETB"},
        {"code": "NPL", "name": "Nepal",     "region": "South Asia",    "staple": "Rice",   "currency": "NPR"},
        {"code": "BOL", "name": "Bolivia",   "region": "Andean LatAm",  "staple": "Maize",  "currency": "BOB"},
    ]

    for country in GIEWS_COUNTRIES:
        try:
            # GIEWS FPMA API — monthly retail prices
            url = (
                "https://fpma.fao.org/giews/fpmat4/dashboard/monitor/"
                f"FPMAMonitor?country={country['code']}&commodity=Maize&currency=USD"
            )
            resp = SESSION.get(url, timeout=15)
            if resp.ok:
                data  = resp.json()
                # Look for most recent monthly price
                prices = data.get("data", []) if isinstance(data, dict) else data
                if prices and isinstance(prices, list):
                    latest = prices[-1]
                    price  = latest.get("price") or latest.get("value")
                    period = latest.get("date") or latest.get("period", "")
                    if price:
                        results.append(_ok(
                            f"{country['name']} — Retail {country['staple']} Price",
                            round(float(price), 2),
                            "USD/100kg",
                            "FAO GIEWS FPMA",
                            "ok",
                            f"Retail {country['staple']} price in {country['name']}: ${price:.2f}/100kg ({period}). "
                            f"Source: FAO GIEWS Food Price Monitoring and Analysis tool. "
                            f"Direct household-level food cost indicator for {country['region']} pilot community.",
                            sector,
                            country["region"]
                        ))
                        continue
        except Exception:
            pass

        # Fallback — manual seed with last-known values if API fails
        seed = {
            "Ethiopia": (45.0, "~$45/100kg maize — 2025 estimate. Update from fpma.fao.org."),
            "Nepal":    (38.0, "~$38/100kg rice — 2025 estimate. Update from fpma.fao.org."),
            "Bolivia":  (32.0, "~$32/100kg maize — 2025 estimate. Update from fpma.fao.org."),
        }
        val, note = seed.get(country["name"], (None, ""))
        results.append(_manual(
            f"{country['name']} — Retail {country['staple']} Price",
            val, "USD/100kg", "FAO GIEWS FPMA (manual seed)",
            note, sector, country["region"]
        ))

    # ── FAO fertilizer price index — 4-6 month food price leading indicator ───
    try:
        url  = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
        resp = SESSION.get(url, timeout=15)
        if resp.ok:
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(" ", strip=True)
            # Look for urea or DAP price mention
            m = re.search(r'[Uu]rea[^\d]*\$?\s*([\d,]+\.?\d*)\s*/?\s*(tonne|MT|t)', text)
            if m:
                urea_price = float(m.group(1).replace(",",""))
                if 100 < urea_price < 2000:
                    status = "alert" if urea_price > 600 else "watch" if urea_price > 400 else "ok"
                    results.append(_ok(
                        "Urea Fertilizer Price (Leading Food Indicator)",
                        urea_price, "USD/tonne", "FAO",
                        status,
                        f"Urea at ${urea_price:.0f}/tonne. "
                        f"Urea prices lead food prices by 4–6 months (affects next planting season costs). "
                        f"{'ELEVATED — food price inflation likely in coming months.' if status != 'ok' else 'Normal range.'}",
                        sector, "global"
                    ))
    except Exception:
        pass

    # Fallback manual fertilizer seed
    if not any(r["indicator"] == "Urea Fertilizer Price (Leading Food Indicator)" for r in results):
        results.append(_manual(
            "Urea Fertilizer Price (Leading Food Indicator)",
            None, "USD/tonne", "FAO / World Bank",
            "Manual update required. Check fao.org/worldfoodsituation or World Bank commodity prices. "
            "Urea leads food prices by 4–6 months — critical leading indicator for pilot community food costs.",
            sector, "global"
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
    
    # Baltic Dry Index — FRED (St. Louis Fed) hosts the BDI series free, no key needed
    bdi = None

    # Source 1 — FRED API, series DBDI (Daily Baltic Dry Index), no key required
    try:
        url  = ("https://fred.stlouisfed.org/graph/fredgraph.csv"
                "?id=DBDI&vintage_date=" +
                datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("DATE")]
        if lines:
            last = lines[-1].split(",")
            val  = last[1].strip()
            if val and val != ".":
                bdi = float(val)
    except Exception:
        pass

    # Source 2 — Stooq BDI tickers as fallback
    if bdi is None:
        for ticker in ["bdi", "^bdi"]:
            try:
                url  = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
                resp = SESSION.get(url, timeout=12)
                resp.raise_for_status()
                lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
                if lines and len(lines[-1].split(",")) >= 5:
                    val = float(lines[-1].split(",")[4])
                    if 200 < val < 20000:
                        bdi = val
                        break
            except Exception:
                continue
    if bdi is not None:
        if bdi < 1500:
            status = "ok"
            notes  = f"Baltic Dry Index: {bdi:.0f}. Low freight rates — shipping capacity available."
        elif bdi < 2500:
            status = "watch"
            notes  = f"Baltic Dry Index: {bdi:.0f}. Elevated freight rates — supply chain cost pressure building."
        else:
            status = "alert"
            notes  = f"ELEVATED: Baltic Dry Index: {bdi:.0f}. High freight rates — significant supply chain cost pressure."
        results.append(_ok("Baltic Dry Index (BDI)", bdi, "index", "Baltic Exchange", status, notes, sector))
    else:
        results.append(_err("Baltic Dry Index (BDI)", "BDI unavailable — Baltic Exchange and Stooq both failed", "Baltic Exchange / Stooq", sector))
    
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
#
# Using GDELT theme tags instead of keyword queries — pre-classified by GDELT,
# faster, and dramatically less prone to 429 rate limiting.
# Theme reference: https://api.gdeltproject.org/api/v2/doc/doc?query=theme:TAX_FAMINE&mode=artlist

GDELT_THEMES = [
    {
        "key":     "supply_shock",
        "label":   "Supply Shock News Volume",
        "theme":   "WB_639_CONFLICT_PREVENTION",
        "fallback_query": "supply chain disruption energy food",
        "context": "Monitors global news volume on supply chain disruption events.",
    },
    {
        "key":     "energy_crisis",
        "label":   "Energy Crisis News Volume",
        "theme":   "WB_673_ENERGY",
        "fallback_query": "energy crisis fuel shortage developing",
        "context": "Monitors global energy crisis and fuel shortage coverage.",
    },
    {
        "key":     "food_crisis",
        "label":   "Food Crisis News Volume",
        "theme":   "TAX_FAMINE",
        "fallback_query": "food crisis famine food insecurity Africa Asia",
        "context": "Monitors famine and food insecurity coverage in target regions.",
    },
    {
        "key":     "health_emergency",
        "label":   "Health Emergency News Volume",
        "theme":   "HEALTH_PANDEMIC",
        "fallback_query": "disease outbreak health emergency developing countries",
        "context": "Monitors pandemic and health emergency news coverage.",
    },
]

def fetch_gdelt_signals():
    """
    GDELT DOC 2.0 API — free, no auth required.
    Uses theme tag queries (timelinevol mode) — pre-classified by GDELT,
    faster and less rate-limit prone than complex keyword queries.
    Falls back to simplified keyword query if theme returns no data.
    12-second sleep between requests to respect rate limits.
    """
    import time
    results = []
    sector  = "geopolitical"

    for i, q in enumerate(GDELT_THEMES):
        if i > 0:
            time.sleep(12)
        try:
            # Primary: theme tag query (fastest, most reliable)
            url = (
                "https://api.gdeltproject.org/api/v2/doc/doc"
                f"?query=theme%3A{q['theme']}"
                "&mode=timelinevol&timespan=7d&timezoom=yes&TIMELINESMOOTH=3&format=json"
            )
            resp = SESSION.get(url, timeout=20)

            # If theme query rate-limited, try fallback keyword
            if resp.status_code == 429:
                time.sleep(20)
                url = (
                    "https://api.gdeltproject.org/api/v2/doc/doc"
                    f"?query={requests.utils.quote(q['fallback_query'])}"
                    "&mode=timelinevol&timespan=7d&timezoom=yes&TIMELINESMOOTH=3&format=json"
                )
                resp = SESSION.get(url, timeout=20)

            resp.raise_for_status()
            data = resp.json()

            timeline = data.get("timeline", [])
            if timeline and timeline[0].get("data"):
                points = timeline[0]["data"]
                latest = points[-1]["value"] if points else 0.0
                avg_7d = sum(p["value"] for p in points) / len(points) if points else 0.0
                status = _threshold_gdelt(latest)
                trend  = "↑ rising" if len(points) > 1 and points[-1]["value"] > points[-2]["value"] else "→ stable"
                notes  = (
                    f"{q['context']} Theme: {q['theme']}. "
                    f"Current: {latest:.4f}% of global coverage. "
                    f"7-day avg: {avg_7d:.4f}%. Trend: {trend}."
                )
                results.append(_ok(q["label"], round(latest, 5), "% global coverage",
                                   "GDELT DOC 2.0", status, notes, sector))
            else:
                results.append(_ok(q["label"], 0.0, "% global coverage", "GDELT DOC 2.0", "ok",
                                   f"No timeline data for theme {q['theme']}. {q['context']}", sector))

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


# ── 8. HEALTH — WHO Disease Outbreak News + manual active outbreaks ───────────

# Pilot region keywords — any DON mentioning these triggers a health alert
PILOT_REGION_KEYWORDS = [
    "bangladesh", "kenya", "ethiopia", "nepal", "bolivia", "peru",
    "mexico", "chile", "africa", "south asia", "latin america",
    "south atlantic", "cape verde", "saint helena", "argentina",
    "caribbean", "pacific island",
]

# Known high-consequence pathogens — always alert regardless of region
HIGH_CONSEQUENCE_PATHOGENS = [
    "andes virus", "hantavirus", "ebola", "marburg", "lassa",
    "nipah", "mers", "h5n1", "mpox", "cholera",
]

# ── MANUAL ACTIVE OUTBREAKS ──────────────────────────────────────────────────
# Update this list as outbreaks evolve. Format:
# (indicator, value, unit, status, region, source, notes)
ACTIVE_OUTBREAKS = [
    {
        "indicator": "MV Hondius — Andes Virus (Hantavirus) Outbreak",
        "value":     8,
        "unit":      "confirmed/suspected cases",
        "status":    "alert",
        "region":    "Global — 23 nationalities, 6+ countries",
        "source":    "WHO DON599 / CDC HAN00528",
        "notes": (
            "Andes virus (only human-to-human transmissible hantavirus). "
            "MV Hondius cruise ship departed Ushuaia, Argentina April 1 2026. "
            "8 cases (6 confirmed, 2 suspected), 3 deaths as of May 9 2026. "
            "Passengers from 23 nationalities dispersed via repatriation flights. "
            "Active cases in: South Africa, Netherlands, Germany, Spain, Switzerland, Canada. "
            "45-day monitoring window extends to ~June 15 2026. "
            "Cape Verde UNABLE to handle evacuation — direct illustration of "
            "health infrastructure gap in CSI pilot regions. "
            "38% HPS fatality rate. No antiviral treatment. "
            "MONITOR: WHO DON updates at who.int/emergencies/disease-outbreak-news. "
            "ESCALATE TO: community transmission confirmed outside ship contacts."
        ),
    },
]


def fetch_who_don():
    """
    WHO Disease Outbreak News (DON) RSS feed.
    Flags any outbreak mentioning CSI pilot regions or high-consequence pathogens.
    Feed updates within hours of WHO publishing a new DON.
    """
    results = []
    sector  = "health"
    source  = "WHO Disease Outbreak News"

    try:
        url  = "https://www.who.int/feeds/entity/csr/don/en/rss.xml"
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "xml" if "xml" in resp.headers.get("content-type","") else "html.parser")

        items = soup.find_all("item")
        if not items:
            # Try lxml-style parsing
            items = soup.find_all("item") or soup.select("item")

        pilot_hits    = []
        pathogen_hits = []
        recent_titles = []

        for item in items[:20]:  # Check most recent 20 DONs
            title = (item.find("title") or item.find("title")).get_text(strip=True) if item.find("title") else ""
            desc  = (item.find("description") or item.find("summary") or "")
            desc  = desc.get_text(strip=True) if hasattr(desc, "get_text") else str(desc)
            pub   = (item.find("pubDate") or item.find("published") or "")
            pub   = pub.get_text(strip=True) if hasattr(pub, "get_text") else ""
            combined = (title + " " + desc).lower()

            recent_titles.append(title[:80])

            # Check for pilot region mention
            for kw in PILOT_REGION_KEYWORDS:
                if kw in combined:
                    pilot_hits.append({"title": title, "pub": pub, "keyword": kw})
                    break

            # Check for high-consequence pathogen
            for pathogen in HIGH_CONSEQUENCE_PATHOGENS:
                if pathogen in combined:
                    pathogen_hits.append({"title": title, "pub": pub, "pathogen": pathogen})
                    break

        # Report pilot region hits
        if pathogen_hits:
            hit = pathogen_hits[0]
            results.append(_ok(
                "WHO DON — High-Consequence Pathogen Alert",
                len(pathogen_hits),
                "active DONs",
                source, "alert",
                f"HIGH CONSEQUENCE: {hit['pathogen'].title()} detected in recent WHO DONs. "
                f"Most recent: '{hit['title']}' ({hit['pub']}). "
                f"Total matching DONs (last 20): {len(pathogen_hits)}. "
                f"Review: who.int/emergencies/disease-outbreak-news",
                sector, "global"
            ))
        elif pilot_hits:
            hit = pilot_hits[0]
            results.append(_ok(
                "WHO DON — Pilot Region Health Alert",
                len(pilot_hits),
                "active DONs",
                source, "watch",
                f"WHO Disease Outbreak News mentions CSI pilot region keyword '{hit['keyword']}'. "
                f"Most recent: '{hit['title']}' ({hit['pub']}). "
                f"Total matching DONs (last 20): {len(pilot_hits)}. "
                f"Review: who.int/emergencies/disease-outbreak-news",
                sector, "global"
            ))
        else:
            recent = " | ".join(recent_titles[:3])
            results.append(_ok(
                "WHO DON — Pilot Region Health Signal",
                0,
                "active DONs in pilot regions",
                source, "ok",
                f"No recent WHO Disease Outbreak News for CSI pilot regions or "
                f"high-consequence pathogens. Recent DONs: {recent}",
                sector, "global"
            ))

    except Exception as exc:
        results.append(_err(
            "WHO DON — Disease Outbreak Monitor",
            str(exc), source, sector
        ))

    return results


def fetch_active_outbreaks():
    """
    Manual active outbreak indicators — updated by operator.
    Seeded with the MV Hondius Andes virus outbreak (May 2026).
    Update ACTIVE_OUTBREAKS list above as situations evolve.
    """
    results = []
    sector  = "health"

    for outbreak in ACTIVE_OUTBREAKS:
        results.append({
            "indicator": outbreak["indicator"],
            "value":     outbreak["value"],
            "unit":      outbreak["unit"],
            "source":    outbreak["source"],
            "status":    outbreak["status"],
            "notes":     outbreak["notes"],
            "sector":    sector,
            "region":    outbreak["region"],
            "ts":        datetime.now(timezone.utc).isoformat(),
        })

    if not ACTIVE_OUTBREAKS:
        results.append(_ok(
            "Active Outbreak Monitor",
            0, "active outbreaks tracked", "Manual / CSI operator",
            "ok", "No manually tracked outbreaks active.", sector
        ))

    return results


# ── 9. CURRENCY STRESS — ODA currencies vs USD ───────────────────────────────

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
    "health":      [fetch_who_don, fetch_active_outbreaks],
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
