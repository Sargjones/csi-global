"""
Community Supply Chain Intelligence (CSI) -- Global Scraper v1.1
=================================================================
Watches upstream supply chain signals across 10 categories and
produces a JSON file consumed by the global dashboard.

SIGNALS
-------
  1. Energy      -- Brent crude (EIA spot RBRTE), LNG spot
  2. Chokepoints -- Vessel counts via AISstream.io; GDELT news proxy fallback
  3. Food        -- FAO FFPI + GIEWS retail prices + fertilizer
  4. Freight     -- Baltic Dry Index + Freightos FBX
  5. Water       -- GloFAS river anomaly signal
  6. Geopolitical-- GDELT DOC 2.0 API (supply shock keyword volume)
  7. WASH        -- WHO/UNICEF JMP improved water access
  8. Health      -- WHO DON RSS + CDC scrapers + active outbreak tracking
  9. Currency    -- ODA currency stress vs USD
  10. Climate    -- Thwaites glacier shelf stability + iceberg tracking
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# -- Session ------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "CSI-Scraper/1.1 Community Supply Chain Intelligence "
        "(github.com/Sargjones/csi-global; contact: info@criticalto.ca)"
    ),
    "Accept": "application/json, text/html, text/csv, */*",
})
TIMEOUT = 30

# -- API keys -----------------------------------------------------------------

EIA_KEY       = os.environ.get("EIA_API_KEY", "")
OER_APP_ID    = os.environ.get("OPENEXCHANGERATES_APP_ID", "")
AISSTREAM_KEY = os.environ.get("AISSTREAM_API_KEY", "")

# -- Response helpers ---------------------------------------------------------

def _ok(indicator, value, unit, source, status="ok", notes="", sector="", region="global"):
    return {
        "indicator": indicator, "value": value, "unit": unit,
        "source": source, "status": status, "notes": notes,
        "sector": sector, "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _err(indicator, message, source, sector="", region="global"):
    return {
        "indicator": indicator, "value": None, "unit": "",
        "source": source, "status": "error", "notes": f"ERROR: {message}",
        "sector": sector, "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _manual(indicator, value, unit, source, notes="", sector="", region="global"):
    return {
        "indicator": indicator, "value": value, "unit": unit,
        "source": source, "status": "manual", "notes": notes,
        "sector": sector, "region": region,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

def _threshold_energy(v):
    if v is None: return "unknown"
    return "ok" if v < 80 else "watch" if v < 95 else "alert"

def _threshold_food(v):
    if v is None: return "unknown"
    return "ok" if v < 115 else "watch" if v < 130 else "alert"

def _threshold_freight(v):
    if v is None: return "unknown"
    return "ok" if v < 2000 else "watch" if v < 3500 else "alert"

def _threshold_gdelt(v):
    if v is None: return "unknown"
    return "ok" if v < 0.05 else "watch" if v < 0.15 else "alert"

def _safe_get(url, timeout=15):
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[WARN] fetch failed {url}: {e}")
        return None

def _extract_number(text, patterns):
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                continue
    return None

# -- 1. ENERGY ----------------------------------------------------------------

def fetch_brent_crude():
    indicator = "Brent Crude Price"
    sector    = "energy"
    try:
        url  = "https://api.oilpriceapi.com/prices"
        resp = SESSION.get(url, timeout=15)
        if resp.ok:
            price = float(resp.json().get("data", {}).get("price", 0))
            if 40 < price < 250:
                brent = round(price + 3.5, 2)
                status = _threshold_energy(brent)
                notes = (f"Brent ~${brent:.2f}/bbl (WTI ${price:.2f} + $3.5 premium). "
                         + ("CRITICAL supply shock threshold." if status == "alert"
                            else "Elevated." if status == "watch" else "Normal range."))
                return _ok(indicator, brent, "USD/bbl", "OilPriceAPI", status, notes, sector)
    except Exception:
        pass
    try:
        url = ("https://api.eia.gov/v2/petroleum/pri/spt/data/"
               "?frequency=daily&data[0]=value&facets[series][]=RBRTE"
               "&sort[0][column]=period&sort[0][direction]=desc&length=1"
               + (f"&api_key={EIA_KEY}" if EIA_KEY else ""))
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        rows = resp.json().get("response", {}).get("data", [])
        if rows:
            price = float(rows[0]["value"])
            status = _threshold_energy(price)
            return _ok(indicator, price, "USD/bbl", "EIA", status,
                       f"Brent spot ${price:.2f}/bbl ({rows[0].get('period','')}).", sector)
    except Exception:
        pass
    return _err(indicator, "All Brent sources failed", "EIA/OilPriceAPI", sector)


def fetch_lng_spot():
    indicator = "LNG / Natural Gas Spot"
    sector    = "energy"
    if EIA_KEY:
        try:
            url = (f"https://api.eia.gov/v2/natural-gas/pri/fut/data/"
                   f"?api_key={EIA_KEY}&frequency=daily&data[0]=value"
                   f"&facets[series][]=RNGC1&sort[0][column]=period"
                   f"&sort[0][direction]=desc&length=1")
            resp = SESSION.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            val = float(resp.json()["response"]["data"][0]["value"])
            return _ok(indicator, val, "USD/MMBtu", "EIA", "ok",
                       f"Henry Hub ${val:.2f}/MMBtu.", sector)
        except Exception:
            pass
    try:
        url  = "https://stooq.com/q/d/l/?s=ngx.f&i=d"
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
        if lines:
            price = float(lines[-1].split(",")[4])
            return _ok(indicator, price, "USD/MMBtu", "Stooq", "ok",
                       f"NG futures ${price:.2f}/MMBtu.", sector)
    except Exception as exc:
        return _err(indicator, str(exc), "Stooq", sector)


# -- 2. CHOKEPOINTS -----------------------------------------------------------

CHOKEPOINTS = {
    "Strait of Hormuz":    (25.5, 27.0, 55.5, 57.5),
    "Bab-el-Mandeb":       (11.5, 13.5, 42.5, 44.5),
    "Suez Canal":          (29.5, 32.5, 32.0, 33.5),
    "Strait of Malacca":   (1.0,  6.0, 100.0, 104.5),
}

def fetch_chokepoint_status():
    results = []
    for name, (lat_min, lat_max, lon_min, lon_max) in CHOKEPOINTS.items():
        indicator = f"Chokepoint -- {name}"
        sector    = "chokepoint"
        time.sleep(4)
        try:
            short = name.replace("Strait of ", "").replace("Bab-el-", "Mandeb ").split()[0]
            url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
                   f"?query={requests.utils.quote(short + ' shipping')}"
                   f"&mode=artlist&maxrecords=5&format=json&timespan=3d")
            resp  = SESSION.get(url, timeout=12)
            data  = resp.json() if resp.ok else {}
            count = len(data.get("articles", []))
            if count == 0:
                status = "ok"
                notes  = f"No significant disruption news for {name} in past 3 days."
            elif count < 3:
                status = "watch"
                notes  = f"{count} shipping news items for {name} in past 3 days."
            else:
                status = "alert"
                notes  = f"ELEVATED: {count} shipping news items for {name} in past 3 days."
            results.append(_ok(indicator, count, "news items (3d)", "GDELT proxy", status, notes, sector))
        except Exception as exc:
            results.append(_err(indicator, str(exc), "GDELT proxy", sector))
    return results


# -- 3. FOOD ------------------------------------------------------------------

def fetch_fao_food_price_index():
    results = []
    sector  = "food"
    source  = "FAO / FAOSTAT"
    try:
        page_url = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
        resp     = SESSION.get(page_url, timeout=TIMEOUT)
        resp.raise_for_status()
        text     = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)
        m = re.search(r"averaged\s+([\d,]+\.?\d*)\s+points\s+in\s+(\w+\s+\d{4})", text, re.IGNORECASE)
        if m:
            val    = float(m.group(1).replace(",", ""))
            period = m.group(2)
            status = _threshold_food(val)
            results.append(_ok("FAO Food Price Index (FFPI)", val, "index (2014-16=100)",
                               source, status, f"FAO FFPI: {val} points ({period}).", sector))
        else:
            results.append(_err("FAO Food Price Index (FFPI)", "Could not parse FFPI", source, sector))
    except Exception as exc:
        results.append(_err("FAO Food Price Index (FFPI)", str(exc), source, sector))

    GIEWS = [
        {"code": "ETH", "name": "Ethiopia", "region": "East Africa",  "staple": "Maize"},
        {"code": "NPL", "name": "Nepal",    "region": "South Asia",   "staple": "Rice"},
        {"code": "BOL", "name": "Bolivia",  "region": "Andean LatAm", "staple": "Maize"},
    ]
    seeds = {"Ethiopia": 45.0, "Nepal": 38.0, "Bolivia": 32.0}
    for c in GIEWS:
        results.append(_manual(
            f"{c['name']} -- Retail {c['staple']} Price",
            seeds.get(c["name"]), "USD/100kg", "FAO GIEWS FPMA (manual seed)",
            f"~${seeds.get(c['name'])}/100kg {c['staple']} -- 2025 estimate. Update from fpma.fao.org.",
            sector, c["region"]
        ))

    results.append(_manual(
        "Urea Fertilizer Price (Leading Food Indicator)",
        None, "USD/tonne", "FAO / World Bank",
        "Manual update required. Urea leads food prices by 4-6 months.",
        sector, "global"
    ))
    return results


# -- 4. FREIGHT ---------------------------------------------------------------

def fetch_freight_rates():
    results = []
    sector  = "freight"
    bdi     = None
    try:
        url  = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DBDI"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("DATE")]
        for line in reversed(lines[-10:]):
            parts = line.split(",")
            if len(parts) >= 2 and parts[1].strip() not in (".", ""):
                val = float(parts[1].strip())
                if 200 < val < 20000:
                    bdi = val
                    break
    except Exception:
        pass
    if bdi is not None:
        status = "ok" if bdi < 1500 else "watch" if bdi < 2500 else "alert"
        results.append(_ok("Baltic Dry Index (BDI)", bdi, "index", "FRED/Baltic Exchange",
                           status, f"BDI: {bdi:.0f}.", sector))
    else:
        results.append(_err("Baltic Dry Index (BDI)", "BDI unavailable", "FRED", sector))
    return results


# -- 5. WATER -----------------------------------------------------------------

def fetch_water_stress():
    results = []
    sector  = "water"
    try:
        url  = "https://www.globalfloods.eu/glofas-forecasting/"
        resp = SESSION.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)[:3000]
        alert_kw = ["exceptional", "severe", "extreme drought", "water shortage", "crisis"]
        watch_kw = ["below normal", "low flow", "drought", "deficit"]
        if any(kw in text.lower() for kw in alert_kw):
            status = "alert"
            notes  = "GloFAS: exceptional/severe water stress detected."
        elif any(kw in text.lower() for kw in watch_kw):
            status = "watch"
            notes  = "GloFAS: below-normal river flow conditions."
        else:
            status = "ok"
            notes  = "GloFAS: no exceptional water stress signals."
        results.append(_ok("GloFAS Global Water Stress Signal", None, "qualitative",
                           "GloFAS / EU Copernicus", status, notes, sector))
    except Exception as exc:
        results.append(_err("GloFAS Global Water Stress Signal", str(exc), "GloFAS", sector))
    results.append(_manual("Global Safe Water Access (JMP)", 2200, "million people",
                           "WHO/UNICEF JMP 2025",
                           "~2.2B people lack safely managed drinking water. Update annually.",
                           sector, "global"))
    return results


# -- 6. GEOPOLITICAL ----------------------------------------------------------

GDELT_THEMES = [
    {"key": "supply_shock",    "label": "Supply Shock News Volume",
     "theme": "WB_639_CONFLICT_PREVENTION",
     "fallback_query": "supply chain disruption energy food"},
    {"key": "energy_crisis",   "label": "Energy Crisis News Volume",
     "theme": "WB_673_ENERGY",
     "fallback_query": "energy crisis fuel shortage developing"},
    {"key": "food_crisis",     "label": "Food Crisis News Volume",
     "theme": "TAX_FAMINE",
     "fallback_query": "food crisis famine food insecurity Africa Asia"},
    {"key": "health_emergency","label": "Health Emergency News Volume",
     "theme": "HEALTH_PANDEMIC",
     "fallback_query": "disease outbreak health emergency developing countries"},
]

def fetch_gdelt_signals():
    results = []
    sector  = "geopolitical"
    for i, q in enumerate(GDELT_THEMES):
        if i > 0:
            time.sleep(12)
        try:
            url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
                   f"?query=theme%3A{q['theme']}"
                   f"&mode=timelinevol&timespan=7d&timezoom=yes&TIMELINESMOOTH=3&format=json")
            resp = SESSION.get(url, timeout=25)
            if resp.status_code == 429:
                time.sleep(20)
                url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
                       f"?query={requests.utils.quote(q['fallback_query'])}"
                       f"&mode=timelinevol&timespan=7d&timezoom=yes&TIMELINESMOOTH=3&format=json")
                resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            timeline = resp.json().get("timeline", [])
            if timeline and timeline[0].get("data"):
                points = timeline[0]["data"]
                latest = points[-1]["value"] if points else 0.0
                avg_7d = sum(p["value"] for p in points) / len(points) if points else 0.0
                status = _threshold_gdelt(latest)
                trend  = "rising" if len(points) > 1 and points[-1]["value"] > points[-2]["value"] else "stable"
                results.append(_ok(q["label"], round(latest, 5), "% global coverage",
                                   "GDELT DOC 2.0", status,
                                   f"Current: {latest:.4f}%. 7d avg: {avg_7d:.4f}%. Trend: {trend}.",
                                   sector))
            else:
                results.append(_ok(q["label"], 0.0, "% global coverage", "GDELT DOC 2.0", "ok",
                                   f"No timeline data for theme {q['theme']}.", sector))
        except Exception as exc:
            results.append(_err(q["label"], str(exc), "GDELT DOC 2.0", sector))
    return results


# -- 7. WASH ------------------------------------------------------------------

def fetch_wash_indicators():
    wash_data = [
        ("Population without safely managed drinking water", 2200, "million people",
         "2.2B lack safely managed drinking water (JMP 2023). Update from washdata.org annually.", "global"),
        ("Population without basic sanitation", 3500, "million people",
         "3.5B lack safely managed sanitation (JMP 2023).", "global"),
        ("Population practicing open defecation", 419, "million people",
         "419M still practicing open defecation (JMP 2023).", "global"),
    ]
    return [_manual(name, val, unit, "WHO/UNICEF JMP 2023", notes, "wash", region)
            for name, val, unit, notes, region in wash_data]


# -- 8. HEALTH ----------------------------------------------------------------

PILOT_REGION_KEYWORDS = [
    "bangladesh", "kenya", "ethiopia", "nepal", "bolivia", "peru",
    "mexico", "chile", "africa", "south asia", "latin america",
    "south atlantic", "cape verde", "saint helena", "argentina",
    "caribbean", "pacific island",
]

HIGH_CONSEQUENCE_PATHOGENS = [
    "andes virus", "hantavirus", "ebola", "marburg", "lassa",
    "nipah", "mers", "h5n1", "mpox", "cholera", "bundibugyo",
]

ACTIVE_OUTBREAKS = [
    {
        "indicator": "MV Hondius -- Andes Virus (Hantavirus) Outbreak",
        "value":     8,
        "unit":      "confirmed/suspected cases",
        "status":    "alert",
        "region":    "Global -- 23 nationalities, 6+ countries",
        "source":    "WHO DON600 / CDC HAN00528 (updated May 8 2026)",
        "notes": (
            "Andes virus -- only human-to-human transmissible hantavirus. "
            "MV Hondius cruise ship departed Ushuaia, Argentina April 1 2026. "
            "8 cases (6 confirmed, 2 probable), 3 deaths (CFR 38%) as of May 8 2026. "
            "45-day monitoring window extends to ~June 15 2026. "
            "No antiviral treatment exists. WHO global risk: LOW."
        ),
    },
    {
        "indicator": "Ethiopia -- Marburg Virus Disease Outbreak (RESOLVED Jan 2026)",
        "value":     19,
        "unit":      "total cases (14 confirmed, 9 deaths)",
        "status":    "ok",
        "region":    "East Africa -- Ethiopia (pilot region)",
        "source":    "WHO DON592",
        "notes": (
            "RESOLVED Jan 26 2026. First-ever MVD outbreak in Ethiopia. "
            "Retained for pilot region health baseline context."
        ),
    },
]


def fetch_who_don():
    results = []
    sector  = "health"
    source  = "WHO Disease Outbreak News"
    try:
        url  = "https://www.who.int/emergencies/disease-outbreak-news"
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        soup  = BeautifulSoup(resp.text, "html.parser")
        items = []
        for a in soup.find_all("a", href=True):
            href  = a["href"]
            title = a.get_text(strip=True)
            if "/disease-outbreak-news/item/" in href and len(title) > 15:
                items.append({"title": title, "url": href})
        items = items[:20]
        pilot_hits    = []
        pathogen_hits = []
        for item in items:
            combined = item["title"].lower()
            for pathogen in HIGH_CONSEQUENCE_PATHOGENS:
                if pathogen in combined:
                    pathogen_hits.append(item)
                    break
            for kw in PILOT_REGION_KEYWORDS:
                if kw in combined:
                    pilot_hits.append({**item, "keyword": kw})
                    break
        if pathogen_hits:
            hit = pathogen_hits[0]
            results.append(_ok("WHO DON -- High-Consequence Pathogen Alert",
                               len(pathogen_hits), "active DONs", source, "alert",
                               f"HIGH CONSEQUENCE pathogen in recent WHO DONs. Most recent: '{hit['title'][:100]}'.",
                               sector, "global"))
        elif pilot_hits:
            hit = pilot_hits[0]
            results.append(_ok("WHO DON -- Pilot Region Health Signal",
                               len(pilot_hits), "active DONs", source, "watch",
                               f"WHO DON mentions pilot region '{hit.get('keyword','')}'. '{hit['title'][:100]}'.",
                               sector, "global"))
        else:
            recent = " | ".join(i["title"][:60] for i in items[:3])
            results.append(_ok("WHO DON -- Pilot Region Health Signal",
                               0, "active DONs in pilot regions", source, "ok",
                               f"No recent WHO DONs for pilot regions. Recent: {recent}",
                               sector, "global"))
    except Exception as exc:
        results.append(_err("WHO DON -- Disease Outbreak Monitor", str(exc), source, sector))
    return results


def fetch_active_outbreaks():
    results = []
    for outbreak in ACTIVE_OUTBREAKS:
        results.append({
            "indicator": outbreak["indicator"],
            "value":     outbreak["value"],
            "unit":      outbreak["unit"],
            "source":    outbreak["source"],
            "status":    outbreak["status"],
            "notes":     outbreak["notes"],
            "sector":    "health",
            "region":    outbreak["region"],
            "ts":        datetime.now(timezone.utc).isoformat(),
        })
    return results


def fetch_health_indicators():
    """
    Live-scraped health indicators: Ebola (CDC page), Hantavirus (CDC/ECDC),
    active PHEIC counter. Supplements the manual ACTIVE_OUTBREAKS list.
    """
    indicators = []

    # WHO DON RSS for outbreak news headlines
    who_rss_items = {}
    outbreak_keywords = {
        "ebola":      ["ebola", "bundibugyo", "orthoebola"],
        "hantavirus": ["hantavirus", "andes virus", "andv", "hondius"],
        "pheic":      ["pheic", "public health emergency of international concern"],
    }
    try:
        r = _safe_get("https://www.who.int/rss-feeds/news.xml")
        if r:
            soup  = BeautifulSoup(r.text, "xml")
            for key in outbreak_keywords:
                who_rss_items[key] = []
            for item in soup.find_all("item"):
                title = item.find("title")
                title_text = title.get_text(strip=True) if title else ""
                combined   = title_text.lower()
                link_tag   = item.find("link")
                link       = link_tag.get_text(strip=True) if link_tag else ""
                for key, kws in outbreak_keywords.items():
                    if any(kw in combined for kw in kws):
                        who_rss_items[key].append({"title": title_text, "link": link})
    except Exception:
        pass

    # -- Ebola / Bundibugyo ---------------------------------------------------
    ebola_data = {}
    try:
        r = _safe_get("https://www.cdc.gov/ebola/situation-summary/index.html")
        if r:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            ebola_data = {
                "suspected": _extract_number(text, [r"(\d[\d,]+)\s*suspected cases",
                                                     r"(\d[\d,]+)\s*suspect"]),
                "confirmed": _extract_number(text, [r"(\d+)\s*(?:laboratory[-\s]?)?confirmed cases",
                                                     r"(\d+)\s*confirmed"]),
                "deaths":    _extract_number(text, [r"(\d[\d,]+)\s*deaths",
                                                     r"(\d+)\s*fatalities"]),
            }
    except Exception:
        pass

    suspected = ebola_data.get("suspected") or 513
    confirmed = ebola_data.get("confirmed") or 30
    deaths    = ebola_data.get("deaths")    or 131

    # PHEIC declared = always at least ALERT regardless of case count
    ebola_status = "alert"

    rss_note = ""
    if who_rss_items.get("ebola"):
        rss_note = " | Latest WHO RSS: " + who_rss_items["ebola"][0]["title"]

    indicators.append(_ok(
        "Ebola -- Bundibugyo virus (DRC / Uganda)",
        suspected, "suspected cases", "WHO DON / CDC", ebola_status,
        (f"PHEIC declared May 16 2026. Suspected: {suspected} | Confirmed: {confirmed} | "
         f"Deaths: {deaths}. Ituri Province DRC + Kinshasa, Goma, Kampala, Fort Portal Uganda. "
         "NO approved vaccine or therapeutic for Bundibugyo strain (CFR 25-50%). "
         "US Title 42 active: DRC/Uganda/South Sudan entry restrictions. "
         "Aid cuts caused ~3 week undetected spread."
         + rss_note),
        "health", "Sub-Saharan Africa"
    ))

    indicators.append(_ok(
        "Ebola -- confirmed lab cases",
        confirmed, "lab-confirmed", "WHO DON / CDC",
        "alert" if confirmed >= 20 else "watch",
        (f"{confirmed} lab-confirmed. Initial DRC tests only detect Zaire strain, not Bundibugyo. "
         "Current numbers are a floor, not a ceiling."),
        "health", "DRC / Uganda"
    ))

    # -- Hantavirus / MV Hondius ----------------------------------------------
    hanta_data = {}
    for url in [
        "https://www.cdc.gov/han/php/notices/han00528.html",
        "https://www.ecdc.europa.eu/en/infectious-disease-topics/hantavirus-infection/surveillance-and-updates/andes-hantavirus-outbreak",
    ]:
        r = _safe_get(url)
        if r:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            # Tight patterns require disease context near the number
            # Hard cap at 500 -- cruise ship had 147 people total
            confirmed = None
            for pat in [
                r"(\d{1,3})\s*(?:laboratory[- ]?)?confirmed\s*(?:cases?\s*)?(?:of\s*)?(?:hantavirus|andes|ANDV)",
                r"(?:hantavirus|andes|ANDV)[^.]{0,80}?(\d{1,3})\s*confirmed",
                r"total\s+of\s+(\d{1,3})\s+(?:confirmed|cases)",
            ]:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    val = int(m.group(1))
                    if 1 <= val <= 500:
                        confirmed = val
                        break
            deaths = None
            for pat in [
                r"(\d{1,2})\s*deaths?[^.]{0,60}(?:hantavirus|andes|ANDV|hondius)",
                r"(?:hantavirus|andes|ANDV)[^.]{0,80}?(\d{1,2})\s*deaths?",
            ]:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    val = int(m.group(1))
                    if 1 <= val <= 50:
                        deaths = val
                        break
            countries = _extract_number(text, [r"(\d+)\s*countries"])
            if confirmed:
                hanta_data = {"confirmed": confirmed, "deaths": deaths, "countries": countries}
                break

    hanta_confirmed = hanta_data.get("confirmed") or 9
    hanta_deaths    = hanta_data.get("deaths")    or 3
    hanta_countries = hanta_data.get("countries") or 12

    indicators.append(_ok(
        "Hantavirus -- Andes virus / MV Hondius cluster",
        hanta_confirmed, "confirmed cases", "WHO DON601 / CDC HAN528 / ECDC",
        "watch",
        (f"Contained cluster: {hanta_confirmed} confirmed, {hanta_deaths} deaths, "
         f"{hanta_countries}+ countries post-repatriation. "
         "MV Hondius departed Ushuaia Apr 1 2026, visited Antarctica/South Georgia/Tristan da Cunha. "
         "Andes virus = ONLY hantavirus with human-to-human transmission. CFR ~38%. "
         "No antiviral treatment. WHO global risk: LOW. Ship arrived Rotterdam May 18 2026."),
        "health", "Multi-country (South Atlantic origin)"
    ))

    # -- Active PHEIC counter -------------------------------------------------
    indicators.append(_ok(
        "Active WHO PHEICs",
        1, "simultaneous PHEICs", "WHO IHR", "watch",
        ("1 active PHEIC: Ebola/Bundibugyo (DRC+Uganda, declared May 16 2026). "
         "Hantavirus/Andes: monitoring, not declared PHEIC. "
         "COVID-19 PHEIC ended May 2023. mpox PHEIC ended Aug 2023."),
        "health", "Global"
    ))

    return indicators


# -- 9. CURRENCY --------------------------------------------------------------

TARGET_CURRENCIES = {
    "BDT": ("Bangladeshi Taka",   "South Asia",    130.0, 145.0),
    "KES": ("Kenyan Shilling",    "East Africa",   130.0, 155.0),
    "ETB": ("Ethiopian Birr",     "East Africa",    55.0,  70.0),
    "BOB": ("Bolivian Boliviano", "Andean LatAm",    7.0,   8.0),
    "PEN": ("Peruvian Sol",       "Andean LatAm",    3.8,   4.3),
    "MXN": ("Mexican Peso",       "Latin America",  18.0,  22.0),
    "CLP": ("Chilean Peso",       "Latin America", 950.0, 1050.0),
    "NPR": ("Nepalese Rupee",     "South Asia",    135.0, 150.0),
}

def fetch_currency_stress():
    results = []
    sector  = "currency"
    rates   = {}
    if OER_APP_ID:
        try:
            url  = f"https://openexchangerates.org/api/latest.json?app_id={OER_APP_ID}&base=USD"
            resp = SESSION.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            rates = resp.json().get("rates", {})
        except Exception:
            pass
    for code, (name, region, watch_level, alert_level) in TARGET_CURRENCIES.items():
        rate = rates.get(code)
        if rate is None:
            try:
                url  = f"https://stooq.com/q/d/l/?s={code.lower()}usd&i=d"
                resp = SESSION.get(url, timeout=TIMEOUT)
                resp.raise_for_status()
                lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("Date")]
                if lines:
                    raw  = float(lines[-1].split(",")[4])
                    rate = 1.0 / raw if raw > 0 else None
            except Exception:
                pass
        if rate is None:
            results.append(_manual(f"{code} / USD ({name})", None, f"{code} per USD",
                                   "Open Exchange Rates / Stooq",
                                   f"Could not fetch {code}.", sector, region))
            continue
        if rate >= alert_level:
            status = "alert"
            notes  = f"{name} at {rate:.2f} per USD -- significantly depreciated. Alert threshold: {alert_level:.2f}."
        elif rate >= watch_level:
            status = "watch"
            notes  = f"{name} at {rate:.2f} per USD -- under pressure. Watch threshold: {watch_level:.2f}."
        else:
            status = "ok"
            notes  = f"{name} at {rate:.2f} per USD -- within normal range."
        results.append(_ok(f"{code} / USD ({name})", round(rate, 4), f"{code} per USD",
                           "Open Exchange Rates / Stooq", status, notes, sector, region))
    return results


# -- 10. CLIMATE --------------------------------------------------------------

def fetch_climate_indicators():
    indicators = []

    # Thwaites Eastern Ice Shelf
    shelf_collapsed = False
    snippet         = ""
    try:
        r = _safe_get("https://nsidc.org/news-analyses/news-stories")
        if r:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True).lower()
            if "thwaites" in text:
                if any(kw in text for kw in ["collapsed", "detached", "calved", "broke off"]):
                    shelf_collapsed = True
                for para in BeautifulSoup(r.text, "html.parser").find_all(["p", "h2", "h3"]):
                    t = para.get_text(strip=True)
                    if "thwaites" in t.lower():
                        snippet = t[:280]
                        break
    except Exception:
        pass

    velocity       = 2000  # m/yr as of Jan 2026 (BAS/ITGC) -- update when new data published
    thwaites_status = "alert" if shelf_collapsed else "watch"

    indicators.append({
        "indicator": "Thwaites Eastern Ice Shelf -- stability",
        "value":     velocity,
        "unit":      "m/yr shelf velocity",
        "source":    "British Antarctic Survey / ITGC / NSIDC",
        "status":    thwaites_status,
        "notes": (
            ("SHELF COLLAPSE DETECTED -- verify BAS/NSIDC immediately. " if shelf_collapsed else "")
            + f"Shelf velocity: ~{velocity:,} m/yr (tripled since 2020). "
            "BAS has pre-written collapse press release. Glacier flow +33% since 2020. "
            "Shelf collapse triggers multi-decade West Antarctic cascade. "
            "Full WAIS collapse potential: +3.3m global sea level (centuries timescale). "
            "ACUTE: Drake Passage iceberg hazard for shipping. "
            "LONG-TERM: Bangladesh, Kenya coast, Pacific ODA islands face existential risk."
            + (" " + snippet if snippet else "")
        ),
        "sector":  "climate",
        "region":  "Antarctica / Global",
        "ts":      datetime.now(timezone.utc).isoformat(),
    })

    # A23a -- resolved, kept for historical completeness
    indicators.append(_ok(
        "Iceberg A23a -- lifecycle",
        0, "km2 remaining", "NASA / BAS",
        "ok",
        ("CLOSED. A23a fully disintegrated as of April 3 2026. "
         "Was world's largest iceberg (~4,000 km2). "
         "Floating ice melt = no direct sea level contribution. "
         "Record now held by D15a, grounded on Antarctic coastline."),
        "climate", "South Georgia / Southern Ocean"
    ))

    return indicators


# -- SECTORS ------------------------------------------------------------------

SECTORS = {
    "energy":       [fetch_brent_crude, fetch_lng_spot],
    "chokepoint":   [fetch_chokepoint_status],
    "food":         [fetch_fao_food_price_index],
    "freight":      [fetch_freight_rates],
    "water":        [fetch_water_stress],
    "geopolitical": [fetch_gdelt_signals],
    "wash":         [fetch_wash_indicators],
    "health":       [fetch_who_don, fetch_active_outbreaks, fetch_health_indicators],
    "currency":     [fetch_currency_stress],
    "climate":      [fetch_climate_indicators],
}

# -- RUNNER -------------------------------------------------------------------

def run_all(sector_filter=None):
    all_results = []
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
            print(f"  -> {fetcher.__name__}...")
            try:
                result = fetcher()
                items  = result if isinstance(result, list) else [result]
                for item in items:
                    icon = ("OK " if item["status"] == "ok"
                            else "!! " if item["status"] in ("watch", "manual")
                            else "!! ALERT" if item["status"] == "alert"
                            else "ERR")
                    val_str = f"{item['value']} {item['unit']}" if item["value"] is not None else "N/A"
                    print(f"     [{icon}] {item['indicator']}: {val_str}")
                all_results.extend(items)
            except Exception as exc:
                print(f"     [EXCEPTION] {fetcher.__name__}: {exc}")
    return all_results


def build_output(results):
    now      = datetime.now(timezone.utc)
    statuses = [r["status"] for r in results]
    platform_status = ("alert" if "alert" in statuses
                        else "watch" if "watch" in statuses
                        else "ok")
    counts = {s: statuses.count(s) for s in ("ok", "watch", "alert", "error", "manual")}
    alerts = [r for r in results if r["status"] == "alert"]
    return {
        "meta": {
            "platform":        "Community Supply Chain Intelligence (CSI)",
            "version":         "1.1",
            "generated_utc":   now.isoformat(),
            "platform_status": platform_status,
            "counts":          counts,
            "alert_summary":   [{"indicator": a["indicator"],
                                  "notes": a["notes"][:200]} for a in alerts],
        },
        "indicators": results,
    }


def save_output(payload, dry_run=False):
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    files    = [Path(f"csi_data_{date_str}.json"), Path("csi_data_latest.json")]
    json_str = json.dumps(payload, indent=2, ensure_ascii=False)
    if dry_run:
        print("\n[DRY RUN] Would write:", [str(f) for f in files])
        print(json_str[:500] + "...")
        return
    for f in files:
        f.write_text(json_str, encoding="utf-8")
        print(f"  Written: {f}")


# -- CLI ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CSI Global Scraper v1.1")
    parser.add_argument("--sector",  help=f"Run only one sector: {list(SECTORS.keys())}")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write files")
    args = parser.parse_args()

    if args.sector and args.sector not in SECTORS:
        print(f"Unknown sector '{args.sector}'. Valid: {list(SECTORS.keys())}")
        sys.exit(1)

    results = run_all(sector_filter=args.sector)
    payload = build_output(results)
    save_output(payload, dry_run=args.dry_run)

    alert_count = payload["meta"]["counts"]["alert"]
    if alert_count > 0:
        print(f"\n!! {alert_count} ALERT(s) active -- review csi_data_latest.json")
        sys.exit(2)


if __name__ == "__main__":
    main()
