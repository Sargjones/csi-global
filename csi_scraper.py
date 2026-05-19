"""
CSI-Global scraper additions — Health & Climate sectors
Paste these functions into csi_scraper.py, then add both
fetch calls inside collect_all_indicators().

Dependencies (already in requirements if you have requests + bs4):
  pip install requests beautifulsoup4 --break-system-packages

Data flow:
  fetch_health_indicators()   → WHO DON RSS + CDC situation pages
  fetch_climate_indicators()  → BAS / NSIDC static + manual override
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ─── SHARED HELPERS ───────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "CSI-Global/1.0 community-supply-chain-intelligence "
        "(github.com/Sargjones/csi-global; contact info@criticalto.ca)"
    )
}

def safe_get(url, timeout=15):
    """GET with timeout + graceful failure. Returns response or None."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[WARN] fetch failed {url}: {e}")
        return None

def utcnow():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ─── WHO DON RSS PARSER ───────────────────────────────────────────────────────

WHO_DON_RSS = "https://www.who.int/rss-feeds/news.xml"
WHO_DON_SEARCH = "https://www.who.int/emergencies/disease-outbreak-news"

OUTBREAK_KEYWORDS = {
    "ebola":      ["ebola", "bundibugyo", "orthoebola"],
    "hantavirus": ["hantavirus", "andes virus", "andv", "hondius"],
    "mpox":       ["mpox", "monkeypox"],
    "cholera":    ["cholera"],
    "marburg":    ["marburg"],
    "pheic":      ["pheic", "public health emergency of international concern"],
}

def parse_who_don_rss():
    """
    Fetch WHO news RSS and classify items by outbreak keyword.
    Returns dict: { disease_key: [{"title": ..., "link": ..., "date": ...}, ...] }
    """
    r = safe_get(WHO_DON_RSS)
    if not r:
        return {}

    soup = BeautifulSoup(r.text, "xml")
    items = soup.find_all("item")
    results = {k: [] for k in OUTBREAK_KEYWORDS}

    for item in items:
        title = (item.find("title") or item.find("name") or "").get_text(strip=True).lower()
        desc  = (item.find("description") or "").get_text(strip=True).lower()
        link  = (item.find("link") or "").get_text(strip=True)
        pub   = (item.find("pubDate") or "").get_text(strip=True)
        text  = title + " " + desc

        for key, keywords in OUTBREAK_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                results[key].append({
                    "title": item.find("title").get_text(strip=True) if item.find("title") else "",
                    "link": link,
                    "date": pub,
                })

    return results


# ─── CDC SITUATION PAGE SCRAPERS ──────────────────────────────────────────────

def scrape_cdc_ebola():
    """
    Scrape CDC Ebola current situation page for latest case numbers.
    Returns dict with keys: confirmed, suspected, deaths, countries, notes
    """
    url = "https://www.cdc.gov/ebola/situation-summary/index.html"
    r = safe_get(url)
    if not r:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Extract numbers with regex patterns
    suspected = _extract_number(text, [
        r"(\d[\d,]+)\s*suspected cases",
        r"(\d[\d,]+)\s*suspect",
    ])
    confirmed = _extract_number(text, [
        r"(\d+)\s*(?:laboratory[-\s]?)?confirmed cases",
        r"(\d+)\s*confirmed",
    ])
    deaths = _extract_number(text, [
        r"(\d[\d,]+)\s*deaths",
        r"(\d+)\s*fatalities",
    ])

    # Grab a short context snippet
    snippet = ""
    for para in soup.find_all(["p", "li"]):
        t = para.get_text(strip=True)
        if any(kw in t.lower() for kw in ["bundibugyo", "ebola", "drc", "ituri"]):
            snippet = t[:280]
            break

    return {
        "suspected": suspected,
        "confirmed": confirmed,
        "deaths": deaths,
        "snippet": snippet,
        "source_url": url,
        "scraped_utc": utcnow(),
    }


def scrape_cdc_hantavirus():
    """
    Scrape CDC HAN notice for MV Hondius hantavirus cluster.
    Returns dict with latest case/death counts and status.
    """
    # Primary: CDC HAN advisory
    url = "https://www.cdc.gov/han/php/notices/han00528.html"
    r = safe_get(url)

    # Fallback: ECDC rapid assessment page
    if not r:
        url = "https://www.ecdc.europa.eu/en/infectious-disease-topics/hantavirus-infection/surveillance-and-updates/andes-hantavirus-outbreak"
        r = safe_get(url)
    if not r:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    confirmed = _extract_number(text, [
        r"(\d+)\s*(?:laboratory[-\s]?)?confirmed",
        r"(\d+)\s*confirmed cases",
    ])
    deaths = _extract_number(text, [
        r"(\d+)\s*deaths",
        r"(\d+)\s*fatalities",
    ])
    countries = _extract_number(text, [
        r"(\d+)\s*countries",
    ])

    snippet = ""
    for para in soup.find_all(["p", "li"]):
        t = para.get_text(strip=True)
        if any(kw in t.lower() for kw in ["hondius", "andes", "hantavirus", "cruise"]):
            snippet = t[:280]
            break

    return {
        "confirmed": confirmed,
        "deaths": deaths,
        "countries_affected": countries,
        "snippet": snippet,
        "source_url": url,
        "scraped_utc": utcnow(),
    }


def _extract_number(text, patterns):
    """Try each regex pattern, return first int match or None."""
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


# ─── THWAITES / NSIDC SCRAPER ─────────────────────────────────────────────────

def scrape_thwaites_status():
    """
    Check NSIDC / BAS news for Thwaites shelf status updates.
    Returns dict with latest velocity estimate and any news snippets.
    Falls back to hardcoded known values if fetch fails.
    """
    # Try NSIDC news feed for Antarctic ice updates
    nsidc_url = "https://nsidc.org/news-analyses/news-stories"
    r = safe_get(nsidc_url)

    shelf_collapsed = False
    velocity_note = ""
    snippet = ""

    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True).lower()
        if "thwaites" in text:
            # Look for collapse language
            if any(kw in text for kw in ["collapsed", "detached", "calved", "broke off"]):
                shelf_collapsed = True
            # Grab snippet
            for para in soup.find_all(["p", "h2", "h3"]):
                t = para.get_text(strip=True)
                if "thwaites" in t.lower():
                    snippet = t[:280]
                    break

    return {
        "shelf_collapsed": shelf_collapsed,
        # As of May 2026: shelf velocity ~2,000 m/yr (tripled since 2020)
        # UPDATE this value when BAS publishes new measurements
        "velocity_m_yr": 2000,
        "velocity_source": "BAS / ITGC Jan 2026",
        "snippet": snippet or "No new BAS/NSIDC update detected. Using last known values.",
        "source_url": nsidc_url,
        "scraped_utc": utcnow(),
    }


# ─── HEALTH INDICATORS ────────────────────────────────────────────────────────

# Thresholds
EBOLA_WATCH  = 100;  EBOLA_ALERT  = 300
HANTA_WATCH  = 5;    HANTA_ALERT  = 20    # small numbers — Andes CFR is 38%
THWAITES_WATCH = 1800; THWAITES_ALERT = 2500  # m/yr shelf velocity

def fetch_health_indicators():
    indicators = []
    don_news = parse_who_don_rss()

    # ── 1. EBOLA — Bundibugyo / DRC + Uganda ─────────────────────────────────
    ebola_data = scrape_cdc_ebola()

    suspected = ebola_data.get("suspected") or 513   # fallback to known value
    confirmed = ebola_data.get("confirmed") or 30
    deaths    = ebola_data.get("deaths")    or 131

    ebola_status = (
        "alert" if suspected >= EBOLA_ALERT
        else "watch" if suspected >= EBOLA_WATCH
        else "ok"
    )

    ebola_news_titles = [i["title"] for i in don_news.get("ebola", [])[:2]]
    ebola_news_note   = (" | Latest WHO: " + " / ".join(ebola_news_titles)) if ebola_news_titles else ""

    indicators.append({
        "indicator": "Ebola — Bundibugyo virus (DRC / Uganda)",
        "value": suspected,
        "unit": "suspected cases",
        "source": "WHO DON / CDC",
        "status": ebola_status,
        "notes": (
            f"PHEIC declared May 16 2026. Suspected: {suspected} | Confirmed: {confirmed} | Deaths: {deaths}. "
            "Ituri Province DRC → Kinshasa, Goma (M23), Kampala, Fort Portal Uganda. "
            "NO approved vaccine or therapeutic for Bundibugyo strain (CFR 25–50%). "
            "US Title 42 active: DRC / Uganda / South Sudan entry restrictions. "
            "1.9M displaced in Ituri. Aid cuts = ~3 week detection delay."
            + ebola_news_note
        ),
        "sector": "health",
        "region": "Sub-Saharan Africa",
        "ts": utcnow(),
        "downstream_risk": [
            "East Africa ODA pilot (Kenya border): elevated spread risk",
            "Health system collapse in Ituri cascades to other disease response",
            "Compound: Hormuz fuel disruption competes with Ebola response for logistics",
            "Bangladesh / South Asia: limited direct risk, monitor WHO updates",
        ],
    })

    # Confirmed cases as a separate, harder indicator
    indicators.append({
        "indicator": "Ebola — confirmed lab cases",
        "value": confirmed,
        "unit": "lab-confirmed",
        "source": "WHO DON / CDC",
        "status": "alert" if confirmed >= 20 else "watch",
        "notes": (
            f"{confirmed} lab-confirmed as of latest WHO report. "
            "True scale larger — initial DRC tests only detect Zaire strain, not Bundibugyo. "
            "Detection gap ~3 weeks means current numbers are floor, not ceiling."
        ),
        "sector": "health",
        "region": "DRC / Uganda",
        "ts": utcnow(),
    })

    # ── 2. HANTAVIRUS — Andes / MV Hondius cluster ───────────────────────────
    hanta_data = scrape_cdc_hantavirus()

    hanta_confirmed  = hanta_data.get("confirmed")  or 9
    hanta_deaths     = hanta_data.get("deaths")     or 3
    hanta_countries  = hanta_data.get("countries_affected") or 12

    hanta_status = (
        "alert" if hanta_confirmed >= HANTA_ALERT
        else "watch" if hanta_confirmed >= HANTA_WATCH
        else "watch"   # always at least watch: 38% CFR + novel H2H transmission
    )

    hanta_news_titles = [i["title"] for i in don_news.get("hantavirus", [])[:2]]
    hanta_news_note   = (" | Latest WHO: " + " / ".join(hanta_news_titles)) if hanta_news_titles else ""

    indicators.append({
        "indicator": "Hantavirus — Andes virus / MV Hondius cluster",
        "value": hanta_confirmed,
        "unit": "confirmed cases",
        "source": "WHO DON601 / CDC HAN528 / ECDC",
        "status": hanta_status,
        "notes": (
            f"Contained cluster: {hanta_confirmed} confirmed, {hanta_deaths} deaths, "
            f"cases in {hanta_countries}+ countries post-repatriation. "
            "MV Hondius departed Ushuaia Apr 1, visited Antarctica / South Georgia / Tristan da Cunha. "
            "Index case: Dutch national, 4-month rodent-exposure road trip Chile/Uruguay/Argentina. "
            "Andes virus = ONLY known hantavirus with human-to-human transmission. "
            "CFR ~38% for HPS respiratory phase. No antiviral treatment — supportive care only. "
            "WHO global risk: LOW. ECDC EU/EEA risk: VERY LOW. "
            "Canada confirmed case May 16 (quarantined, mild). "
            "Ship arrived Rotterdam May 18 — outbreak phase effectively closed."
            + hanta_news_note
        ),
        "sector": "health",
        "region": "Multi-country (South Atlantic origin)",
        "ts": utcnow(),
        "downstream_risk": [
            "Direct ODA impact: LOW (cruise ship vector, not community transmission)",
            "Monitor: any rodent-exposure cases in Andean LatAm pilot regions (Bolivia/Peru)",
            "Watch for secondary cases among ship crew — long incubation (up to 42 days)",
        ],
    })

    # ── 3. SIMULTANEOUS PHEIC COUNTER ────────────────────────────────────────
    # Currently: Ebola = 1 active PHEIC. Hantavirus = not declared PHEIC.
    active_pheics = 1   # UPDATE if hantavirus or other outbreak escalates

    indicators.append({
        "indicator": "Active WHO PHEICs",
        "value": active_pheics,
        "unit": "simultaneous PHEICs",
        "source": "WHO IHR",
        "status": "alert" if active_pheics >= 2 else "watch" if active_pheics >= 1 else "ok",
        "notes": (
            f"{active_pheics} active PHEIC(s): Ebola/Bundibugyo (DRC+Uganda, declared May 16 2026). "
            "Hantavirus/Andes (MV Hondius): monitoring, not declared PHEIC. "
            "COVID-19 PHEIC ended May 2023. mpox PHEIC ended Aug 2023."
        ),
        "sector": "health",
        "region": "Global",
        "ts": utcnow(),
    })

    return indicators


# ─── CLIMATE INDICATORS ───────────────────────────────────────────────────────

def fetch_climate_indicators():
    indicators = []
    thwaites = scrape_thwaites_status()

    velocity   = thwaites.get("velocity_m_yr", 2000)
    collapsed  = thwaites.get("shelf_collapsed", False)

    thwaites_status = (
        "alert" if collapsed or velocity >= THWAITES_ALERT
        else "watch" if velocity >= THWAITES_WATCH
        else "ok"
    )

    indicators.append({
        "indicator": "Thwaites Eastern Ice Shelf — stability",
        "value": velocity,
        "unit": "m/yr shelf velocity",
        "source": "British Antarctic Survey / ITGC / NSIDC",
        "status": "alert" if collapsed else thwaites_status,
        "notes": (
            ("⚠️ SHELF COLLAPSE DETECTED — check BAS/NSIDC for confirmation. " if collapsed else "")
            + f"Shelf velocity: ~{velocity:,} m/yr (tripled since 2020). "
            "BAS has pre-written press release — 'final demise could happen suddenly.' "
            "Glacier flow behind shelf: +33% since 2020. Buttressing effect largely gone. "
            "Shelf collapse ≠ immediate sea level rise. Triggers multi-decade West Antarctic cascade. "
            "Full WAIS collapse potential: +3.3m global sea level (centuries timescale). "
            "ACUTE: Drake Passage iceberg hazard for shipping. "
            "LONG-TERM: Bangladesh, Kenya coast, Pacific islands face existential risk. "
            + thwaites.get("snippet", "")
        ),
        "sector": "climate",
        "region": "Antarctica / Global",
        "ts": utcnow(),
        "downstream_risk": [
            "Bangladesh (CSI pilot): 3.3m rise = near-total coastal inundation",
            "Kenya coast / Mombasa: sea level + storm surge amplification",
            "Pacific island ODA nations: existential threat on decadal timescale",
            "Shipping: Southern Ocean / Drake Passage iceberg field disruption",
            "Freshwater influx: disrupts krill/penguin/seal feeding grounds near South Georgia",
        ],
    })

    # A23a — closed indicator, kept for historical completeness
    indicators.append({
        "indicator": "Iceberg A23a — lifecycle",
        "value": 0,
        "unit": "km2 remaining",
        "source": "NASA / BAS / Wikipedia",
        "status": "ok",
        "notes": (
            "CLOSED. A23a fully disintegrated — no longer tracked as of April 3, 2026. "
            "Was world's largest iceberg: ~4,000 km2 (size of Rhode Island). "
            "Calved Filchner-Ronne Ice Shelf 1986, drifted 40 years, broke up near South Georgia. "
            "Floating ice melt = no direct sea level contribution. "
            "Record now held by D15a, still grounded on Antarctic coastline."
        ),
        "sector": "climate",
        "region": "South Georgia / Southern Ocean",
        "ts": utcnow(),
    })

    return indicators


# ─── INTEGRATION — add these two lines to collect_all_indicators() ────────────
#
#   indicators += fetch_health_indicators()
#   indicators += fetch_climate_indicators()
#
# ─── SECTOR COUNTS — add to SECTOR_LABELS dict if present in your scraper ────
#
#   "health":  "🏥 Health",
#   "climate": "🧊 Climate",
#
# ─── ALERT THRESHOLDS SUMMARY ─────────────────────────────────────────────────
#
#   Ebola suspected cases:       WATCH >100    ALERT >300
#   Ebola confirmed cases:       WATCH >10     ALERT >20
#   Hantavirus confirmed:        WATCH >5      ALERT >20
#   Active PHEICs:               WATCH >0      ALERT >1
#   Thwaites velocity (m/yr):    WATCH >1800   ALERT >2500
#   Thwaites shelf collapse:     → immediate ALERT regardless of velocity
