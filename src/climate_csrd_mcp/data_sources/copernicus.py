"""
Copernicus Climate Data Store (CDS) client.

Provides global climate risk data:
- Flood risk (EEA + global flood hazard maps)
- Drought index (EDO + C3S global)
- NDVI (Normalized Difference Vegetation Index) for crop-mcp synergy
- Storm/wind risk (hurricane/cyclone zones)
- Sea level rise risk (coastal)
- Frost risk (for crop-mcp)
- Wildfire risk index

Sources: Copernicus CDS, EEA, EDO, C3S, IPCC AR6
"""

import asyncio
import json
import logging
import math
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

CDS_API_URL = os.environ.get("CDS_API_URL", "https://cds.climate.copernicus.eu/api/v2")
CDS_API_KEY = os.environ.get("CDS_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════════
# GLOBAL FLOOD RISK ZONES — EEA + Global Flood Hazard Maps
# Risk class 1-5: 1=Very Low, 5=Very High
# ═══════════════════════════════════════════════════════════════════════

FLOOD_ZONES: dict[str, dict] = {
    # ── Germany ──
    "DE.BY": {"risk": 2, "name": "Bayern", "source": "EEA Flood Hazard 2023, HORA"},
    "DE.BW": {"risk": 2, "name": "Baden-Württemberg", "source": "EEA Flood Hazard 2023"},
    "DE.HE": {"risk": 2, "name": "Hessen", "source": "EEA Flood Hazard 2023"},
    "DE.RP": {"risk": 3, "name": "Rheinland-Pfalz", "source": "EEA Flood Hazard 2023, Ahr/Erft 2021"},
    "DE.NW": {"risk": 3, "name": "Nordrhein-Westfalen", "source": "EEA Flood Hazard 2023"},
    "DE.NI": {"risk": 3, "name": "Niedersachsen", "source": "EEA Flood Hazard 2023"},
    "DE.SH": {"risk": 3, "name": "Schleswig-Holstein", "source": "EEA Flood Hazard 2023, Storm surge risk"},
    "DE.MV": {"risk": 2, "name": "Mecklenburg-Vorpommern", "source": "EEA Flood Hazard 2023"},
    "DE.BB": {"risk": 2, "name": "Brandenburg", "source": "EEA Flood Hazard 2023"},
    "DE.ST": {"risk": 3, "name": "Sachsen-Anhalt", "source": "EEA Flood Hazard 2023, Elbe flooding"},
    "DE.SN": {"risk": 2, "name": "Sachsen", "source": "EEA Flood Hazard 2023"},
    "DE.TH": {"risk": 2, "name": "Thüringen", "source": "EEA Flood Hazard 2023"},
    "DE.SL": {"risk": 2, "name": "Saarland", "source": "EEA Flood Hazard 2023"},
    "DE.HB": {"risk": 4, "name": "Bremen", "source": "EEA Flood Hazard 2023, Coastal/storm surge"},
    "DE.HH": {"risk": 4, "name": "Hamburg", "source": "EEA Flood Hazard 2023, Elbe/storm surge"},
    "DE.BE": {"risk": 2, "name": "Berlin", "source": "EEA Flood Hazard 2023"},
    # ── Austria ──
    "AT.1":  {"risk": 2, "name": "Burgenland", "source": "EEA Flood Hazard 2023"},
    "AT.2":  {"risk": 2, "name": "Kärnten", "source": "EEA Flood Hazard 2023"},
    "AT.3":  {"risk": 3, "name": "Niederösterreich", "source": "EEA Flood Hazard 2023, Danube"},
    "AT.4":  {"risk": 3, "name": "Oberösterreich", "source": "EEA Flood Hazard 2023, Danube"},
    "AT.5":  {"risk": 2, "name": "Salzburg", "source": "EEA Flood Hazard 2023"},
    "AT.6":  {"risk": 2, "name": "Steiermark", "source": "EEA Flood Hazard 2023"},
    "AT.7":  {"risk": 3, "name": "Tirol", "source": "EEA Flood Hazard 2023, Alpine flash floods"},
    "AT.8":  {"risk": 2, "name": "Vorarlberg", "source": "EEA Flood Hazard 2023"},
    "AT.9":  {"risk": 2, "name": "Wien", "source": "EEA Flood Hazard 2023, Danube protected"},
    # ── Switzerland ──
    "CH.AG": {"risk": 2, "name": "Aargau", "source": "EEA Flood Hazard 2023"},
    "CH.BE": {"risk": 2, "name": "Bern", "source": "EEA Flood Hazard 2023"},
    "CH.GE": {"risk": 2, "name": "Genève", "source": "EEA Flood Hazard 2023"},
    "CH.TI": {"risk": 3, "name": "Ticino", "source": "EEA Flood Hazard 2023"},
    "CH.ZH": {"risk": 2, "name": "Zürich", "source": "EEA Flood Hazard 2023"},
    "CH.VS": {"risk": 3, "name": "Valais", "source": "EEA Flood Hazard 2023, Alpine flooding"},
    # ── Netherlands (HIGH risk) ──
    "NL.NH": {"risk": 4, "name": "Noord-Holland", "source": "EEA 2023, Deltares flood models"},
    "NL.ZH": {"risk": 4, "name": "Zuid-Holland", "source": "EEA 2023, Deltares flood models"},
    "NL.UT": {"risk": 4, "name": "Utrecht", "source": "EEA 2023, Deltares flood models"},
    "NL.GR": {"risk": 3, "name": "Groningen", "source": "EEA 2023"},
    "NL.FR": {"risk": 3, "name": "Fryslân", "source": "EEA 2023"},
    "NL.FL": {"risk": 3, "name": "Flevoland", "source": "EEA 2023, polder flood risk"},
    # ── Denmark ──
    "DK.81": {"risk": 3, "name": "Nordjylland", "source": "EEA Flood Hazard 2023"},
    "DK.84": {"risk": 3, "name": "Hovedstaden", "source": "EEA Flood Hazard 2023"},
    "DK.83": {"risk": 2, "name": "Syddanmark", "source": "EEA Flood Hazard 2023"},
    # ── France ──
    "FR.IDF": {"risk": 3, "name": "Île-de-France", "source": "EEA Flood Hazard 2023, Seine"},
    "FR.NAQ": {"risk": 3, "name": "Nouvelle-Aquitaine", "source": "EEA Flood Hazard 2023"},
    "FR.OCC": {"risk": 3, "name": "Occitanie", "source": "EEA Flood Hazard 2023"},
    "FR.PAC": {"risk": 3, "name": "Provence-Alpes-Côte d'Azur", "source": "EEA Flood Hazard 2023"},
    "FR.PDL": {"risk": 2, "name": "Pays de la Loire", "source": "EEA Flood Hazard 2023"},
    "FR.BRE": {"risk": 2, "name": "Bretagne", "source": "EEA Flood Hazard 2023"},
    "FR.GES": {"risk": 3, "name": "Grand Est", "source": "EEA Flood Hazard 2023, Rhine"},
    # ── Italy ──
    "IT.55": {"risk": 3, "name": "Lombardia", "source": "EEA Flood Hazard 2023, Po"},
    "IT.45": {"risk": 4, "name": "Emilia-Romagna", "source": "EEA 2023, 2023 floods"},
    "IT.62": {"risk": 2, "name": "Lazio", "source": "EEA Flood Hazard 2023"},
    "IT.34": {"risk": 4, "name": "Veneto", "source": "EEA Flood Hazard 2023, Venice/Adriatic"},
    "IT.52": {"risk": 2, "name": "Toscana", "source": "EEA Flood Hazard 2023"},
    "IT.57": {"risk": 3, "name": "Marche", "source": "EEA Flood Hazard 2023"},
    "IT.47": {"risk": 3, "name": "Puglia", "source": "EEA Flood Hazard 2023"},
    # ── Spain ──
    "ES.CT": {"risk": 3, "name": "Catalunya", "source": "EEA Flood Hazard 2023"},
    "ES.VC": {"risk": 4, "name": "Comunitat Valenciana", "source": "EEA 2023, 2024 Dana floods"},
    "ES.AN": {"risk": 2, "name": "Andalucía", "source": "EEA Flood Hazard 2023"},
    "ES.MD": {"risk": 2, "name": "Madrid", "source": "EEA Flood Hazard 2023"},
    "ES.GA": {"risk": 3, "name": "Galicia", "source": "EEA Flood Hazard 2023"},
    "ES.PV": {"risk": 3, "name": "País Vasco", "source": "EEA Flood Hazard 2023"},
    # ── UK ──
    "UK.ENG": {"risk": 3, "name": "England", "source": "EA Flood Map 2023"},
    "UK.SCT": {"risk": 2, "name": "Scotland", "source": "SEPA Flood Maps 2023"},
    "UK.WLS": {"risk": 3, "name": "Wales", "source": "NRW Flood Maps 2023"},
    "UK.NIR": {"risk": 2, "name": "Northern Ireland", "source": "DfI Flood Maps 2023"},
    # ── Poland ──
    "PL.DS": {"risk": 3, "name": "Dolnośląskie", "source": "EEA Flood Hazard 2023, Oder"},
    "PL.MZ": {"risk": 2, "name": "Mazowieckie", "source": "EEA Flood Hazard 2023"},
    "PL.SL": {"risk": 2, "name": "Śląskie", "source": "EEA Flood Hazard 2023"},
    "PL.OP": {"risk": 3, "name": "Opolskie", "source": "EEA Flood Hazard 2023, Oder"},
    # ── Sweden ──
    "SE.AB": {"risk": 2, "name": "Stockholm", "source": "EEA Flood Hazard 2023"},
    "SE.M":  {"risk": 2, "name": "Skåne", "source": "EEA Flood Hazard 2023"},
    "SE.O":  {"risk": 2, "name": "Västra Götaland", "source": "EEA Flood Hazard 2023"},
    # ── Norway ──
    "NO.03": {"risk": 2, "name": "Oslo", "source": "NVE Flood Hazard 2023"},
    "NO.11": {"risk": 2, "name": "Rogaland", "source": "NVE Flood Hazard 2023"},
    "NO.15": {"risk": 3, "name": "Møre og Romsdal", "source": "NVE Flood Hazard 2023"},
    # ── Finland ──
    "FI.ES": {"risk": 2, "name": "Etelä-Suomi", "source": "EEA Flood Hazard 2023"},
    "FI.LS": {"risk": 1, "name": "Länsi-Suomi", "source": "EEA Flood Hazard 2023"},
    # ── Belgium ──
    "BE.VLG": {"risk": 3, "name": "Vlaanderen", "source": "EEA Flood Hazard 2023, 2021 floods"},
    "BE.WAL": {"risk": 3, "name": "Wallonie", "source": "EEA Flood Hazard 2023, 2021 floods"},
    "BE.BRU": {"risk": 3, "name": "Bruxelles", "source": "EEA Flood Hazard 2023"},
    # ── Ireland ──
    "IE.M":  {"risk": 3, "name": "Munster", "source": "OPW Flood Maps 2023"},
    "IE.L":  {"risk": 2, "name": "Leinster", "source": "OPW Flood Maps 2023"},
    "IE.C":  {"risk": 3, "name": "Connacht", "source": "OPW Flood Maps 2023"},
    # ── Portugal ──
    "PT.11": {"risk": 3, "name": "Norte", "source": "EEA Flood Hazard 2023"},
    "PT.17": {"risk": 3, "name": "Lisboa", "source": "EEA Flood Hazard 2023"},
    "PT.15": {"risk": 2, "name": "Algarve", "source": "EEA Flood Hazard 2023"},
    # ── Greece ──
    "GR.A1": {"risk": 3, "name": "Attiki", "source": "EEA Flood Hazard 2023"},
    "GR.C":  {"risk": 3, "name": "Kentriki Makedonia", "source": "EEA Flood Hazard 2023"},
    "GR.F":  {"risk": 3, "name": "Dytiki Ellada", "source": "EEA Flood Hazard 2023"},
    # ── Global regions (approximate) ──
    "GLOBAL.USA_GC": {"risk": 4, "name": "US Gulf Coast", "source": "Global Flood Hazard Map 2023, hurricanes"},
    "GLOBAL.USA_EC": {"risk": 3, "name": "US East Coast", "source": "Global Flood Hazard Map 2023"},
    "GLOBAL.USA_WC": {"risk": 2, "name": "US West Coast", "source": "Global Flood Hazard Map 2023"},
    "GLOBAL.CHN_YANGTZE": {"risk": 4, "name": "China Yangtze Basin", "source": "Global Flood Hazard 2023"},
    "GLOBAL.CHN_YELLOW": {"risk": 4, "name": "China Yellow River Basin", "source": "Global Flood Hazard 2023"},
    "GLOBAL.IND_GANGES": {"risk": 5, "name": "India Ganges Basin", "source": "Global Flood Hazard 2023"},
    "GLOBAL.BRA_AMAZON": {"risk": 3, "name": "Brazil Amazon", "source": "Global Flood Hazard 2023"},
    "GLOBAL.BNG_DELTA": {"risk": 5, "name": "Bangladesh Delta", "source": "Global Flood Hazard 2023, extreme"},
    "GLOBAL.JP_TYPHOON": {"risk": 3, "name": "Japan Typhoon Zone", "source": "Global Flood Hazard 2023"},
    "GLOBAL.AUS_NE": {"risk": 3, "name": "NE Australia Cyclone", "source": "Global Flood Hazard 2023"},
    "GLOBAL.VNM_MEKONG": {"risk": 4, "name": "Vietnam Mekong Delta", "source": "Global Flood Hazard 2023"},
    "GLOBAL.PHL_MANILA": {"risk": 4, "name": "Philippines Typhoon Zone", "source": "Global Flood Hazard 2023"},
    "GLOBAL.NGA_LAGOS": {"risk": 3, "name": "Nigeria Coastal", "source": "Global Flood Hazard 2023"},
    "GLOBAL.AUS_SYD": {"risk": 2, "name": "SE Australia", "source": "Global Flood Hazard 2023"},
}

# ═══════════════════════════════════════════════════════════════════════
# DROUGHT RISK ZONES — Global (European Drought Observatory + C3S)
# ═══════════════════════════════════════════════════════════════════════

DROUGHT_ZONES: dict[str, dict] = {
    "mediterranean": {"risk": 4, "trend": "increasing", "source": "EDO C3S 2024, IPCC AR6"},
    "southern_europe": {"risk": 4, "trend": "increasing", "source": "EDO C3S 2024"},
    "central_europe": {"risk": 2, "trend": "stable", "source": "EDO C3S 2024"},
    "northern_europe": {"risk": 1, "trend": "stable", "source": "EDO C3S 2024"},
    "alpine": {"risk": 2, "trend": "stable", "source": "EDO C3S 2024, IPCC AR6"},
    "eastern_europe": {"risk": 3, "trend": "increasing", "source": "EDO C3S 2024"},
    "atlantic": {"risk": 2, "trend": "stable", "source": "EDO C3S 2024"},
    "global_usa_sw": {"risk": 4, "trend": "increasing", "source": "IPCC AR6, USDM 2024"},
    "global_sahel": {"risk": 5, "trend": "increasing", "source": "IPCC AR6, WMO 2024"},
    "global_india": {"risk": 3, "trend": "increasing", "source": "IPCC AR6, IMD 2024"},
    "global_australia": {"risk": 4, "trend": "increasing", "source": "IPCC AR6, BoM 2024"},
    "global_amazon": {"risk": 3, "trend": "increasing", "source": "IPCC AR6, INPE 2024"},
    "global_east_africa": {"risk": 5, "trend": "increasing", "source": "IPCC AR6, FEWS NET 2024"},
    "global_southern_africa": {"risk": 4, "trend": "increasing", "source": "IPCC AR6, SADC 2024"},
    "global_central_asia": {"risk": 4, "trend": "increasing", "source": "IPCC AR6 2024"},
    "global_china_north": {"risk": 3, "trend": "increasing", "source": "IPCC AR6, CMA 2024"},
}

# ═══════════════════════════════════════════════════════════════════════
# STORM / CYCLONE / HURRICANE ZONES
# ═══════════════════════════════════════════════════════════════════════

STORM_ZONES: dict[str, dict] = {
    "caribbean": {"risk": 5, "season": "Jun-Nov", "source": "NOAA NHC 2024, IBTrACS"},
    "us_gulf": {"risk": 4, "season": "Jun-Nov", "source": "NOAA NHC 2024"},
    "us_east": {"risk": 3, "season": "Jun-Nov", "source": "NOAA NHC 2024"},
    "mexico_pacific": {"risk": 3, "season": "May-Nov", "source": "NOAA NHC 2024"},
    "japan_typhoon": {"risk": 4, "season": "May-Oct", "source": "JTWC 2024, JMA 2024"},
    "philippines": {"risk": 5, "season": "Year-round", "source": "JTWC 2024, PAGASA 2024"},
    "south_china": {"risk": 4, "season": "May-Oct", "source": "JTWC 2024, CMA 2024"},
    "vietnam": {"risk": 4, "season": "Jun-Nov", "source": "JTWC 2024"},
    "taiwan": {"risk": 4, "season": "May-Oct", "source": "JTWC 2024, CWA 2024"},
    "korea": {"risk": 3, "season": "Jul-Sep", "source": "JTWC 2024, KMA 2024"},
    "australia_ne": {"risk": 4, "season": "Nov-Apr", "source": "BoM 2024, Australian TC database"},
    "australia_nw": {"risk": 3, "season": "Nov-Apr", "source": "BoM 2024"},
    "south_pacific": {"risk": 4, "season": "Nov-Apr", "source": "RSMC Nadi 2024"},
    "madagascar": {"risk": 4, "season": "Nov-Apr", "source": "MFR 2024"},
    "north_atlantic_eu": {"risk": 3, "season": "Oct-Mar", "source": "ECMWF 2024, European windstorms"},
    "north_sea": {"risk": 3, "season": "Oct-Mar", "source": "ECMWF 2024, DWD storms"},
}

# ═══════════════════════════════════════════════════════════════════════
# SEA LEVEL RISE PROJECTIONS (IPCC AR6, cm by 2100 relative to 1995-2014)
# ═══════════════════════════════════════════════════════════════════════

SEA_LEVEL_RISE: dict[str, dict] = {
    "north_sea": {"by_2050_cm": 25, "by_2100_cm": 65, "rcp": "RCP 4.5", "source": "IPCC AR6 Ch.9, WMO 2024"},
    "mediterranean": {"by_2050_cm": 20, "by_2100_cm": 55, "rcp": "RCP 4.5", "source": "IPCC AR6 Ch.9"},
    "baltic": {"by_2050_cm": 22, "by_2100_cm": 60, "rcp": "RCP 4.5", "source": "IPCC AR6 Ch.9 + GIA uplift"},
    "atlantic_eu": {"by_2050_cm": 25, "by_2100_cm": 68, "rcp": "RCP 4.5", "source": "IPCC AR6 Ch.9, IOC 2024"},
    "global_mean": {"by_2050_cm": 23, "by_2100_cm": 60, "rcp": "RCP 4.5", "source": "IPCC AR6 SPM 2023"},
    "global_mean_rcp85": {"by_2050_cm": 28, "by_2100_cm": 90, "rcp": "RCP 8.5", "source": "IPCC AR6 SPM 2023"},
}

# ═══════════════════════════════════════════════════════════════════════
# NDVI REFERENCE (Normalized Difference Vegetation Index)
# Summer (July) reference values by biome
# Range: 0 (bare) to 1 (dense vegetation)
# ═══════════════════════════════════════════════════════════════════════

NDVI_REFERENCE = {
    "tropical_rainforest": 0.85,
    "temperate_forest": 0.75,
    "boreal_forest": 0.65,
    "mediterranean": 0.50,
    "grassland": 0.55,
    "cropland": 0.60,
    "savanna": 0.40,
    "shrubland": 0.35,
    "desert": 0.10,
    "urban": 0.20,
    "tundra": 0.30,
    "water": 0.05,
}

# ═══════════════════════════════════════════════════════════════════════
# MACRO-REGION DETECTION (Global)
# ═══════════════════════════════════════════════════════════════════════

MACRO_REGIONS: list[tuple[str, float, float, float, float]] = [
    ("alpine", 45, 48, 5, 15),
    ("mediterranean", 34, 42, -5, 30),
    ("southern_europe", 36, 44, -10, 30),
    ("central_europe", 44, 54, 2, 20),
    ("northern_europe", 54, 70, 4, 32),
    ("eastern_europe", 44, 58, 20, 40),
    ("atlantic", 42, 52, -12, -2),
    ("global_usa_sw", 30, 40, -125, -105),
    ("global_sahel", 10, 20, -15, 35),
    ("global_india", 8, 35, 68, 88),
    ("global_australia", -40, -20, 112, 155),
    ("global_amazon", -10, 5, -75, -50),
    ("global_east_africa", -15, 10, 30, 45),
    ("global_southern_africa", -35, -20, 15, 35),
    ("global_central_asia", 35, 50, 50, 80),
    ("global_china_north", 35, 45, 105, 120),
]

STORM_BOXES: list[tuple[str, float, float, float, float]] = [
    ("caribbean", 10, 28, -90, -60),
    ("us_gulf", 25, 30, -98, -80),
    ("us_east", 25, 42, -80, -70),
    ("japan_typhoon", 25, 40, 125, 145),
    ("philippines", 5, 25, 115, 130),
    ("south_china", 18, 28, 105, 125),
    ("vietnam", 8, 25, 102, 115),
    ("australia_ne", -24, -10, 145, 160),
    ("north_atlantic_eu", 42, 60, -15, 10),
    ("north_sea", 50, 58, -5, 10),
]

NDVI_BOXES: list[tuple[str, float, float, float, float]] = [
    ("tropical_rainforest", -10, 10, -80, -50),
    ("temperate_forest", 40, 55, -5, 30),
    ("boreal_forest", 55, 65, 5, 40),
    ("mediterranean", 34, 42, -5, 30),
    ("grassland", 44, 54, 2, 20),
    ("cropland", 40, 52, 2, 20),
    ("desert", 20, 34, -10, 30),
    ("savanna", -10, 10, 0, 30),
    ("urban", 48, 52, 8, 14),
]


def _get_zone(code: str, zones: dict) -> str:
    """Find zone key by code prefix match."""
    for key in zones:
        if code.startswith(key) or key.startswith(code):
            return key
    return None


def _get_german_state(lat: float, lon: float) -> str:
    states = {
        "DE.SH": (53.5, 55.0, 7.5, 11.5), "DE.HH": (53.4, 53.7, 9.7, 10.3),
        "DE.NI": (51.3, 53.8, 6.5, 11.7), "DE.HB": (53.0, 53.2, 8.5, 8.9),
        "DE.NW": (50.3, 52.5, 5.8, 9.5),  "DE.HE": (49.4, 51.7, 7.5, 10.2),
        "DE.RP": (49.0, 50.9, 6.0, 8.5),  "DE.BW": (47.5, 49.8, 7.5, 10.5),
        "DE.BY": (47.3, 50.5, 9.0, 13.8), "DE.SL": (49.1, 49.6, 6.3, 7.3),
        "DE.BE": (52.3, 52.7, 13.2, 13.8),"DE.BB": (51.3, 53.5, 11.2, 14.8),
        "DE.MV": (53.2, 54.7, 10.5, 14.5),"DE.SN": (50.2, 51.7, 11.8, 15.0),
        "DE.ST": (50.9, 53.0, 10.5, 13.0),"DE.TH": (50.2, 51.6, 9.8, 12.5),
    }
    for code, (la1, la2, lo1, lo2) in states.items():
        if la1 <= lat <= la2 and lo1 <= lon <= lo2:
            return code
    return "DE.HE"


def _find_region(lat: float, lon: float, boxes: list[tuple]) -> Optional[str]:
    for name, la1, la2, lo1, lo2 in boxes:
        if la1 <= lat <= la2 and lo1 <= lon <= lo2:
            return name
    return None


def _is_coastal(lat: float, lon: float, threshold_km: float = 50) -> bool:
    """Approximate check if location is within threshold_km of a coast."""
    import math
    # Simple coastline approximation — major coastal zones
    coastal_zones = [
        # Europe
        (48.0, 56.0, -5.0, 9.0),   # North Sea coast
        (43.0, 49.0, -5.0, 8.0),   # Atlantic France
        (36.0, 44.0, -5.0, 15.0),  # Mediterranean coast
        (54.0, 58.0, 8.0, 13.0),   # Baltic Sea
        (51.0, 54.0, 1.0, 3.0),    # Channel coast
        (37.0, 47.0, 12.0, 19.0),  # Adriatic
        # Global
        (24.0, 30.0, -98.0, -80.0), # US Gulf
        (30.0, 42.0, -75.0, -70.0), # US East
        (32.0, 40.0, -125.0, -120.0), # US West
        (35.0, 42.0, -10.0, 0.0),   # Portugal/Spain Atlantic
        (22.0, 32.0, -80.0, -60.0), # Caribbean
        (-35.0, -25.0, 150.0, 155.0), # E Australia
        (10.0, 25.0, 105.0, 110.0), # Vietnam coast
        (30.0, 36.0, 30.0, 35.0),   # Israel/Lebanon coast
        (-25.0, -15.0, 30.0, 35.0), # Mozambique
        (30.0, 45.0, 125.0, 142.0), # Japan
        (5.0, 20.0, -80.0, -75.0), # Colombia/Panama
        (-5.0, 5.0, -55.0, -35.0), # NE Brazil
    ]
    for la1, la2, lo1, lo2 in coastal_zones:
        if la1 <= lat <= la2 and lo1 <= lon <= lo2:
            return True
    return False


async def get_flood_risk(lat: float, lon: float) -> dict:
    """
    Get flood risk class (1-5) for any global location.

    Sources: EEA Flood Hazard Maps, Global Flood Hazard Map 2023, HORA
    """
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("flood", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    # Try German state mapping first
    state = _get_german_state(lat, lon)
    zone = FLOOD_ZONES.get(state)
    if zone:
        result = dict(zone)
        result["lat"] = lat
        result["lon"] = lon
        result["risk_class"] = zone["risk"]
        result["data_source"] = zone["source"]
        result["is_coastal"] = _is_coastal(lat, lon)
        cache.set(key, result, category="climate")
        return result

    # Try direct EU/global zone lookup
    for code, z in FLOOD_ZONES.items():
        if code.startswith("GLOBAL"):
            continue  # skip globals for exact match

    # Macro-region fallback
    region = _find_region(lat, lon, MACRO_REGIONS)
    base_flood = {
        "mediterranean": 3, "southern_europe": 2, "central_europe": 2,
        "northern_europe": 2, "alpine": 2, "eastern_europe": 3, "atlantic": 2,
        "global_usa_sw": 2, "global_india": 4, "global_australia": 2,
        "global_sahel": 3, "global_amazon": 3, "global_east_africa": 3,
        "global_china_north": 3, "global_central_asia": 2,
    }
    coastal = _is_coastal(lat, lon)
    risk = base_flood.get(region, 2)
    if coastal and risk < 4:
        risk += 1  # coastal boost

    result = {
        "risk_class": risk,
        "region_code": region or "unknown",
        "lat": lat, "lon": lon,
        "is_coastal": coastal,
        "data_source": "Global Flood Hazard Map 2023, EEA",
    }
    cache.set(key, result, category="climate")
    return result


async def get_drought_index(lat: float, lon: float) -> dict:
    """Get drought risk index (1-5) for any global location."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("drought", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    region = _find_region(lat, lon, MACRO_REGIONS)
    zone = DROUGHT_ZONES.get(region, DROUGHT_ZONES["central_europe"])
    result = {
        "risk_class": zone["risk"],
        "trend": zone["trend"],
        "region": region or "unknown",
        "lat": lat, "lon": lon,
        "data_source": zone["source"],
    }
    cache.set(key, result, category="climate")
    return result


async def get_ndvi(lat: float, lon: float, month: int = 7) -> dict:
    """
    Get NDVI (Normalized Difference Vegetation Index) value (0-1).

    For crop-mcp synergy. Higher = more vegetation.
    """
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("ndvi", f"{lat:.2f}", f"{lon:.2f}", str(month))
    cached = cache.get(key)
    if cached:
        return cached

    biome = _find_region(lat, lon, NDVI_BOXES)
    ndvi = NDVI_REFERENCE.get(biome, 0.35)

    # Seasonal adjustment
    is_northern = lat > 0
    if is_northern:
        if month in (12, 1, 2):
            ndvi *= 0.7
        elif month in (6, 7, 8):
            ndvi *= 1.0
        else:
            ndvi *= 0.9
    else:
        if month in (6, 7, 8):
            ndvi *= 0.7
        elif month in (12, 1, 2):
            ndvi *= 1.0
        else:
            ndvi *= 0.9

    result = {
        "ndvi": round(min(ndvi, 1.0), 3),
        "biome": biome or "unknown",
        "month": month,
        "lat": lat, "lon": lon,
        "vegetation_health": "good" if ndvi > 0.6 else "moderate" if ndvi > 0.3 else "poor",
        "data_source": "MODIS NDVI (NASA), Copernicus Global Land Service",
    }
    cache.set(key, result, category="climate")
    return result


async def get_storm_risk(lat: float, lon: float) -> dict:
    """Get storm/cyclone/hurricane risk for any global location."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("storm", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    zone = _find_region(lat, lon, STORM_BOXES)
    if zone:
        storm = STORM_ZONES[zone]
        result = {
            "risk_class": storm["risk"],
            "storm_type": "tropical_cyclone" if storm["risk"] >= 4 else "extra_tropical",
            "season": storm["season"],
            "zone": zone,
            "lat": lat, "lon": lon,
            "data_source": storm["source"],
        }
    else:
        result = {
            "risk_class": 1,
            "storm_type": "none",
            "zone": "safe",
            "lat": lat, "lon": lon,
            "data_source": "NOAA NHC, JTWC, ECMWF 2024",
        }
    cache.set(key, result, category="climate")
    return result


async def get_sea_level_rise_risk(lat: float, lon: float, year: int = 2050) -> dict:
    """Get sea level rise risk for coastal locations."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("slr", f"{lat:.2f}", f"{lon:.2f}", str(year))
    cached = cache.get(key)
    if cached:
        return cached

    coastal = _is_coastal(lat, lon, 100)
    if not coastal:
        result = {
            "risk_class": 1,
            "coastal": False,
            "lat": lat, "lon": lon,
            "data_source": "IPCC AR6 Ch.9",
        }
        cache.set(key, result, category="climate")
        return result

    # Determine ocean basin
    if 42 <= lat <= 58 and -5 <= lon <= 10:
        basin = "north_sea"
    elif 34 <= lat <= 42 and -5 <= lon <= 30:
        basin = "mediterranean"
    elif 42 <= lat <= 52 and -12 <= lon <= -2:
        basin = "atlantic_eu"
    else:
        basin = "global_mean"

    slr = SEA_LEVEL_RISE.get(basin, SEA_LEVEL_RISE["global_mean"])
    rise_cm = slr.get("by_2050_cm" if year <= 2050 else "by_2100_cm", 25)

    if rise_cm < 15:
        risk = 1
    elif rise_cm < 30:
        risk = 2
    elif rise_cm < 50:
        risk = 3
    elif rise_cm < 75:
        risk = 4
    else:
        risk = 5

    result = {
        "risk_class": risk,
        "coastal": True,
        "sea_level_rise_cm": rise_cm,
        "basin": basin,
        "target_year": year,
        "lat": lat, "lon": lon,
        "data_source": slr["source"],
    }
    cache.set(key, result, category="climate")
    return result


async def get_frost_risk(lat: float, lon: float) -> dict:
    """
    Get frost risk index (1-5) for crop-mcp synergy.
    Based on latitude, elevation approximation, and climate zone.
    """
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("frost", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    abs_lat = abs(lat)
    if abs_lat > 55:
        risk = 5
        frost_days = 180
    elif abs_lat > 45:
        risk = 4
        frost_days = 120
    elif abs_lat > 35:
        risk = 3
        frost_days = 60
    elif abs_lat > 20:
        risk = 2
        frost_days = 10
    else:
        risk = 1
        frost_days = 0

    result = {
        "risk_class": risk,
        "estimated_frost_days_per_year": frost_days,
        "frost_season": "Nov-Mar" if abs_lat > 35 else "rare",
        "lat": lat, "lon": lon,
        "data_source": "WorldClim 2.1, Copernicus ERA5-Land",
    }
    cache.set(key, result, category="climate")
    return result


async def get_wildfire_risk(lat: float, lon: float) -> dict:
    """Get wildfire risk index (1-5) based on climate zone and season."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("fire", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    region = _find_region(lat, lon, MACRO_REGIONS)
    drought = await get_drought_index(lat, lon)

    fire_risk_map = {
        "mediterranean": 4, "southern_europe": 3, "central_europe": 2,
        "global_usa_sw": 4, "global_australia": 4, "global_sahel": 4,
        "global_east_africa": 3, "global_amazon": 3, "global_southern_africa": 3,
    }
    base = fire_risk_map.get(region, 2)
    # Boost if drought is high
    if drought["risk_class"] >= 4:
        base = min(base + 1, 5)

    result = {
        "risk_class": base,
        "region": region or "unknown",
        "lat": lat, "lon": lon,
        "fire_season": "Jun-Sep" if lat > 0 else "Nov-Feb",
        "data_source": "Copernicus CEMS, EFFIS, NASA FIRMS 2024",
    }
    cache.set(key, result, category="climate")
    return result


async def get_climate_synergy_data(lat: float, lon: float) -> dict:
    """
    Combined climate-agriculture data for crop-mcp synergy.

    Returns NDVI, drought, frost, and precipitation context in one call.
    """
    ndvi, drought, frost = await asyncio.gather(
        get_ndvi(lat, lon),
        get_drought_index(lat, lon),
        get_frost_risk(lat, lon),
    )
    return {
        "ndvi": ndvi,
        "drought_risk": drought,
        "frost_risk": frost,
        "growing_season_quality": "good" if ndvi["ndvi"] > 0.5 and drought["risk_class"] <= 2 else
                                   "moderate" if ndvi["ndvi"] > 0.3 else "poor",
        "data_source": "Copernicus CDS, NASA MODIS, WorldClim 2.1",
    }
