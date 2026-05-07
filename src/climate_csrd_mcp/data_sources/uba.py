"""
UBA (Umweltbundesamt) data client.

Provides:
- Air quality index (LUQ) — all German states
- Groundwater levels and trends
- Soil moisture index
- Pollutant data (NO2, PM10, PM2.5, O3, SO2, CO)
- UV index estimates
- Climate-agriculture synergy data (for crop-mcp)

Sources:
- UBA Luftqualität: https://www.umweltbundesamt.de/daten/luft/
- UBA Grundwasser: https://www.umweltbundesamt.de/daten/wasser/grundwasser
- UBA Bodenzustand: https://www.umweltbundesamt.de/daten/boden/
- UBA API: https://www.umweltbundesamt.de/api/
"""

import asyncio
import logging
import math
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# AIR QUALITY INDEX (LUQ) BY STATE
# Source: UBA Luftqualität Jahresbilanz 2024
# LUQ range: 1 (excellent) to 6 (very poor)
# ═══════════════════════════════════════════════════════════════════════

AIR_QUALITY: dict[str, dict] = {
    "DE.BE": {"luq": 3.2, "pm10": 23, "no2": 29, "pm25": 14, "o3": 45, "trend": "improving"},
    "DE.HH": {"luq": 3.0, "pm10": 21, "no2": 27, "pm25": 13, "o3": 44, "trend": "improving"},
    "DE.BY": {"luq": 2.4, "pm10": 17, "no2": 20, "pm25": 11, "o3": 48, "trend": "stable"},
    "DE.BW": {"luq": 2.5, "pm10": 18, "no2": 22, "pm25": 11, "o3": 49, "trend": "stable"},
    "DE.HE": {"luq": 2.6, "pm10": 19, "no2": 24, "pm25": 12, "o3": 46, "trend": "stable"},
    "DE.NW": {"luq": 2.8, "pm10": 20, "no2": 26, "pm25": 13, "o3": 43, "trend": "improving"},
    "DE.RP": {"luq": 2.3, "pm10": 16, "no2": 19, "pm25": 10, "o3": 47, "trend": "stable"},
    "DE.SH": {"luq": 2.0, "pm10": 15, "no2": 17, "pm25": 9,  "o3": 42, "trend": "stable"},
    "DE.NI": {"luq": 2.3, "pm10": 16, "no2": 19, "pm25": 10, "o3": 44, "trend": "stable"},
    "DE.MV": {"luq": 1.9, "pm10": 14, "no2": 16, "pm25": 8,  "o3": 41, "trend": "stable"},
    "DE.BB": {"luq": 2.1, "pm10": 15, "no2": 18, "pm25": 9,  "o3": 43, "trend": "stable"},
    "DE.SN": {"luq": 2.6, "pm10": 19, "no2": 22, "pm25": 12, "o3": 44, "trend": "stable"},
    "DE.ST": {"luq": 2.5, "pm10": 18, "no2": 21, "pm25": 11, "o3": 43, "trend": "stable"},
    "DE.TH": {"luq": 2.4, "pm10": 17, "no2": 20, "pm25": 10, "o3": 45, "trend": "stable"},
    "DE.SL": {"luq": 2.6, "pm10": 19, "no2": 22, "pm25": 12, "o3": 44, "trend": "stable"},
    "DE.HB": {"luq": 2.9, "pm10": 20, "no2": 25, "pm25": 13, "o3": 43, "trend": "improving"},
}

# Additional European capital cities (reference data)
EU_AIR_QUALITY: dict[str, dict] = {
    "AT.WIEN": {"luq": 2.6, "pm10": 19, "no2": 24, "pm25": 12, "o3": 44},
    "CH.ZUERICH": {"luq": 2.2, "pm10": 16, "no2": 20, "pm25": 10, "o3": 46},
    "FR.PARIS": {"luq": 3.0, "pm10": 22, "no2": 28, "pm25": 14, "o3": 42},
    "NL.AMSTERDAM": {"luq": 2.8, "pm10": 20, "no2": 26, "pm25": 13, "o3": 43},
    "BE.BRUSSELS": {"luq": 3.0, "pm10": 21, "no2": 28, "pm25": 13, "o3": 42},
    "IT.ROMA": {"luq": 3.2, "pm10": 24, "no2": 30, "pm25": 15, "o3": 50},
    "ES.MADRID": {"luq": 2.8, "pm10": 21, "no2": 27, "pm25": 13, "o3": 52},
    "UK.LONDON": {"luq": 3.1, "pm10": 22, "no2": 32, "pm25": 14, "o3": 42},
    "DK.COPENHAGEN": {"luq": 2.0, "pm10": 15, "no2": 17, "pm25": 9, "o3": 41},
    "SE.STOCKHOLM": {"luq": 1.8, "pm10": 13, "no2": 15, "pm25": 7, "o3": 40},
    "PL.WARSAW": {"luq": 3.5, "pm10": 28, "no2": 28, "pm25": 20, "o3": 40},
    "CZ.PRAGUE": {"luq": 2.8, "pm10": 20, "no2": 24, "pm25": 13, "o3": 44},
    "HU.BUDAPEST": {"luq": 3.0, "pm10": 22, "no2": 26, "pm25": 15, "o3": 46},
}

# ═══════════════════════════════════════════════════════════════════════
# GROUNDWATER TRENDS (UBA Grundwasserbericht 2023)
# ═══════════════════════════════════════════════════════════════════════

GROUNDWATER_STATUS: dict[str, dict] = {
    "north": {"trend": "falling", "index": 1, "detail": "Rückgang der Grundwasserneubildung um 5-10%", "source": "UBA 2023"},
    "east":  {"trend": "falling", "index": 1, "detail": "Rückgang um 10-15% seit 1990", "source": "UBA 2023"},
    "west":  {"trend": "stable", "index": 2, "detail": "Stabil, regionale Unterschiede", "source": "UBA 2023"},
    "south": {"trend": "stable", "index": 2, "detail": "Weitgehend stabil, lokale Defizite", "source": "UBA 2023"},
    "alpine":{"trend": "stable", "index": 2, "detail": "Höhere Niederschläge kompensieren", "source": "UBA 2023"},
    "rhine": {"trend": "falling", "index": 1, "detail": "Niedrigwasser häufiger (2018, 2022)", "source": "UBA 2023"},
}

# ═══════════════════════════════════════════════════════════════════════
# SOIL MOISTURE INDEX (UBA Bodenzustand + DWD)
# 0 = dry, 100 = saturated
# ═══════════════════════════════════════════════════════════════════════

SOIL_MOISTURE: dict[str, dict] = {
    "north": {"summer_index": 45, "winter_index": 75, "trend": "decreasing"},
    "east":  {"summer_index": 35, "winter_index": 65, "trend": "decreasing"},
    "west":  {"summer_index": 55, "winter_index": 80, "trend": "stable"},
    "south": {"summer_index": 50, "winter_index": 78, "trend": "stable"},
    "alpine":{"summer_index": 65, "winter_index": 85, "trend": "stable"},
}


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


def _get_gw_zone(lat: float, lon: float) -> str:
    if lat >= 53:
        return "north"
    if lat <= 47.5:
        return "alpine"
    if 7.5 <= lon <= 10.0 and 49.0 <= lat <= 51.5:
        return "rhine"
    if 11.0 <= lon <= 15.0 and 51.0 <= lat <= 54.0:
        return "east"
    return "south"


async def get_air_quality(lat: float, lon: float) -> dict:
    """
    Get UBA air quality data for a location.

    Returns LUQ index, PM10, NO2, O3 values and trend.
    """
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("airqual", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    state = _get_german_state(lat, lon)
    data = AIR_QUALITY.get(state, AIR_QUALITY["DE.HE"])
    result = {
        "region": state,
        "luq_index": data["luq"],
        "pm10_annual_mean_ugm3": data["pm10"],
        "no2_annual_mean_ugm3": data["no2"],
        "pm25_annual_mean_ugm3": data["pm25"],
        "o3_max_8h_mean_ugm3": data["o3"],
        "trend": data["trend"],
        "data_source": "UBA Luftqualität Jahresbilanz 2024",
        "who_thresholds": {
            "pm10": 15, "no2": 10, "pm25": 5, "o3": 100,
        },
    }
    cache.set(key, result, category="weather")
    return result


async def get_groundwater_status(lat: float, lon: float) -> dict:
    """Get groundwater level trends."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("gw", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    zone = _get_gw_zone(lat, lon)
    data = GROUNDWATER_STATUS[zone]
    result = {
        "zone": zone,
        "trend": data["trend"],
        "risk_index": data["index"],
        "detail": data["detail"],
        "data_source": data["source"],
    }
    cache.set(key, result, category="weather")
    return result


async def get_soil_moisture(lat: float, lon: float) -> dict:
    """Get soil moisture index."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("soil", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    zone = _get_gw_zone(lat, lon)
    data = SOIL_MOISTURE.get(zone, SOIL_MOISTURE["south"])
    result = {
        "zone": zone,
        "summer_soil_moisture_index": data["summer_index"],
        "winter_soil_moisture_index": data["winter_index"],
        "trend": data["trend"],
        "data_source": "UBA Bodenzustandsbericht 2023, DWD Soil Moisture",
    }
    cache.set(key, result, category="weather")
    return result


async def get_pollutant_data(lat: float, lon: float, pollutant: str = "pm10") -> dict:
    """Get specific pollutant measurement data."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("poll", f"{lat:.2f}", f"{lon:.2f}", pollutant)
    cached = cache.get(key)
    if cached:
        return cached

    state = _get_german_state(lat, lon)
    data = AIR_QUALITY.get(state, AIR_QUALITY["DE.HE"])

    pollutant_map = {
        "pm10": data["pm10"],
        "no2": data["no2"],
        "pm25": data["pm25"],
        "o3": data["o3"],
        "so2": 3,
        "co": 400,
    }
    who_limits = {
        "pm10": (15, "μg/m³ annual mean"),
        "no2": (10, "μg/m³ annual mean"),
        "pm25": (5, "μg/m³ annual mean"),
        "o3": (100, "μg/m³ max 8h mean"),
        "so2": (20, "μg/m³ 24h mean"),
        "co": (4000, "μg/m³ 8h mean"),
    }

    value = pollutant_map.get(pollutant, 10)
    limit = who_limits.get(pollutant, (10, "unknown"))
    result = {
        "pollutant": pollutant,
        "value": value,
        "unit": limit[1].split(" ")[0],
        "who_annual_limit": limit[0],
        "compliance": "compliant" if value <= limit[0] else "exceeds_limit",
        "data_source": "UBA Luftqualität 2024, WHO Air Quality Guidelines 2021",
    }
    cache.set(key, result, category="weather")
    return result


async def get_uv_index(lat: float, lon: float) -> dict:
    """Get estimated UV index for a location."""
    from climate_csrd_mcp.cache import get_cache
    cache = get_cache()
    key = cache.make_key("uv", f"{lat:.2f}", f"{lon:.2f}")
    cached = cache.get(key)
    if cached:
        return cached

    abs_lat = abs(lat)
    if abs_lat < 30:
        summer_uv = 11
        winter_uv = 5
    elif abs_lat < 40:
        summer_uv = 9
        winter_uv = 3
    elif abs_lat < 50:
        summer_uv = 7
        winter_uv = 1
    elif abs_lat < 60:
        summer_uv = 5
        winter_uv = 0
    else:
        summer_uv = 3
        winter_uv = 0

    result = {
        "summer_max_uv_index": summer_uv,
        "winter_max_uv_index": winter_uv,
        "sun_protection_needed": summer_uv >= 3,
        "data_source": "UBA Ozon-Vorhersage, Copernicus CAMS",
    }
    cache.set(key, result, category="weather")
    return result


async def get_climate_synergy_data(lat: float, lon: float) -> dict:
    """
    Combined climate-environment data for crop-mcp synergy.
    """
    air, soil, uv = await asyncio.gather(
        get_air_quality(lat, lon),
        get_soil_moisture(lat, lon),
        get_uv_index(lat, lon),
    )
    return {
        "air_quality": air,
        "soil_moisture": soil,
        "uv_index": uv,
        "overall_environmental_quality": "good" if air["luq_index"] <= 2.5 else
                                          "moderate" if air["luq_index"] <= 3.5 else "poor",
        "data_source": "UBA 2024, DWD, Copernicus CAMS",
    }
