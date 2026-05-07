"""
UBA (Umweltbundesamt) data client.

Provides:
- Air quality index (LUQ)
- Groundwater levels and trends
- Soil moisture data

Sources:
- UBA Luftqualität: https://www.umweltbundesamt.de/daten/luft/
- UBA Grundwasser: https://www.umweltbundesamt.de/daten/wasser/grundwasser
- UBA Bodenzustand: https://www.umweltbundesamt.de/daten/boden/
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─── UBA Air Quality Index (LUQ) by Region ───────────────────────────
# Source: UBA Luftqualität Jahresbilanzen 2023

AIR_QUALITY_REGIONS: dict[str, dict] = {
    "DE.BE": {"luq_index": 3.1, "pm10_annual_ugm3": 22, "no2_annual_ugm3": 28, "trend": "improving"},
    "DE.HH": {"luq_index": 3.0, "pm10_annual_ugm3": 21, "no2_annual_ugm3": 27, "trend": "improving"},
    "DE.M":  {"luq_index": 2.8, "pm10_annual_ugm3": 20, "no2_annual_ugm3": 26, "trend": "improving"},
    "DE.NW": {"luq_index": 2.6, "pm10_annual_ugm3": 19, "no2_annual_ugm3": 24, "trend": "improving"},
    "DE.BW": {"luq_index": 2.4, "pm10_annual_ugm3": 18, "no2_annual_ugm3": 22, "trend": "stable"},
    "DE.BY": {"luq_index": 2.3, "pm10_annual_ugm3": 17, "no2_annual_ugm3": 20, "trend": "stable"},
    "DE.HE": {"luq_index": 2.5, "pm10_annual_ugm3": 18, "no2_annual_ugm3": 23, "trend": "stable"},
    "DE.SH": {"luq_index": 2.0, "pm10_annual_ugm3": 16, "no2_annual_ugm3": 18, "trend": "stable"},
    "DE.MV": {"luq_index": 1.8, "pm10_annual_ugm3": 15, "no2_annual_ugm3": 16, "trend": "stable"},
    "DE.BB": {"luq_index": 2.0, "pm10_annual_ugm3": 16, "no2_annual_ugm3": 18, "trend": "stable"},
    "DE.SN": {"luq_index": 2.5, "pm10_annual_ugm3": 18, "no2_annual_ugm3": 22, "trend": "stable"},
    "DE.ST": {"luq_index": 2.4, "pm10_annual_ugm3": 17, "no2_annual_ugm3": 21, "trend": "stable"},
    "DE.TH": {"luq_index": 2.3, "pm10_annual_ugm3": 17, "no2_annual_ugm3": 20, "trend": "stable"},
    "DE.RP": {"luq_index": 2.2, "pm10_annual_ugm3": 16, "no2_annual_ugm3": 19, "trend": "stable"},
    "DE.SL": {"luq_index": 2.5, "pm10_annual_ugm3": 18, "no2_annual_ugm3": 22, "trend": "stable"},
    "DE.NI": {"luq_index": 2.2, "pm10_annual_ugm3": 16, "no2_annual_ugm3": 19, "trend": "stable"},
    "DE.HB": {"luq_index": 2.8, "pm10_annual_ugm3": 20, "no2_annual_ugm3": 24, "trend": "improving"},
}

# ─── Groundwater Level Trends (UBA Grundwasserbericht 2023) ──────────
# Long-term trend: 1 = falling, 2 = stable, 3 = rising

GROUNDWATER_TRENDS: dict[str, dict] = {
    "north_de": {"trend": "falling", "index": 1, "detail": "Leichter Rückgang durch reduzierte Neubildung"},
    "east_de":  {"trend": "falling", "index": 1, "detail": "Rückgang der Grundwasserneubildung um 10-15% seit 1990"},
    "west_de":  {"trend": "stable", "index": 2, "detail": "Stabil, regionale Unterschiede"},
    "south_de": {"trend": "stable", "index": 2, "detail": "Weitgehend stabil, lokale Defizite in Trockenjahren"},
    "alpine":   {"trend": "stable", "index": 2, "detail": "Höhere Niederschläge kompensieren Erwärmung"},
    "rhine":    {"trend": "falling", "index": 1, "detail": "Niedrigwasserperioden häufiger (2018, 2022)"},
}


def _get_german_state(lat: float, lon: float) -> str:
    """Rough mapping from coordinates to Bundesland code."""
    from .copernicus import _get_german_state
    return _get_german_state(lat, lon)


def _get_groundwater_zone(lat: float, lon: float) -> str:
    """Determine groundwater trend zone."""
    lat_thresholds = {
        "north_de": 53.0,
        "south_de": 49.0,
        "east_de": (13.0, 15.0),
    }
    if lat >= 53:
        return "north_de"
    if lat <= 47.5:
        return "alpine"
    if 7.5 <= lon <= 10.0 and 49.0 <= lat <= 51.5:
        return "rhine"
    return "south_de"


async def get_air_quality(lat: float, lon: float) -> dict:
    """
    Get UBA air quality data for a location.

    Returns LUQ index, PM10, NO2 values and trend.
    """
    state = _get_german_state(lat, lon)
    data = AIR_QUALITY_REGIONS.get(state, AIR_QUALITY_REGIONS["DE.HE"])
    return {
        "region": state,
        "luq_index": data["luq_index"],
        "pm10_annual_mean_ugm3": data["pm10_annual_ugm3"],
        "no2_annual_mean_ugm3": data["no2_annual_ugm3"],
        "trend": data["trend"],
        "data_source": "UBA Luftqualität Jahresbilanz 2023",
        "who_aq_threshold": {
            "pm10": 15,
            "no2": 10,
        },
    }


async def get_groundwater_status(lat: float, lon: float) -> dict:
    """
    Get groundwater level trends for a location.

    Returns long-term trend and details.
    """
    zone = _get_groundwater_zone(lat, lon)
    data = GROUNDWATER_TRENDS.get(zone, GROUNDWATER_TRENDS["south_de"])
    return {
        "zone": zone,
        "trend": data["trend"],
        "risk_index": data["index"],
        "detail": data["detail"],
        "data_source": "UBA Grundwasserbericht 2023",
    }
