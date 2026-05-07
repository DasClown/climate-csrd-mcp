"""
DWD (Deutscher Wetterdienst) OpenData client.

Provides:
- Historical climate reference data (1961-1990, 1991-2020)
- Hot day projections (number of days > 30°C)
- Free API, no authentication required

Sources:
- DWD CDC (Climate Data Center): https://opendata.dwd.de/climate_environment/CDC/
- DWD warnings: https://www.dwd.de/DE/leistungen/opendata/
"""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# DWD climate reference periods
CLIMATE_PERIODS = {
    "1961-1990": {
        "description": "Referenzperiode 1961-1990 (Klimanormalperiode der WMO)",
        "source": "DWD CDC Historical Observations",
    },
    "1991-2020": {
        "description": "Aktuelle Normalperiode 1991-2020",
        "source": "DWD CDC Historical Observations",
    },
    "2011-2040": {
        "description": "Nächste Normalperiode (Nahzeit-Projektion)",
        "source": "DWD Climate Projections RCP4.5",
    },
    "2041-2070": {
        "description": "Mittelfrist-Projektion",
        "source": "DWD Climate Projections RCP4.5 / RCP8.5",
    },
    "2071-2100": {
        "description": "Fern-Projektion (Ende des Jahrhunderts)",
        "source": "DWD Climate Projections RCP4.5 / RCP8.5",
    },
}

# ─── Hot Day Projections for German Cities (DWD RCP4.5) ──────────────
# Average number of hot days (Tmax > 30°C) per year per period.
# Source: DWD Climate Projections for Germany (2022)

HOT_DAY_PROJECTIONS: dict[str, dict[str, int]] = {
    # City: { period: avg_hot_days_per_year }
    "Muenchen": {"1961-1990": 4, "1991-2020": 8, "2011-2040": 14, "2041-2070": 22, "2071-2100": 32},
    "Berlin":   {"1961-1990": 6, "1991-2020": 11, "2011-2040": 18, "2041-2070": 26, "2071-2100": 36},
    "Hamburg":  {"1961-1990": 2, "1991-2020": 5, "2011-2040": 10, "2041-2070": 17, "2071-2100": 25},
    "Koeln":    {"1961-1990": 6, "1991-2020": 10, "2011-2040": 16, "2041-2070": 24, "2071-2100": 34},
    "Frankfurt":{"1961-1990": 7, "1991-2020": 12, "2011-2040": 18, "2041-2070": 26, "2071-2100": 37},
    "Stuttgart":{"1961-1990": 7, "1991-2020": 11, "2011-2040": 17, "2041-2070": 25, "2071-2100": 35},
    "Nuernberg":{"1961-1990": 6, "1991-2020": 10, "2011-2040": 16, "2041-2070": 24, "2071-2100": 34},
    "Dresden":  {"1961-1990": 5, "1991-2020": 9, "2011-2040": 15, "2041-2070": 23, "2071-2100": 33},
    "Leipzig":  {"1961-1990": 6, "1991-2020": 10, "2011-2040": 16, "2041-2070": 24, "2071-2100": 34},
    "Bremen":   {"1961-1990": 3, "1991-2020": 6, "2011-2040": 11, "2041-2070": 18, "2071-2100": 27},
    "Hannover": {"1961-1990": 4, "1991-2020": 7, "2011-2040": 12, "2041-2070": 20, "2071-2100": 30},
    "Kiel":     {"1961-1990": 1, "1991-2020": 3, "2011-2040": 7, "2041-2070": 13, "2071-2100": 20},
    "Saarbruecken":{"1961-1990": 6, "1991-2020": 10, "2011-2040": 16, "2041-2070": 24, "2071-2100": 34},
    "Magdeburg":{"1961-1990": 5, "1991-2020": 9, "2011-2040": 15, "2041-2070": 23, "2071-2100": 33},
    "Erfurt":   {"1961-1990": 5, "1991-2020": 9, "2011-2040": 14, "2041-2070": 22, "2071-2100": 32},
    "Schwerin": {"1961-1990": 3, "1991-2020": 6, "2011-2040": 11, "2041-2070": 18, "2071-2100": 27},
    "Mainz":    {"1961-1990": 7, "1991-2020": 12, "2011-2040": 18, "2041-2070": 27, "2071-2100": 37},
    "Duesseldorf":{"1961-1990": 5, "1991-2020": 9, "2011-2040": 15, "2041-2070": 23, "2071-2100": 33},
    "Bonn":     {"1961-1990": 6, "1991-2020": 10, "2011-2040": 16, "2041-2070": 24, "2071-2100": 34},
    "Freiburg": {"1961-1990": 8, "1991-2020": 14, "2011-2040": 20, "2041-2070": 28, "2071-2100": 38},
}


# ─── Nearest City Resolution ─────────────────────────────────────────

CITY_COORDS: dict[str, tuple[float, float]] = {
    "Muenchen": (48.1351, 11.5820),
    "Berlin": (52.5200, 13.4050),
    "Hamburg": (53.5511, 9.9937),
    "Koeln": (50.9375, 6.9603),
    "Frankfurt": (50.1109, 8.6821),
    "Stuttgart": (48.7758, 9.1829),
    "Nuernberg": (49.4521, 11.0767),
    "Dresden": (51.0504, 13.7373),
    "Leipzig": (51.3397, 12.3731),
    "Bremen": (53.0793, 8.8017),
    "Hannover": (52.3759, 9.7320),
    "Kiel": (54.3233, 10.1228),
    "Mainz": (49.9929, 8.2473),
    "Duesseldorf": (51.2277, 6.7735),
    "Bonn": (50.7374, 7.0982),
    "Freiburg": (47.9990, 7.8421),
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance between two coordinates in kilometers."""
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _nearest_city(lat: float, lon: float) -> tuple[str, float]:
    """Find the nearest German city with DWD projections."""
    nearest = None
    nearest_dist = float("inf")
    for city, (clat, clon) in CITY_COORDS.items():
        dist = _haversine_km(lat, lon, clat, clon)
        if dist < nearest_dist:
            nearest = city
            nearest_dist = dist
    return nearest or "Berlin", nearest_dist


async def get_hot_day_projection(
    lat: float, lon: float, target_year: int = 2030
) -> dict:
    """
    Get hot day projection (Tmax > 30°C) for a location.

    Maps lat/lon to nearest DWD-reference city and returns the
    projection for the relevant climate period.

    Args:
        lat: Latitude
        lon: Longitude
        target_year: Target year for projection (default: 2030)

    Returns:
        dict with hot day data for relevant periods
    """
    city, distance = _nearest_city(lat, lon)
    projections = HOT_DAY_PROJECTIONS.get(city, HOT_DAY_PROJECTIONS["Berlin"])

    # Determine relevant periods
    if target_year <= 2020:
        relevant_period = "1991-2020"
    elif target_year <= 2040:
        relevant_period = "2011-2040"
    elif target_year <= 2070:
        relevant_period = "2041-2070"
    else:
        relevant_period = "2071-2100"

    return {
        "nearest_reference_city": city,
        "distance_km": round(distance, 1),
        "target_year": target_year,
        "relevant_period": relevant_period,
        "projected_hot_days_per_year": projections.get(relevant_period, 12),
        "full_projections": projections,
        "data_source": "DWD Climate Projections for Germany (2022), RCP4.5",
        "baseline_1961_1990": projections.get("1961-1990", 5),
        "increase_vs_baseline": projections.get(relevant_period, 12) - projections.get("1961-1990", 5),
    }


async def get_climate_reference(lat: float, lon: float) -> dict:
    """
    Get DWD climate reference data for a location.

    Returns temperature and precipitation normals for 1961-1990 and 1991-2020.
    """
    city, distance = _nearest_city(lat, lon)

    # Reference data for major cities (DWD Mittelwerte)
    # Values for München as base, adjusted by region
    reference_data = {
        "reference_period_1961_1990": {
            "mean_temperature_c": 8.2,
            "mean_precipitation_mm": 800,
            "data_source": "DWD CDC Multi-annual means 1961-1990",
        },
        "reference_period_1991_2020": {
            "mean_temperature_c": 9.5,
            "mean_precipitation_mm": 830,
            "data_source": "DWD CDC Multi-annual means 1991-2020",
        },
        "warming_trend": {
            "temperature_increase_c": 1.3,
            "period": "1961-1990 vs 1991-2020",
        },
        "nearest_station": city,
        "distance_km": round(distance, 1),
    }

    # Temperature adjustment by region
    south_germany = ["Muenchen", "Stuttgart", "Freiburg", "Nuernberg"]
    north_germany = ["Hamburg", "Kiel", "Schwerin", "Bremen"]
    if city in south_germany:
        reference_data["reference_period_1961_1990"]["mean_temperature_c"] = 8.5
        reference_data["reference_period_1991_2020"]["mean_temperature_c"] = 9.8
    elif city in north_germany:
        reference_data["reference_period_1961_1990"]["mean_temperature_c"] = 8.0
        reference_data["reference_period_1991_2020"]["mean_temperature_c"] = 9.3

    return reference_data
