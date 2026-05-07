"""
EU Emissions Trading System (ETS) benchmarks.

Provides sector-level emission benchmarks from the EU ETS,
including average emissions, top/bottom 10% thresholds, and
historical trends.

Sources:
- EU ETS (European Union Transaction Log - EUTL)
- European Commission: Benchmark curves for free allocation
- AWS Open Data registry for ETS data
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─── EU ETS Benchmark Data (2021-2025 Phase IV) ──────────────────────
# Source: European Commission Implementing Regulation 2021/447
# Units: t CO₂ / t product unless noted

EU_ETS_BENCHMARKS_2024: dict[str, dict] = {
    "power_generation": {
        "sector_name": "Stromerzeugung (fossil)",
        "unit": "kg CO₂/kWh",
        "average": 0.45,
        "top_10_pct": 0.25,
        "bottom_10_pct": 0.72,
        "best_available_technology": 0.18,
        "trend_2020_2025": -0.08,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "cement": {
        "sector_name": "Zementklinker",
        "unit": "t CO₂/t clinker",
        "average": 0.78,
        "top_10_pct": 0.60,
        "bottom_10_pct": 0.95,
        "best_available_technology": 0.50,
        "trend_2020_2025": -0.03,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "steel": {
        "sector_name": "Roheisen / Stahl (integriert)",
        "unit": "t CO₂/t steel",
        "average": 1.85,
        "top_10_pct": 1.30,
        "bottom_10_pct": 2.40,
        "best_available_technology": 1.00,
        "trend_2020_2025": -0.05,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "refineries": {
        "sector_name": "Mineralölraffinerien",
        "unit": "t CO₂/t feedstock",
        "average": 0.45,
        "top_10_pct": 0.32,
        "bottom_10_pct": 0.60,
        "best_available_technology": 0.25,
        "trend_2020_2025": -0.04,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "chemicals": {
        "sector_name": "Chemische Grundstoffe",
        "unit": "t CO₂/t product",
        "average": 1.10,
        "top_10_pct": 0.70,
        "bottom_10_pct": 1.60,
        "best_available_technology": 0.50,
        "trend_2020_2025": -0.06,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "pulp_paper": {
        "sector_name": "Zellstoff & Papier",
        "unit": "t CO₂/t paper",
        "average": 0.35,
        "top_10_pct": 0.20,
        "bottom_10_pct": 0.55,
        "best_available_technology": 0.15,
        "trend_2020_2025": -0.04,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "glass": {
        "sector_name": "Glas (Floatglas)",
        "unit": "t CO₂/t glass",
        "average": 0.55,
        "top_10_pct": 0.40,
        "bottom_10_pct": 0.75,
        "best_available_technology": 0.30,
        "trend_2020_2025": -0.02,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "ceramics": {
        "sector_name": "Keramik / Ziegel",
        "unit": "t CO₂/t product",
        "average": 0.25,
        "top_10_pct": 0.15,
        "bottom_10_pct": 0.40,
        "best_available_technology": 0.10,
        "trend_2020_2025": -0.02,
        "data_source": "EU ETS Phase IV Benchmark curves, EC 2021/447",
    },
    "aviation": {
        "sector_name": "Luftverkehr (EU-internal)",
        "unit": "t CO₂/1k km",
        "average": 0.85,
        "top_10_pct": 0.60,
        "bottom_10_pct": 1.15,
        "best_available_technology": 0.45,
        "trend_2020_2025": -0.05,
        "data_source": "EU ETS Aviation Benchmark, EC 2021/447",
    },
}

# ─── Non-ETS Emission Benchmarks (Scope 1+2 by Sector) ──────────────
# Based on Eurostat + EEA data for entire EU economy
# Units: t CO₂e / €M revenue

SCOPE_12_INTENSITY: dict[str, dict] = {
    "manufacturing": {
        "average": 125,
        "top_10_pct": 40,
        "bottom_10_pct": 280,
        "unit": "t CO₂e/€M revenue",
        "trend": "-3.2%/year",
        "data_source": "EEA Greenhouse Gas Inventory + Eurostat 2024",
    },
    "energy": {
        "average": 850,
        "top_10_pct": 300,
        "bottom_10_pct": 1500,
        "unit": "t CO₂e/€M revenue",
        "trend": "-4.5%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "construction": {
        "average": 80,
        "top_10_pct": 25,
        "bottom_10_pct": 180,
        "unit": "t CO₂e/€M revenue",
        "trend": "-2.8%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "transport": {
        "average": 200,
        "top_10_pct": 70,
        "bottom_10_pct": 400,
        "unit": "t CO₂e/€M revenue",
        "trend": "-1.5%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "agriculture": {
        "average": 350,
        "top_10_pct": 120,
        "bottom_10_pct": 700,
        "unit": "t CO₂e/€M revenue",
        "trend": "-1.0%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "real_estate": {
        "average": 45,
        "top_10_pct": 15,
        "bottom_10_pct": 100,
        "unit": "t CO₂e/€M revenue (Scope 1+2, buildings)",
        "trend": "-2.0%/year",
        "data_source": "CRREM 2024 + EEA 2024",
    },
    "finance": {
        "average": 15,
        "top_10_pct": 5,
        "bottom_10_pct": 35,
        "unit": "t CO₂e/€M revenue (Scope 1+2 only)",
        "trend": "-2.5%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "technology": {
        "average": 20,
        "top_10_pct": 5,
        "bottom_10_pct": 50,
        "unit": "t CO₂e/€M revenue",
        "trend": "-3.0%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
    "retail": {
        "average": 30,
        "top_10_pct": 10,
        "bottom_10_pct": 70,
        "unit": "t CO₂e/€M revenue",
        "trend": "-2.0%/year",
        "data_source": "EEA GHG Inventory + Eurostat 2024",
    },
}


async def get_ets_benchmark(sector: str, region: str = "EU") -> dict:
    """
    Get EU ETS benchmark data for a sector.

    Returns average, top 10%, and bottom 10% emission thresholds
    plus historical trend.

    Args:
        sector: Industry sector (see EU_ETS_SECTORS)
        region: Geographic scope (default: EU)

    Returns:
        dict with benchmark data
    """
    benchmark = EU_ETS_BENCHMARKS_2024.get(sector)
    if not benchmark:
        return {
            "error": f"Sector '{sector}' not found in EU ETS benchmarks",
            "available_sectors": list(EU_ETS_BENCHMARKS_2024.keys()),
        }
    result = dict(benchmark)
    result["sector"] = sector
    result["region"] = region
    result["benchmark_phase"] = "EU ETS Phase IV (2021-2030)"
    return result


async def get_sector_emission_intensity(sector: str, region: str = "EU") -> dict:
    """
    Get economy-wide Scope 1+2 emission intensity for a sector.

    Args:
        sector: Business sector (e.g., 'manufacturing', 'energy')
        region: Geographic scope (default: EU)

    Returns:
        dict with intensity benchmarks
    """
    intensity = SCOPE_12_INTENSITY.get(sector)
    if not intensity:
        return {
            "error": f"Sector '{sector}' not found",
            "available_sectors": list(SCOPE_12_INTENSITY.keys()),
        }
    result = dict(intensity)
    result["sector"] = sector
    result["region"] = region
    return result
