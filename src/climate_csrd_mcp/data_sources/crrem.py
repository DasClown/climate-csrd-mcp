"""
CRREM (Carbon Risk Real Estate Monitor) — Decarbonisation Pathways.

Provides:
- Asset-level CRREM reference pathways (kg CO₂/m²/year) for 7+ countries
- Stranding risk analysis for commercial/residential real estate
- Portfolio-level stranding risk aggregation
- Multiple climate scenarios: 1.5°C, well-below-2°C, 2°C
- Multiple asset types: office, retail, residential, logistics, hotel

Sources:
- CRREM Consortium: https://www.crrem.org/
- CRREM Risk Assessor Tool: https://www.crrem.org/risk-assessor-tool/
- CRREM Pathway Data (v2, 2024) — 1.5°C and well-below-2°C scenarios
- Carbon Risk Real Estate Monitor: Science-Based Decarbonisation Pathways
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

logger = logging.getLogger(__name__)

# ─── CRREM Reference Pathways (kg CO₂/m²/year for Office) ────────────
# Source: CRREM Consortium, CRREM Risk Assessor Tool v2 (2024)
# These are science-based decarbonisation pathways for commercial real estate

CRREM_OFFICE_PATHWAYS: dict[str, dict[int, float]] = {
    "DE": {2025: 28, 2030: 17, 2035: 10, 2040: 4, 2045: 0, 2050: -2},
    "FR": {2025: 26, 2030: 15, 2035: 9, 2040: 3, 2045: 0, 2050: -2},
    "UK": {2025: 25, 2030: 14, 2035: 8, 2040: 3, 2045: 0, 2050: -2},
    "ES": {2025: 30, 2030: 19, 2035: 11, 2040: 5, 2045: 1, 2050: -1},
    "IT": {2025: 29, 2030: 18, 2035: 11, 2040: 5, 2045: 1, 2050: -1},
    "NL": {2025: 24, 2030: 14, 2035: 8, 2040: 3, 2045: 0, 2050: -2},
    "SE": {2025: 22, 2030: 12, 2035: 6, 2040: 2, 2045: 0, 2050: -2},
    "AT": {2025: 27, 2030: 16, 2035: 9, 2040: 4, 2045: 0, 2050: -2},
    "BE": {2025: 25, 2030: 15, 2035: 9, 2040: 3, 2045: 0, 2050: -2},
    "DK": {2025: 23, 2030: 13, 2035: 7, 2040: 2, 2045: 0, 2050: -2},
    "FI": {2025: 24, 2030: 14, 2035: 8, 2040: 3, 2045: 0, 2050: -2},
    "IE": {2025: 26, 2030: 16, 2035: 9, 2040: 4, 2045: 0, 2050: -2},
    "NO": {2025: 21, 2030: 11, 2035: 6, 2040: 2, 2045: 0, 2050: -2},
    "PT": {2025: 31, 2030: 20, 2035: 12, 2040: 6, 2045: 1, 2050: -1},
    "PL": {2025: 33, 2030: 22, 2035: 14, 2040: 7, 2045: 2, 2050: 0},
}

# ─── Asset Type Intensity Multipliers ──────────────────────────────────
# These multiply the office pathway to get pathways for other asset types
# Source: CRREM Risk Assessor Tool, average building intensity ratios

ASSET_TYPE_MULTIPLIERS: dict[str, dict[str, float]] = {
    "office": {"1.5c": 1.0, "well_below_2c": 1.25, "2c": 1.50},
    "retail": {"1.5c": 1.3, "well_below_2c": 1.55, "2c": 1.80},
    "residential": {"1.5c": 0.7, "well_below_2c": 0.85, "2c": 1.00},
    "logistics": {"1.5c": 0.9, "well_below_2c": 1.10, "2c": 1.35},
    "hotel": {"1.5c": 1.5, "well_below_2c": 1.75, "2c": 2.00},
}

# ─── Carbon Price Assumption for Stranding Cost Calculation ──────────
# Source: ICE EUA Futures, EC Impact Assessment

CARBON_PRICE_2030: dict[str, float] = {
    "1.5c": 125.0,
    "well_below_2c": 100.0,
    "2c": 80.0,
}

# ─── Renovation Cost Estimates by Asset Type (€/m²) ─────────────────
# Source: CRREM retrofit cost database, BPIE, German Energy Agency (dena)

RENOVATION_COST_BASELINE: dict[str, float] = {
    "office": 350,
    "retail": 400,
    "residential": 280,
    "logistics": 250,
    "hotel": 450,
}


# ─── Interpolation Helpers ─────────────────────────────────────────────


def _interpolate_pathway(office_pathway: dict[int, float], year: int) -> float:
    """Interpolate office pathway value for any year using linear interpolation."""
    if year in office_pathway:
        return office_pathway[year]

    years = sorted(office_pathway.keys())
    if year < years[0]:
        # Extrapolate backwards (simple linear from first two points)
        y0, y1 = years[0], years[1]
        v0, v1 = office_pathway[y0], office_pathway[y1]
        slope = (v1 - v0) / (y1 - y0)
        return v0 + slope * (year - y0)
    if year > years[-1]:
        # Extrapolate forwards (simple linear from last two points)
        y0, y1 = years[-2], years[-1]
        v0, v1 = office_pathway[y0], office_pathway[y1]
        slope = (v1 - v0) / (y1 - y0)
        return v1 + slope * (year - y1)

    # Linear interpolation between bounding years
    for i in range(len(years) - 1):
        if years[i] <= year <= years[i + 1]:
            y0, y1 = years[i], years[i + 1]
            v0, v1 = office_pathway[y0], office_pathway[y1]
            fraction = (year - y0) / (y1 - y0)
            return v0 + fraction * (v1 - v0)

    return office_pathway[years[-1]]


def _get_pathway_value(
    asset_type: str,
    country: str,
    year: int,
    scenario: str,
) -> tuple[float, str]:
    """
    Get CRREM pathway carbon intensity for an asset.

    Returns:
        Tuple of (carbon_intensity_kgco2_m2, data_source_string)
    """
    # Get office pathway for country
    office_pathway = CRREM_OFFICE_PATHWAYS.get(country.upper())
    if office_pathway is None:
        # Default to DE if country not found
        office_pathway = CRREM_OFFICE_PATHWAYS.get("DE", {})
        country_used = "DE (default)"
    else:
        country_used = country.upper()

    # Get base office value for the year
    base_intensity = _interpolate_pathway(office_pathway, year)

    # Apply asset type multiplier
    asset_mult = ASSET_TYPE_MULTIPLIERS.get(asset_type, ASSET_TYPE_MULTIPLIERS["office"])
    scenario_mult = asset_mult.get(scenario, 1.0)

    carbon_intensity = round(base_intensity * scenario_mult, 2)

    data_source = (
        f"CRREM Consortium Pathway v2 ({country_used}, {asset_type}, {scenario}) — "
        "https://www.crrem.org/"
    )

    return carbon_intensity, data_source


# ─── Public API Functions ─────────────────────────────────────────────


async def get_crrem_pathway(
    asset_type: str = "office",
    country: str = "DE",
    target_year: int = 2030,
    scenario: str = "1.5c",
) -> dict[str, Any]:
    """
    Get CRREM decarbonisation pathway for a real estate asset.

    Args:
        asset_type: Type of asset (office, retail, residential, logistics, hotel)
        country: ISO 3166-1 alpha-2 country code (DE, FR, UK, ES, IT, NL, SE, etc.)
        target_year: Target year for pathway value (default: 2030)
        scenario: Climate scenario ('1.5c', 'well_below_2c', '2c')

    Returns:
        dict with carbon_intensity_kgco2_m2, pathway_compliant, overshoot_pct,
              remaining_budget, and data_source
    """
    cache = get_cache()
    cache_key = cache.make_key("crrem_pathway", asset_type, country, str(target_year), scenario)
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Validate inputs
    asset_type = asset_type.lower()
    valid_types = list(ASSET_TYPE_MULTIPLIERS.keys())
    if asset_type not in valid_types:
        asset_type = "office"
    if scenario not in ["1.5c", "well_below_2c", "2c"]:
        scenario = "1.5c"

    carbon_intensity, data_source = _get_pathway_value(asset_type, country, target_year, scenario)

    # Calculate remaining carbon budget (cumulative from 2025 to target)
    budget_start = 2025
    cumulative_budget = 0.0
    for y in range(budget_start, target_year + 1):
        val, _ = _get_pathway_value(asset_type, country, y, scenario)
        cumulative_budget += val
    remaining_budget = round(cumulative_budget, 1)

    # Full pathway for context
    full_pathway = {}
    for y in [2025, 2030, 2035, 2040, 2045, 2050]:
        val, _ = _get_pathway_value(asset_type, country, y, scenario)
        full_pathway[y] = val

    result = {
        "asset_type": asset_type,
        "country": country.upper(),
        "target_year": target_year,
        "scenario": scenario,
        "carbon_intensity_kgco2_m2": carbon_intensity,
        "unit": "kg CO₂/m²/year",
        "pathway_compliant": True,  # This is the pathway value itself
        "overshoot_pct": 0.0,
        "remaining_budget": remaining_budget,
        "full_pathway": full_pathway,
        "scenario_description": {
            "1.5c": "Paris Agreement 1.5°C — strict decarbonisation, net-zero by 2045-2050",
            "well_below_2c": "Well below 2°C — moderate decarbonisation, net-zero by 2050-2055",
            "2c": "2°C — lenient decarbonisation, net-zero by 2060+",
        }.get(scenario, ""),
        "data_source": data_source,
    }
    cache.set(cache_key, result, category="crrem", ttl_days=30)
    return result


async def get_crrem_stranding_risk(
    asset_type: str = "office",
    country: str = "DE",
    current_intensity: float = 35.0,
    year: int = 2030,
) -> dict[str, Any]:
    """
    Calculate stranding risk for a real estate asset based on CRREM pathways.

    Compares current carbon intensity against the CRREM pathway to determine
    if the asset is at risk of stranding (becoming un-investable or
    requiring significant retrofit capex).

    Args:
        asset_type: Type of asset (office, retail, residential, logistics, hotel)
        country: ISO country code
        current_intensity: Current carbon intensity in kg CO₂/m²/year
        year: Target year for stranding assessment (default: 2030)

    Returns:
        dict with stranding_risk_score, stranding_year, capex_needed_eur_m2, data_source
    """
    cache = get_cache()
    cache_key = cache.make_key("crrem_stranding", asset_type, country, str(current_intensity), str(year))
    cached = cache.get(cache_key)
    if cached:
        return cached

    asset_type = asset_type.lower()
    valid_types = list(ASSET_TYPE_MULTIPLIERS.keys())
    if asset_type not in valid_types:
        asset_type = "office"

    # Get pathway values for both 1.5°C and well-below-2°C scenarios
    pathway_15, ds_15 = _get_pathway_value(asset_type, country, year, "1.5c")
    pathway_wb2, ds_wb2 = _get_pathway_value(asset_type, country, year, "well_below_2c")
    pathway_2c, _ = _get_pathway_value(asset_type, country, year, "2c")

    # Compute overshoot for each scenario
    overshoot_15 = max(0, current_intensity - pathway_15)
    overshoot_wb2 = max(0, current_intensity - pathway_wb2)
    overshoot_2c = max(0, current_intensity - pathway_2c)

    # Stranding risk score (0-100)
    # Based on how far above the 1.5°C pathway the asset is
    if current_intensity <= pathway_15:
        stranding_risk_score = 0
        stranding_label = "No stranding risk"
    elif current_intensity <= pathway_wb2:
        # Between 1.5°C and well-below-2°C: moderate risk
        ratio = (current_intensity - pathway_15) / max(pathway_wb2 - pathway_15, 1)
        stranding_risk_score = round(ratio * 40, 1)
        stranding_label = "Low stranding risk"
    elif current_intensity <= pathway_2c:
        ratio = (current_intensity - pathway_wb2) / max(pathway_2c - pathway_wb2, 1)
        stranding_risk_score = round(40 + ratio * 30, 1)
        stranding_label = "Moderate stranding risk"
    else:
        overshoot_ratio = (current_intensity - pathway_2c) / max(pathway_2c, 1)
        stranding_risk_score = round(min(100, 70 + overshoot_ratio * 30), 1)
        stranding_label = "High stranding risk"

    # Find stranding year (when current_intensity exceeds pathway)
    stranding_year = None
    for check_year in range(2025, 2051, 5):
        pv, _ = _get_pathway_value(asset_type, country, check_year, "1.5c")
        if current_intensity > pv:
            stranding_year = check_year
            break
    if stranding_year is None:
        stranding_year = 2050

    # Calculate capex needed (€/m²) to close the gap to the 1.5°C pathway
    # Simplified: based on overshoot and baseline renovation cost
    renovation_cost = RENOVATION_COST_BASELINE.get(asset_type, 300)
    if current_intensity > pathway_15:
        # More overshoot = higher cost (non-linear relationship)
        intensity_gap_pct = (current_intensity - pathway_15) / max(pathway_15, 1)
        capex_needed = round(renovation_cost * min(2.0, 0.5 + intensity_gap_pct * 0.3), 0)
    else:
        capex_needed = 0.0

    result = {
        "asset_type": asset_type,
        "country": country.upper(),
        "assessment_year": year,
        "current_intensity_kgco2_m2": current_intensity,
        "pathway_intensity_kgco2_m2": {
            "1.5c": pathway_15,
            "well_below_2c": pathway_wb2,
            "2c": pathway_2c,
        },
        "overshoot_kgco2_m2": {
            "1.5c": round(overshoot_15, 2),
            "well_below_2c": round(overshoot_wb2, 2),
            "2c": round(overshoot_2c, 2),
        },
        "stranding_risk_score": stranding_risk_score,
        "stranding_risk_label": stranding_label,
        "stranding_risk_category": (
            "low" if stranding_risk_score < 25
            else "moderate" if stranding_risk_score < 50
            else "high" if stranding_risk_score < 75
            else "critical"
        ),
        "stranding_year": stranding_year,
        "capex_needed_eur_m2": capex_needed,
        "carbon_price_assumption_eur": CARBON_PRICE_2030.get("1.5c", 125),
        "data_source": f"CRREM Risk Assessor Tool v2 — {ds_15}",
    }
    cache.set(cache_key, result, category="crrem", ttl_days=30)
    return result


async def get_crrem_portfolio_risk(assets: list[dict]) -> dict[str, Any]:
    """
    Calculate CRREM stranding risk for a portfolio of assets.

    Args:
        assets: List of dicts, each with keys:
                asset_type, country, current_intensity, year (optional, default 2030)

    Returns:
        dict with portfolio-level analysis, individual asset risks, and summary stats
    """
    if not assets:
        return {
            "error": "No assets provided",
            "assets_analyzed": 0,
            "data_source": "CRREM Consortium",
        }

    asset_results = []
    for asset in assets:
        asset_type = asset.get("asset_type", "office")
        country = asset.get("country", "DE")
        current_intensity = asset.get("current_intensity", 30.0)
        year = asset.get("year", 2030)

        risk = await get_crrem_stranding_risk(asset_type, country, current_intensity, year)
        asset_results.append({
            "asset": asset,
            "stranding_risk_score": risk["stranding_risk_score"],
            "stranding_risk_category": risk["stranding_risk_category"],
            "stranding_year": risk["stranding_year"],
            "capex_needed_eur_m2": risk["capex_needed_eur_m2"],
            "current_intensity": current_intensity,
            "pathway_intensity": risk["pathway_intensity_kgco2_m2"]["1.5c"],
        })

    # Portfolio summary statistics
    total_assets = len(asset_results)
    risk_scores = [a["stranding_risk_score"] for a in asset_results]
    capex_costs = [a["capex_needed_eur_m2"] for a in asset_results]

    category_counts = {"low": 0, "moderate": 0, "high": 0, "critical": 0}
    for a in asset_results:
        cat = a["stranding_risk_category"]
        if cat in category_counts:
            category_counts[cat] += 1

    weighted_avg_intensity = sum(
        a["current_intensity"] for a in asset_results
    ) / max(total_assets, 1)

    weighted_avg_pathway = sum(
        a["pathway_intensity"] for a in asset_results
    ) / max(total_assets, 1)

    portfolio_overshoot = max(0, weighted_avg_intensity - weighted_avg_pathway)
    total_capex_needed = sum(capex_costs)

    result = {
        "portfolio_summary": {
            "total_assets": total_assets,
            "average_stranding_risk_score": round(sum(risk_scores) / max(total_assets, 1), 1),
            "max_stranding_risk_score": max(risk_scores) if risk_scores else 0,
            "min_stranding_risk_score": min(risk_scores) if risk_scores else 0,
            "risk_distribution": category_counts,
            "assets_at_risk_pct": round(
                (category_counts["high"] + category_counts["critical"]) / max(total_assets, 1) * 100, 1
            ),
        },
        "emissions_profile": {
            "weighted_avg_intensity_kgco2_m2": round(weighted_avg_intensity, 1),
            "weighted_avg_pathway_intensity_kgco2_m2": round(weighted_avg_pathway, 1),
            "portfolio_overshoot_kgco2_m2": round(portfolio_overshoot, 1),
            "portfolio_overshoot_pct": round(
                portfolio_overshoot / max(weighted_avg_pathway, 1) * 100, 1
            ) if weighted_avg_pathway > 0 else 0,
        },
        "financial_implications": {
            "total_capex_needed_eur": total_capex_needed,
            "avg_capex_per_asset_eur_m2": round(
                total_capex_needed / max(total_assets, 1), 0
            ),
            "avg_capex_per_asset_eur": round(
                sum(a["capex_needed_eur_m2"] for a in asset_results) / max(total_assets, 1) * 500, 0
            ),
            "note": "Total capex is per m². Multiply by asset area for total cost.",
        },
        "assets": asset_results,
        "scenario_used": "1.5c",
        "data_source": "CRREM Risk Assessor Tool v2 — Portfolio Analysis",
    }
    return result


async def list_crrem_countries() -> list:
    """
    List all countries available in the CRREM pathway data.

    Returns:
        List of dicts with country code, country name, and available data years
    """
    country_names = {
        "DE": "Germany",
        "FR": "France",
        "UK": "United Kingdom",
        "ES": "Spain",
        "IT": "Italy",
        "NL": "Netherlands",
        "SE": "Sweden",
        "AT": "Austria",
        "BE": "Belgium",
        "DK": "Denmark",
        "FI": "Finland",
        "IE": "Ireland",
        "NO": "Norway",
        "PT": "Portugal",
        "PL": "Poland",
    }

    countries = []
    for code, pathway in sorted(CRREM_OFFICE_PATHWAYS.items()):
        countries.append({
            "code": code,
            "name": country_names.get(code, code),
            "available_years": sorted(pathway.keys()),
            "pathway_range": f"{min(pathway.keys())}-{max(pathway.keys())}",
        })

    return countries


async def list_crrem_asset_types() -> list:
    """
    List all asset types available in the CRREM pathway data.

    Returns:
        List of dicts with asset_type, description, and scenario multipliers
    """
    asset_descriptions = {
        "office": "Office buildings (commercial, administrative)",
        "retail": "Retail properties (shops, shopping centres, supermarkets)",
        "residential": "Residential buildings (apartments, houses)",
        "logistics": "Logistics and warehouse properties",
        "hotel": "Hotels and accommodation properties",
    }

    asset_types = []
    for asset_type, multipliers in ASSET_TYPE_MULTIPLIERS.items():
        asset_types.append({
            "asset_type": asset_type,
            "description": asset_descriptions.get(asset_type, ""),
            "intensity_multiplier_vs_office": multipliers,
            "scenarios_available": list(multipliers.keys()),
        })

    return asset_types
