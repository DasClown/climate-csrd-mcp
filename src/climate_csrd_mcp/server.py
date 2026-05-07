"""
Climate CSRD MCP Server — Main entry point.

Implements 5 MCP tools using FastMCP:
  1. assess_climate_risk
  2. get_emission_benchmarks
  3. get_csrd_requirements
  4. csrd_report
  5. get_kfw_funding

Usage:
    climate-csrd-mcp              # starts stdio MCP server
    climate-csrd-mcp --help       # show help
"""

import asyncio
import logging
import os
import sys
from datetime import date
from typing import Any, Optional

from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP

from .cache import get_cache
from .utils import (
    aggregate_risk,
    risk_label,
    risk_color,
    validate_coordinates,
    CSRD_DISCLAIMER,
    today_iso,
    get_esrs_ref,
)
from .data_sources import (
    copernicus as src_copernicus,
    dwd as src_dwd,
    eu_ets as src_eu_ets,
    uba as src_uba,
    eurlex as src_eurlex,
    kfw as src_kfw,
)

# ─── Load .env ───────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Create MCP Server ───────────────────────────────────────────────

mcp = FastMCP(
    name="Climate CSRD Intelligence",
    instructions="Klimarisiko-Analyse und CSRD-Berichterstattung — Copernicus + DWD + EU ETS + UBA + ESRS",
    host="127.0.0.1",
    port=8000,
)

# ─── Tool 1: assess_climate_risk ─────────────────────────────────────

@mcp.tool(
    name="assess_climate_risk",
    description="Bewertet das physische Klimarisiko eines Standorts. "
    "Kombiniert Copernicus-Hochwasser-Risikoklasse, DWD-Hitzetage-Prognose "
    "und Dürre-Index zu einem Gesamt-Risikoscore (1-5).",
)
async def assess_climate_risk(
    lat: float,
    lon: float,
    location_name: str = "",
    year_horizon: int = 2030,
) -> dict[str, Any]:
    """
    Assess physical climate risk for a location.

    Args:
        lat: Latitude (-90 to 90)
        lon: Longitude (-180 to 180)
        location_name: Optional human-readable location name
        year_horizon: Target year for projections (default: 2030)

    Returns:
        dict with flood risk, heat risk, drought index, overall score
    """
    lat, lon = validate_coordinates(lat, lon)
    cache = get_cache()
    cache_key = cache.make_key("climate", str(lat), str(lon), str(year_horizon))

    # Check cache
    cached = cache.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {lat},{lon} ({location_name or 'unknown'})")
        return cached

    logger.info(f"Computing climate risk for {lat},{lon} ({location_name or 'unknown'})")

    # Gather data from all sources in parallel
    flood_task = src_copernicus.get_flood_risk(lat, lon)
    drought_task = src_copernicus.get_drought_index(lat, lon)
    heat_task = src_dwd.get_hot_day_projection(lat, lon, year_horizon)
    climate_ref_task = src_dwd.get_climate_reference(lat, lon)

    flood_result, drought_result, heat_result, climate_ref = await asyncio.gather(
        flood_task, drought_task, heat_task, climate_ref_task
    )

    flood_risk = flood_result["risk_class"]
    drought_risk = drought_result["risk_class"]
    hot_days = heat_result["projected_hot_days_per_year"]

    # Heat risk class (based on hot days)
    if hot_days <= 5:
        heat_risk = 1
    elif hot_days <= 10:
        heat_risk = 2
    elif hot_days <= 18:
        heat_risk = 3
    elif hot_days <= 28:
        heat_risk = 4
    else:
        heat_risk = 5

    # Overall risk
    scores = [flood_risk, drought_risk, heat_risk]
    overall = aggregate_risk(scores)

    result: dict[str, Any] = {
        "location": {
            "name": location_name or f"{lat:.4f}, {lon:.4f}",
            "lat": lat,
            "lon": lon,
            "year_horizon": year_horizon,
        },
        "flood_risk": {
            "class": flood_risk,
            "label": risk_label(flood_risk),
            "color": risk_color(flood_risk),
            "region": flood_result.get("region", ""),
            "data_source": flood_result.get("data_source", "Copernicus EMS"),
        },
        "heat_risk": {
            "class": heat_risk,
            "label": risk_label(heat_risk),
            "color": risk_color(heat_risk),
            "projected_hot_days_per_year": hot_days,
            "reference_city": heat_result.get("nearest_reference_city", ""),
            "increase_vs_1961_1990": heat_result.get("increase_vs_baseline", 0),
            "data_source": heat_result.get("data_source", "DWD CDC"),
        },
        "drought_risk": {
            "class": drought_risk,
            "label": risk_label(drought_risk),
            "color": risk_color(drought_risk),
            "trend": drought_result.get("trend", "stable"),
            "region": drought_result.get("region", ""),
            "data_source": drought_result.get("data_source", "EDO C3S"),
        },
        "climate_reference": {
            "temperature_increase_c": climate_ref.get("warming_trend", {}).get("temperature_increase_c", 1.3),
            "baseline_period": "1961-1990 vs 1991-2020",
            "nearest_station": climate_ref.get("nearest_station", ""),
            "data_source": "DWD CDC",
        },
        "overall_risk": {
            "score": overall,
            "label": risk_label(overall),
            "color": risk_color(overall),
            "components": scores,
            "methodology": "Max-based aggregate, boost +1 if 3+ dimensions ≥ 3",
        },
        "disclaimer": (
            "Diese Risikobewertung basiert auf öffentlichen Klimadaten (Copernicus, DWD, EEA). "
            "Sie ersetzt keine standortspezifische Gefährdungsbeurteilung."
        ),
    }

    # Cache for 30 days (climate data doesn't change daily)
    cache.set(cache_key, result, category="climate")
    return result


# ─── Tool 2: get_emission_benchmarks ─────────────────────────────────

@mcp.tool(
    name="get_emission_benchmarks",
    description="Holt Branchen-Emissionsbenchmarks aus dem EU ETS "
    "und der EEA-Datenbank. Liefert Durchschnitt, Top/Bottom 10% und Trend.",
)
async def get_emission_benchmarks(
    sector: str,
    region: str = "EU",
) -> dict[str, Any]:
    """
    Get emission benchmarks for a sector.

    Args:
        sector: Sector name (e.g., 'cement', 'steel', 'manufacturing')
        region: Geographic scope (default: 'EU')

    Returns:
        dict with benchmark data
    """
    cache = get_cache()
    cache_key = cache.make_key("benchmarks", sector, region)

    cached = cache.get(cache_key)
    if cached:
        return cached

    # Try EU ETS benchmark first (industrial sectors)
    ets_result = await src_eu_ets.get_ets_benchmark(sector, region)
    if "error" not in ets_result:
        result = {
            "type": "eu_ets_benchmark",
            "sector": sector,
            "region": region,
            "data": ets_result,
            "data_source": "EU ETS Phase IV, EC 2021/447",
        }
        cache.set(cache_key, result, category="emissions")
        return result

    # Fall back to economy-wide intensity
    intensity = await src_eu_ets.get_sector_emission_intensity(sector, region)
    if "error" not in intensity:
        result = {
            "type": "sector_emission_intensity",
            "sector": sector,
            "region": region,
            "data": intensity,
            "data_source": "EEA GHG Inventory + Eurostat 2024",
        }
        cache.set(cache_key, result, category="emissions")
        return result

    # Sector not found
    return {
        "error": f"Unbekannter Sektor: '{sector}'",
        "available_ets_sectors": [
            "power_generation", "cement", "steel", "refineries",
            "chemicals", "pulp_paper", "glass", "ceramics", "aviation",
        ],
        "available_economy_sectors": [
            "manufacturing", "energy", "construction", "transport",
            "agriculture", "real_estate", "finance", "technology", "retail",
        ],
    }


# ─── Tool 3: get_csrd_requirements ───────────────────────────────────

@mcp.tool(
    name="get_csrd_requirements",
    description="Ermittelt welche ESRS-Offenlegungspflichten für ein "
    "Unternehmen gelten. Basiert auf CSRD-Schwellenwerten (Art. 3), "
    "Sektor-Materialität und Unternehmensgröße.",
)
async def get_csrd_requirements(
    entity_type: str = "large",
    sector: str = "manufacturing",
    employees: int = 500,
    revenue: float = 100.0,
) -> dict[str, Any]:
    """
    Get CSRD/ESRS reporting requirements for an entity.

    Args:
        entity_type: 'large', 'listed_sme', or 'non_eu_group'
        sector: Business sector
        employees: Number of employees
        revenue: Annual revenue in €M

    Returns:
        dict with applicable ESRS standards
    """
    cache = get_cache()
    cache_key = cache.make_key("csrd", entity_type, sector, str(employees), str(revenue))

    cached = cache.get(cache_key)
    if cached:
        return cached

    result = await src_eurlex.get_csrd_requirements(
        entity_type=entity_type,
        sector=sector,
        employees=employees,
        revenue=revenue,
    )

    result["disclaimer"] = CSRD_DISCLAIMER

    cache.set(cache_key, result, category="csrd")
    return result


# ─── Tool 4: csrd_report ─────────────────────────────────────────────

@mcp.tool(
    name="csrd_report",
    description="Erstellt einen CSRD-konformen Berichtsbaustein für "
    "einen Standort. Enthält physische Risikoanalyse, Emissionsbenchmark "
    "und ESRS-Referenzen. Finale Prüfung durch das Unternehmen erforderlich.",
)
async def csrd_report(
    lat: float,
    lon: float,
    sector: str,
    company_name: str = "",
    site_name: str = "",
    employees: int = 500,
    revenue: float = 100.0,
    year_horizon: int = 2030,
    entity_type: str = "large",
    own_emission_intensity: Optional[float] = None,
) -> dict[str, Any]:
    """
    Generate a CSRD-compliant report module for a location.

    Args:
        lat: Latitude
        lon: Longitude
        sector: Business sector
        company_name: Optional company name
        site_name: Optional site name
        employees: Number of employees
        revenue: Annual revenue in €M
        year_horizon: Target year for projections
        entity_type: 'large', 'listed_sme', or 'non_eu_group'
        own_emission_intensity: Optional company-specific emission intensity

    Returns:
        Comprehensive CSRD report module
    """
    # Gather all data in parallel
    risk_task = assess_climate_risk(lat, lon, site_name or company_name, year_horizon)
    csrd_task = get_csrd_requirements(entity_type, sector, employees, revenue)
    bench_task = get_emission_benchmarks(sector)

    risk_data, csrd_data, bench_data = await asyncio.gather(
        risk_task, csrd_task, bench_task
    )

    # Get location-specific ESRS triggers
    flood_risk = risk_data["flood_risk"]["class"]
    hot_days = risk_data["heat_risk"]["projected_hot_days_per_year"]
    drought_risk = risk_data["drought_risk"]["class"]

    location_triggers = await src_eurlex.get_location_specific_triggers(
        flood_risk=flood_risk,
        hot_days=hot_days,
        drought_index=drought_risk,
    )

    # Build ESRS applicability matrix
    esrs_applicable = []
    for std in csrd_data.get("all_applicable_standards", []):
        ref = get_esrs_ref(std)
        is_core = std in csrd_data.get("core_mandatory_standards", [])
        esrs_applicable.append({
            "standard": std,
            "title": ref,
            "mandatory": is_core,
            "location_triggered": False,
        })

    for trigger in location_triggers:
        for req in trigger.get("requirements", []):
            esrs_applicable.append({
                "standard": req,
                "title": req,
                "mandatory": False,
                "location_triggered": True,
                "trigger_reason": trigger.get("trigger", ""),
                "trigger_severity": trigger.get("severity", 0),
            })

    # Benchmark comparison
    comparison = None
    if own_emission_intensity is not None:
        avg = bench_data.get("data", {}).get("average", 0)
        if avg:
            pct_diff = ((own_emission_intensity - avg) / avg) * 100
            comparison = {
                "own_intensity": own_emission_intensity,
                "sector_average": avg,
                "unit": bench_data.get("data", {}).get("unit", "t CO₂e/€M"),
                "difference_pct": round(pct_diff, 1),
                "status": "above_average" if pct_diff > 0 else "below_average",
                "top_10_pct_threshold": bench_data.get("data", {}).get("top_10_pct"),
                "bottom_10_pct_threshold": bench_data.get("data", {}).get("bottom_10_pct"),
            }

    report = {
        "report_metadata": {
            "generated_at": today_iso(),
            "company": company_name or "N/A",
            "site": site_name or f"{lat:.4f}, {lon:.4f}",
            "sector": sector,
            "entity_type": entity_type,
        },
        "physical_climate_risk": risk_data,
        "emission_benchmarks": bench_data,
        "benchmark_comparison": comparison,
        "csrd_applicability": {
            "entity_classification": csrd_data.get("classification", ""),
            "first_reporting_deadline": csrd_data.get("first_reporting", "N/A"),
            "core_mandatory": csrd_data.get("core_mandatory_standards", []),
            "sector_material_topics": csrd_data.get("sector_material_topics", []),
        },
        "esrs_applicability_matrix": esrs_applicable,
        "location_specific_triggers": location_triggers,
        "recommendations": _generate_recommendations(
            overall_risk=risk_data["overall_risk"]["score"],
            flood_risk=flood_risk,
            heat_risk=risk_data["heat_risk"]["class"],
            drought_risk=drought_risk,
            sector=sector,
        ),
        "disclaimer": CSRD_DISCLAIMER,
    }

    return report


def _generate_recommendations(
    overall_risk: int,
    flood_risk: int,
    heat_risk: int,
    drought_risk: int,
    sector: str,
) -> list[dict]:
    """Generate actionable recommendations based on risk profile."""
    recs = []

    if flood_risk >= 4:
        recs.append({
            "priority": "high",
            "area": "flood_protection",
            "recommendation": "Hochwasserschutzmaßnahmen prüfen: mobile Barrieren, "
                "Rückhaltebecken, wasserdichte Lagerung kritischer Betriebsmittel.",
            "esrs_ref": "E1-7, E4-3",
            "kfw_programs": ["KfW 441"],
        })
    elif flood_risk >= 3:
        recs.append({
            "priority": "medium",
            "area": "flood_protection",
            "recommendation": "Hochwasser-Risikoanalyse für Standort durchführen, "
                "Versicherungsschutz prüfen, Notfallpläne erstellen.",
            "esrs_ref": "E1-7",
            "kfw_programs": ["KfW 441"],
        })

    if heat_risk >= 4:
        recs.append({
            "priority": "high",
            "area": "heat_adaptation",
            "recommendation": "Hitzeschutz für Mitarbeiter (S1): Kühlräume, "
                "flexible Arbeitszeiten, Dach- und Fassadenbegrünung. "
                "Klimatisierung auf erneuerbare Energien umstellen.",
            "esrs_ref": "E1-2, E1-3, S1-8",
            "kfw_programs": ["KfW 290", "BAFA-EW"],
        })
    elif heat_risk >= 3:
        recs.append({
            "priority": "medium",
            "area": "heat_adaptation",
            "recommendation": "Hitzestress-Risikobewertung durchführen, "
                "Grünflächenanteil prüfen, Verschattungskonzept entwickeln.",
            "esrs_ref": "E1-7, S1-8",
            "kfw_programs": ["KfW 441"],
        })

    if drought_risk >= 4:
        recs.append({
            "priority": "high",
            "area": "water_management",
            "recommendation": "Wassermanagement-System implementieren: "
                "Regenwassernutzung, Kreislaufführung, wassersparende Technologien. "
                "Wasserentnahme genehmigen lassen (UBA).",
            "esrs_ref": "E3-1, E3-3, E3-4",
            "kfw_programs": ["KfW 441", "KfW 290"],
        })
    elif drought_risk >= 3:
        recs.append({
            "priority": "medium",
            "area": "water_management",
            "recommendation": "Wasserfußabdruck analysieren, "
                "Wasser-Effizienzmaßnahmen identifizieren, "
                "Risiko von Entnahmebeschränkungen bewerten.",
            "esrs_ref": "E3-1, E3-3",
            "kfw_programs": ["KfW 290"],
        })

    if overall_risk >= 4:
        recs.append({
            "priority": "high",
            "area": "climate_adaptation_strategy",
            "recommendation": "Standort-spezifische Klimaanpassungsstrategie "
                "entwickeln. ERM (Enterprise Risk Management) um physische "
                "Klimarisiken erweitern. Offenlegung nach ESRS E1-7 sicherstellen.",
            "esrs_ref": "E1-1, E1-2, E1-7",
            "kfw_programs": ["KfW 441", "BAFA-EBW"],
        })

    # Sector-specific
    if sector == "manufacturing" and overall_risk >= 3:
        recs.append({
            "priority": "medium",
            "area": "supply_chain_resilience",
            "recommendation": "Lieferketten-Resilienz prüfen: "
                "Klimarisiken in der Wertschöpfungskette identifizieren "
                "(ESRS S2, E1-7).",
            "esrs_ref": "E1-7, S2",
            "kfw_programs": [],
        })

    if not recs:
        recs.append({
            "priority": "low",
            "area": "monitoring",
            "recommendation": "Klimarisiko-Monitoring etablieren. "
                "Jährliche Aktualisierung der Risikobewertung.",
            "esrs_ref": "E1-2",
            "kfw_programs": [],
        })

    return recs


# ─── Tool 5: get_kfw_funding ─────────────────────────────────────────

@mcp.tool(
    name="get_kfw_funding",
    description="Findet passende KfW- und BAFA-Förderprogramme für "
    "Klimaschutz- und Anpassungsmaßnahmen an einem Standort.",
)
async def get_kfw_funding(
    standort_art: str = "produktion",
    sector: str = "manufacturing",
    measure: str = "energy_efficiency",
) -> dict[str, Any]:
    """
    Find matching KfW/BAFA funding programs.

    Args:
        standort_art: Standort-Typ (produktion, buero, logistik, landwirtschaft, etc.)
        sector: Wirtschaftssektor
        measure: Maßnahmenart (energy_efficiency, renewable_energy, flood_protection, etc.)

    Returns:
        dict with matching programs
    """
    cache = get_cache()
    cache_key = cache.make_key("kfw", standort_art, sector, measure)

    cached = cache.get(cache_key)
    if cached:
        return cached

    programs = await src_kfw.get_funding_programs(
        standort_art=standort_art,
        sector=sector,
        measure=measure,
    )

    result = {
        "standort_art": standort_art,
        "sector": sector,
        "measure": measure,
        "matching_programs": programs,
        "program_count": len(programs),
        "data_source": "KfW Bankengruppe / BAFA, Stand 2025",
        "disclaimer": (
            "Förderprogramme können sich ändern. Vor Antragstellung "
            "die aktuellen Konditionen auf www.kfw.de bzw. www.bafa.de prüfen."
        ),
    }

    cache.set(cache_key, result, category="kfw")
    return result


# ─── Entry Point ──────────────────────────────────────────────────────

def main():
    """Run the MCP server over stdio."""
    logger.info("Starting Climate CSRD MCP Server v1.0.0")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
