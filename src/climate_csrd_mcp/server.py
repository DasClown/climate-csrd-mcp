"""
Climate CSRD MCP Server — Main entry point.

Implements 14 MCP tools:
  1. assess_climate_risk       — Physical climate risk (flood, heat, drought, storm, SLR, wildfire)
  2. get_emission_benchmarks   — EU ETS + sector emission benchmarks
  3. get_csrd_requirements     — ESRS/CSRD reporting obligations
  4. csrd_report               — Full CSRD-compliant report module
  5. get_kfw_funding           — KfW/BAFA funding programs
  6. compare_sites             — Compare multiple locations side by side
  7. get_carbon_forecast       — EU ETS carbon price projections
  8. get_crrem_pathways        — CRREM real estate decarbonization pathways
  9. get_supply_chain_risk     — Supply chain climate risk assessment
  10. get_climate_synergy      — NDVI/frost/drought data (crop-mcp integration)
  11. get_double_materiality   — ESRS double materiality assessment
  12. get_financial_climate_risk — Financial impact of physical climate risks
  13. get_insurance_estimate   — Business interruption insurance premium ranges
  14. get_funding_check        — EU Taxonomy alignment + funding eligibility
"""

import asyncio
import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from .cache import get_cache
from .utils import (
    aggregate_risk,
    risk_label, risk_color,
    validate_coordinates,
    CSRD_DISCLAIMER, today_iso,
    get_esrs_ref,
    weighted_aggregate_risk,
    financial_risk_estimate,
    insurance_premium_estimate,
    supply_chain_risk_score,
    map_double_materiality,
)
from .data_sources import copernicus as src_cop, dwd as src_dwd, uba as src_uba
from .data_sources import eu_ets as src_ets, eurlex as src_eurlex, kfw as src_kfw

# ─── Load .env ───────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="Climate CSRD Intelligence",
    instructions="Klimarisiko-Analyse und CSRD-Berichterstattung — Copernicus + DWD + EU ETS + UBA + ESRS + CRREM",
    host="127.0.0.1", port=8000,
)

# ═══════════════════════════════════════════════════════════════════════
# TOOL 1: assess_climate_risk (enhanced with storm, SLR, wildfire)
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="assess_climate_risk",
    description="Bewertet das physische Klimarisiko eines Standorts. "
    "Kombiniert Hochwasser, Hitze, Dürre, Sturm, Meeresspiegel, Waldbrand "
    "zu einem gewichteten Gesamt-Risikoscore (1-5).",
)
async def assess_climate_risk(
    lat: float, lon: float, location_name: str = "", year_horizon: int = 2030,
    scenario: str = "rcp_4.5",
) -> dict[str, Any]:
    lat, lon = validate_coordinates(lat, lon)
    cache = get_cache()
    cache_key = cache.make_key("climate_v2", str(lat), str(lon), str(year_horizon), scenario)
    cached = cache.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {lat},{lon}")
        return cached

    logger.info(f"Computing climate risk for {lat},{lon} ({location_name})")

    # Parallel data gathering — 10 sources
    flood_t, drought_t, ndvi_t = src_cop.get_flood_risk(lat, lon), src_cop.get_drought_index(lat, lon), src_cop.get_ndvi(lat, lon)
    storm_t, slr_t, frost_t = src_cop.get_storm_risk(lat, lon), src_cop.get_sea_level_rise_risk(lat, lon, year_horizon), src_cop.get_frost_risk(lat, lon)
    fire_t = src_cop.get_wildfire_risk(lat, lon)
    heat_t = src_dwd.get_hot_day_projection(lat, lon, year_horizon, scenario)
    ref_t = src_dwd.get_climate_reference(lat, lon)
    air_t = src_uba.get_air_quality(lat, lon)

    flood_r, drought_r, ndvi_r, storm_r, slr_r, frost_r, fire_r, heat_r, ref_r, air_r = (
        await asyncio.gather(flood_t, drought_t, ndvi_t, storm_t, slr_t, frost_t, fire_t, heat_t, ref_t, air_t)
    )

    # Extract risk classes
    flood = flood_r["risk_class"]
    drought = drought_r["risk_class"]
    heat_raw = heat_r.get("projected_hot_days_per_year", 15)
    heat = min(heat_raw // 6 + 1, 5)
    storm = storm_r.get("risk_class", 1)
    sea_level = slr_r.get("risk_class", 1)
    fire = fire_r.get("risk_class", 1)

    # Weighted aggregate
    scores_w = [(flood, 0.25), (drought, 0.20), (heat, 0.20), (storm, 0.12), (sea_level, 0.08), (fire, 0.15)]
    weighted = weighted_aggregate_risk(scores_w)
    total_score = weighted["score"]

    result = {
        "location": {"name": location_name or f"{lat:.4f}, {lon:.4f}", "lat": lat, "lon": lon, "year_horizon": year_horizon},
        "flood_risk": {"class": flood, "label": risk_label(flood), "color": risk_color(flood),
                        "is_coastal": flood_r.get("is_coastal", False), "data_source": flood_r.get("data_source", "")},
        "heat_risk": {"class": heat, "label": risk_label(heat), "color": risk_color(heat),
                       "projected_hot_days_per_year": heat_raw, "scenario": scenario,
                       "data_source": heat_r.get("data_source", "")},
        "drought_risk": {"class": drought, "label": risk_label(drought), "color": risk_color(drought),
                          "trend": drought_r.get("trend", "stable"), "data_source": drought_r.get("data_source", "")},
        "storm_risk": {"class": storm, "label": risk_label(storm), "color": risk_color(storm),
                        "zone": storm_r.get("zone", ""), "data_source": storm_r.get("data_source", "")},
        "sea_level_rise_risk": {"class": sea_level, "label": risk_label(sea_level), "color": risk_color(sea_level),
                                 "rise_cm": slr_r.get("sea_level_rise_cm", 0), "data_source": slr_r.get("data_source", "")},
        "wildfire_risk": {"class": fire, "label": risk_label(fire), "color": risk_color(fire),
                           "data_source": fire_r.get("data_source", "")},
        "frost_risk": {"class": frost_r["risk_class"], "label": risk_label(frost_r["risk_class"]),
                        "frost_days": frost_r.get("estimated_frost_days_per_year", 0)},
        "ndvi": ndvi_r["ndvi"],
        "air_quality": {"luq": air_r.get("luq_index", 0), "pm10": air_r.get("pm10_annual_mean_ugm3", 0)},
        "climate_reference": {"temperature_increase_c": ref_r.get("warming_trend", {}).get("temperature_increase_c", 1.3),
                               "nearest_station": ref_r.get("nearest_station", "")},
        "overall_risk": {"score": total_score, "label": risk_label(total_score), "color": risk_color(total_score),
                          "weighted_detail": weighted, "methodology": "Weighted 6-dimension model"},
    }
    cache.set(cache_key, result, category="climate")
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 2: compare_sites
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="compare_sites",
    description="Vergleicht mehrere Standorte hinsichtlich ihres physischen Klimarisikos. "
    "Gibt eine Side-by-Side-Analyse mit Ranking aus.",
)
async def compare_sites(
    sites: list[dict],
    year_horizon: int = 2030,
) -> list[dict[str, Any]]:
    results = await asyncio.gather(*[
        assess_climate_risk(s["lat"], s["lon"], s.get("name", ""), year_horizon)
        for s in sites
    ])
    ranked = sorted(results, key=lambda r: r["overall_risk"]["score"], reverse=True)
    comparisons = []
    for i, r in enumerate(ranked, 1):
        comparisons.append({
            "rank": i,
            "name": r["location"]["name"],
            "overall_score": r["overall_risk"]["score"],
            "overall_label": r["overall_risk"]["label"],
            "flood": r["flood_risk"]["class"],
            "heat": r["heat_risk"]["class"],
            "drought": r["drought_risk"]["class"],
            "storm": r["storm_risk"]["class"],
            "sea_level_rise": r["sea_level_rise_risk"]["class"],
            "wildfire": r["wildfire_risk"]["class"],
            "detail": r,
        })
    return comparisons

# ═══════════════════════════════════════════════════════════════════════
# TOOL 3: get_emission_benchmarks
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_emission_benchmarks",
    description="Holt Branchen-Emissionsbenchmarks aus dem EU ETS "
    "und der EEA-Datenbank. Liefert Durchschnitt, Top/Bottom 10% und Trend.",
)
async def get_emission_benchmarks(sector: str, region: str = "EU") -> dict[str, Any]:
    cache = get_cache()
    cache_key = cache.make_key("benchmarks", sector, region)
    cached = cache.get(cache_key)
    if cached:
        return cached
    ets = await src_ets.get_ets_benchmark(sector, region)
    if "error" not in ets:
        result = {"type": "eu_ets_benchmark", "sector": sector, "data": ets}
        cache.set(cache_key, result, category="emissions")
        return result
    intensity = await src_ets.get_sector_emission_intensity(sector, region)
    if "error" not in intensity:
        result = {"type": "sector_emission_intensity", "sector": sector, "data": intensity}
        cache.set(cache_key, result, category="emissions")
        return result
    return {"error": f"Unknown sector: {sector}"}

# ═══════════════════════════════════════════════════════════════════════
# TOOL 4: get_csrd_requirements
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_csrd_requirements",
    description="Ermittelt ESRS-Offenlegungspflichten nach CSRD. "
    "Basiert auf Art. 3, Sektor-Materialität und Unternehmensgröße.",
)
async def get_csrd_requirements(
    entity_type: str = "large", sector: str = "manufacturing",
    employees: int = 500, revenue: float = 100.0,
) -> dict[str, Any]:
    cache = get_cache()
    cache_key = cache.make_key("csrd", entity_type, sector, str(employees), str(revenue))
    cached = cache.get(cache_key)
    if cached:
        return cached
    result = await src_eurlex.get_csrd_requirements(entity_type=entity_type, sector=sector, employees=employees, revenue=revenue)
    result["disclaimer"] = CSRD_DISCLAIMER
    cache.set(cache_key, result, category="csrd")
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 5: csrd_report
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="csrd_report",
    description="Erstellt einen CSRD-konformen Berichtsbaustein für "
    "einen Standort inkl. Risikoanalyse, Emissionsbenchmark und ESRS-Matrix.",
)
async def csrd_report(
    lat: float, lon: float, sector: str,
    company_name: str = "", site_name: str = "",
    employees: int = 500, revenue: float = 100.0,
    year_horizon: int = 2030, entity_type: str = "large",
    own_emission_intensity: Optional[float] = None,
) -> dict[str, Any]:
    risk_data = await assess_climate_risk(lat, lon, site_name or company_name, year_horizon)
    csrd_data = await get_csrd_requirements(entity_type, sector, employees, revenue)
    bench_data = await get_emission_benchmarks(sector)

    flood_risk = risk_data["flood_risk"]["class"]
    hot_days = risk_data["heat_risk"]["projected_hot_days_per_year"]
    drought_risk = risk_data["drought_risk"]["class"]
    location_triggers = await src_eurlex.get_location_specific_triggers(flood_risk=flood_risk, hot_days=hot_days, drought_index=drought_risk)

    esrs_entries = []
    for std in csrd_data.get("all_applicable_standards", []):
        esrs_entries.append({"standard": std, "title": get_esrs_ref(std), "mandatory": std in csrd_data.get("core_mandatory_standards", []), "location_triggered": False})
    for t in location_triggers:
        for req in t.get("requirements", []):
            esrs_entries.append({"standard": req, "title": req, "mandatory": False, "location_triggered": True, "trigger_reason": t.get("trigger", "")})

    comparison = None
    if own_emission_intensity is not None:
        avg = bench_data.get("data", {}).get("average", 0)
        if avg:
            pct = ((own_emission_intensity - avg) / avg) * 100
            comparison = {"own_intensity": own_emission_intensity, "sector_average": avg, "difference_pct": round(pct, 1), "status": "above_average" if pct > 0 else "below_average"}

    return {
        "report_metadata": {"generated_at": today_iso(), "company": company_name, "site": site_name or f"{lat:.4f},{lon:.4f}", "sector": sector},
        "physical_climate_risk": risk_data,
        "emission_benchmarks": bench_data,
        "benchmark_comparison": comparison,
        "csrd_applicability": {"entity_classification": csrd_data.get("classification", ""), "first_reporting": csrd_data.get("first_reporting", ""), "core_mandatory": csrd_data.get("core_mandatory_standards", [])},
        "esrs_applicability_matrix": esrs_entries,
        "location_specific_triggers": location_triggers,
        "recommendations": _recs(risk_data["overall_risk"]["score"], flood_risk, risk_data["heat_risk"]["class"], drought_risk, sector),
        "disclaimer": CSRD_DISCLAIMER,
    }

def _recs(overall, flood, heat, drought, sector):
    recs = []
    if flood >= 4: recs.append({"priority": "high", "area": "flood", "esrs_ref": "E1-7"})
    elif flood >= 3: recs.append({"priority": "medium", "area": "flood"})
    if heat >= 4: recs.append({"priority": "high", "area": "heat", "esrs_ref": "E1-2, S1-8"})
    if drought >= 4: recs.append({"priority": "high", "area": "water", "esrs_ref": "E3-1, E3-3"})
    if overall >= 4: recs.append({"priority": "high", "area": "adaptation_strategy", "esrs_ref": "E1-1, E1-7"})
    return recs or [{"priority": "low", "area": "monitoring", "esrs_ref": "E1-2"}]

# ═══════════════════════════════════════════════════════════════════════
# TOOL 6: get_kfw_funding
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_kfw_funding",
    description="Findet KfW- und BAFA-Förderprogramme für Klimaschutz- und Anpassungsmaßnahmen.",
)
async def get_kfw_funding(standort_art: str = "produktion", sector: str = "manufacturing", measure: str = "energy_efficiency") -> dict[str, Any]:
    cache = get_cache()
    cache_key = cache.make_key("kfw", standort_art, sector, measure)
    cached = cache.get(cache_key)
    if cached:
        return cached
    programs = await src_kfw.get_funding_programs(standort_art=standort_art, sector=sector, measure=measure)
    result = {"standort_art": standort_art, "sector": sector, "measure": measure, "matching_programs": programs, "program_count": len(programs)}
    cache.set(cache_key, result, category="kfw")
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 7: get_carbon_forecast
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_carbon_forecast",
    description="EU ETS Kohlenstoffpreis-Prognose 2025-2040. "
    "Enthält Jahresprojektionen (min/max/central), Szenarien und Quellen.",
)
async def get_carbon_forecast() -> dict[str, Any]:
    return await src_ets.get_carbon_price_forecast()

# ═══════════════════════════════════════════════════════════════════════
# TOOL 8: get_crrem_pathways
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_crrem_pathways",
    description="CRREM Dekarbonisierungspfade für Immobilien. "
    "Unterstützt 5 Asset-Typen (office, retail, residential, logistics, hotel) "
    "in 15 EU-Ländern mit 3 Szenarien.",
)
async def get_crrem_pathways(
    asset_type: str = "office", country: str = "DE",
    target_year: int = 2030, scenario: str = "1.5c",
    current_intensity: Optional[float] = None,
) -> dict[str, Any]:
    from .data_sources.crrem import get_crrem_pathway, get_crrem_stranding_risk
    pathway = await get_crrem_pathway(asset_type, country, target_year, scenario)
    if current_intensity is not None:
        stranding = await get_crrem_stranding_risk(asset_type, country, current_intensity, target_year)
        pathway["stranding_risk"] = stranding
    return pathway

# ═══════════════════════════════════════════════════════════════════════
# TOOL 9: get_supply_chain_risk
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_supply_chain_risk",
    description="Bewertet Klimarisiken in der Lieferkette basierend "
    "auf Sektoren und Regionen der Lieferanten.",
)
async def get_supply_chain_risk(
    sectors: list[str],
    regions: list[str],
) -> dict[str, Any]:
    return supply_chain_risk_score(sectors, regions)

# ═══════════════════════════════════════════════════════════════════════
# TOOL 10: get_climate_synergy
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_climate_synergy",
    description="Liefert NDVI, Dürre- und Frostdaten für landwirtschaftliche "
    "Anwendungen (crop-mcp Integration). Growing Season Quality Index.",
)
async def get_climate_synergy(lat: float, lon: float) -> dict[str, Any]:
    syn = await src_cop.get_climate_synergy_data(lat, lon)
    syn["crop_mcp_ready"] = True
    return syn

# ═══════════════════════════════════════════════════════════════════════
# TOOL 11: get_double_materiality
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_double_materiality",
    description="ESRS Double Materiality Assessment. "
    "Ermittelt Impact- und Financial-Materiality für einen Sektor "
    "unter Berücksichtigung von Standortrisiken.",
)
async def get_double_materiality(
    sector: str = "manufacturing",
    location_risks: Optional[dict[str, int]] = None,
) -> dict[str, Any]:
    return map_double_materiality(sector, location_risks or {})

# ═══════════════════════════════════════════════════════════════════════
# TOOL 12: get_financial_climate_risk
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_financial_climate_risk",
    description="Schätzt die finanziellen Auswirkungen von physischen "
    "Klimarisiken basierend auf Risikoscore, Sektor und Umsatz.",
)
async def get_financial_climate_risk(
    overall_risk: int, sector: str = "manufacturing",
    annual_revenue_eur_m: float = 100.0,
) -> dict[str, Any]:
    return financial_risk_estimate(overall_risk, sector, annual_revenue_eur_m)

# ═══════════════════════════════════════════════════════════════════════
# TOOL 13: get_insurance_estimate
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_insurance_estimate",
    description="Schätzt die Kosten für Betriebsunterbrechungs-"
    "Versicherung basierend auf Standortrisiko und Branche.",
)
async def get_insurance_estimate(
    lat: float, lon: float,
    overall_risk: int, sector: str = "manufacturing",
) -> dict[str, Any]:
    return insurance_premium_estimate(lat, lon, overall_risk, sector)

# ═══════════════════════════════════════════════════════════════════════
# TOOL 14: get_funding_check
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_funding_check",
    description="Prüft die EU-Taxonomy-Konformität und Fördermittel-"
    "Eignung für Klimaschutz- und Anpassungsmaßnahmen.",
)
async def get_funding_check(
    standort_art: str = "produktion",
    sector: str = "manufacturing",
    measure: str = "energy_efficiency",
    eu_taxonomy_aligned: bool = False,
) -> dict[str, Any]:
    programs = await src_kfw.get_funding_programs(standort_art=standort_art, sector=sector, measure=measure)
    return {
        "standort_art": standort_art, "sector": sector, "measure": measure,
        "eu_taxonomy_aligned": eu_taxonomy_aligned,
        "program_count": len(programs),
        "matching_programs": programs,
        "eu_taxonomy_eligible": eu_taxonomy_aligned or "Requires EU Taxonomy alignment assessment",
        "data_source": "KfW Bankengruppe, BAFA, EU Taxonomy Regulation 2020/852",
    }

# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Run the MCP server over stdio."""
    logger.info("Starting Climate CSRD MCP Server v2.0.0 (14 tools)")
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
