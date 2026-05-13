"""Climate CSRD MCP Server - Main entry point.

Implements 27 MCP tools:
  1.  assess_climate_risk         - Physical climate risk (flood, heat, drought, storm, SLR, wildfire)
  2.  get_emission_benchmarks     - EU ETS + sector emission benchmarks
  3.  get_csrd_requirements       - ESRS/CSRD reporting obligations
  4.  csrd_report                 - Full CSRD-compliant report module
  5.  get_kfw_funding             - KfW/BAFA funding programs
  6.  compare_sites               - Compare multiple locations side by side
  7.  get_carbon_forecast         - EU ETS carbon price projections
  8.  get_crrem_pathways          - CRREM real estate decarbonization pathways
  9.  get_supply_chain_risk       - Supply chain climate risk assessment
  10. get_climate_synergy         - NDVI/frost/drought data (crop-mcp integration)
  11. get_double_materiality      - ESRS double materiality assessment
  12. get_financial_climate_risk  - Financial impact of physical climate risks
  13. get_insurance_estimate      - Business interruption insurance premium ranges
  14. get_funding_check           - EU Taxonomy alignment + funding eligibility
  15. portfolio_risk              - Portfolio-wide climate risk aggregation and financial exposure
  16. ngfs_scenarios              - NGFS scenario comparison (Net Zero 2050, Below 2C, NDCs, Current Policies)
  17. get_tcfd_report             - TCFD-aligned climate report
  18. get_tnfd_assessment         - TNFD biodiversity/nature LEAP assessment
  19. check_sbti_target           - Validate emission targets against SBTi
  20. calculate_cbam_obligation   - CBAM import carbon cost calculator
  21. get_circular_economy_metrics - ESRS E5 circular economy assessment
  22. get_social_sustainability   - ESRS S1-S4 social assessment
  23. get_csddd_assessment        - CSDDD supply chain due diligence
  24. get_de_specific_assessment  - Germany-specific (BISKO/KSG/LkSG)
  25. get_esg_rating              - ESG rating simulation (MSCI/Sustainalytics)
  26. get_real_estate_assessment  - Real estate energy/renovation
  27. generate_full_report_package - Complete CSRD reporting package
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
from .data_sources import tcfd as src_tcfd
from .data_sources import tnfd as src_tnfd
from .data_sources import sbti as src_sbti
from .data_sources import cbam as src_cbam
from .data_sources import esrs_e5 as src_e5
from .data_sources import esrs_social as src_social
from .data_sources import csddd as src_csddd
from .data_sources import de_specific as src_de
from .data_sources import esg_rating as src_esg
from .data_sources import real_estate as src_re
from .data_sources import auto_report as src_auto

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
    "Unterstützt format='json'|'heatmap'|'table'. Gibt Ranking + visuelle Heatmap.",
)
async def compare_sites(
    sites: list[dict],
    year_horizon: int = 2030,
    format: str = "json",
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
            "overall_color": r["overall_risk"]["color"],
            "flood": r["flood_risk"]["class"],
            "heat": r["heat_risk"]["class"],
            "drought": r["drought_risk"]["class"],
            "storm": r["storm_risk"]["class"],
            "sea_level_rise": r["sea_level_rise_risk"]["class"],
            "wildfire": r["wildfire_risk"]["class"],
            "detail": r,
        })

    # Add heatmap text if requested
    if format in ("heatmap", "table"):
        emoji = {1: "🟢", 2: "🟢", 3: "🟠", 4: "🔴", 5: "🔥"}
        header = f"| {'Rank':<5} | {'Location':<20} | {'🌊 Flood':<8} | {'🌡️ Heat':<8} | {'🏜️ Drought':<8} | {'🌪️ Storm':<8} | {'🔥 Fire':<8} | {'⭐ Score':<8} |"
        sep = "|" + ":" + "-"*4 + ":" + "|" + ":" + "-"*19 + ":" + "|:" + "-"*6 + ":|:" + "-"*6 + ":|:" + "-"*6 + ":|:" + "-"*6 + ":|:" + "-"*6 + ":|:" + "-"*6 + ":|"
        rows = []
        for c in comparisons:
            rows.append(f"| {c['rank']:<5} | {c['name'][:20]:<20} | {emoji.get(c['flood'],'')} {c['flood']:<4} | {emoji.get(c['heat'],'')} {c['heat']:<4} | {emoji.get(c['drought'],'')} {c['drought']:<4} | {emoji.get(c['storm'],'')} {c['storm']:<4} | {emoji.get(c['wildfire'],'')} {c['wildfire']:<4} | {emoji.get(c['overall_score'],'')} {c['overall_score']:<4} |")
        heatmap_text = "## 🌍 Standort-Vergleich (Heatmap)\n\n" + header + "\n" + sep + "\n" + "\n".join(rows)
        return {"comparisons": comparisons, "heatmap": heatmap_text, "format": format}

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
# TOOL 15: portfolio_risk
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="portfolio_risk",
    description="Bewertet das Klimarisiko eines gesamten Standort-Portfolios. "
    "Aggregiert Einzelrisiken, berechnet finanzielle Exposure und priorisiert Massnahmen.",
)
async def portfolio_risk(
    sites: list[dict],
    total_portfolio_revenue_eur_m: float = 1000.0,
    year_horizon: int = 2030,
) -> dict[str, Any]:
    """
    Assess climate risk across a portfolio of sites.

    Each site dict: {"name": str, "lat": float, "lon": float, "sector": str, "revenue_share_pct": float}
    revenue_share_pct is the site's share of total portfolio revenue (0-100).
    """
    if not sites:
        return {"error": "No sites provided", "portfolio_summary": {}}

    # Normalise revenue shares to ensure they sum to 100%
    total_share = sum(s.get("revenue_share_pct", 0) for s in sites)
    if total_share == 0:
        # Equal weights if no revenue shares provided
        for s in sites:
            s["revenue_share_pct"] = 100.0 / len(sites)
        total_share = 100.0

    # Run climate risk assessment for all sites in parallel
    risk_tasks = [
        assess_climate_risk(
            s["lat"], s["lon"],
            s.get("name", f"Site_{i}"),
            year_horizon,
        )
        for i, s in enumerate(sites)
    ]
    risk_results = await asyncio.gather(*risk_tasks)

    # Compute per-site analysis with financial exposure
    site_analyses = []
    risk_scores = []
    total_financial_exposure = 0.0

    for i, s in enumerate(sites):
        risk = risk_results[i]
        overall_score = risk["overall_risk"]["score"]
        site_revenue = total_portfolio_revenue_eur_m * (s.get("revenue_share_pct", 0) / 100.0)

        # Financial risk estimate for this site
        fin = financial_risk_estimate(overall_score, s.get("sector", "manufacturing"), site_revenue)

        site_analyses.append({
            "name": s.get("name", risk["location"]["name"]),
            "lat": s["lat"],
            "lon": s["lon"],
            "sector": s.get("sector", "unknown"),
            "revenue_share_pct": s.get("revenue_share_pct", 0),
            "site_revenue_eur_m": round(site_revenue, 2),
            "overall_risk_score": overall_score,
            "overall_risk_label": risk["overall_risk"]["label"],
            "overall_risk_color": risk["overall_risk"]["color"],
            "flood_risk": risk["flood_risk"]["class"],
            "heat_risk": risk["heat_risk"]["class"],
            "drought_risk": risk["drought_risk"]["class"],
            "storm_risk": risk["storm_risk"]["class"],
            "sea_level_rise_risk": risk["sea_level_rise_risk"]["class"],
            "wildfire_risk": risk["wildfire_risk"]["class"],
            "financial_risk": {
                "annual_loss_pct": fin["annual_loss_pct"],
                "annual_loss_eur_m": fin["annual_loss_eur_m"],
                "confidence_interval": fin["confidence_interval"],
            },
            "detail": risk,
        })
        risk_scores.append(overall_score)
        total_financial_exposure += fin["annual_loss_eur_m"]

    # Sort by risk score descending (highest risk first)
    site_analyses.sort(key=lambda x: x["overall_risk_score"], reverse=True)

    # Portfolio-level aggregation
    weighted_avg_risk = sum(
        a["overall_risk_score"] * (a["revenue_share_pct"] / 100.0)
        for a in site_analyses
    )
    portfolio_risk_score = max(1, min(5, round(weighted_avg_risk)))

    # Risk distribution
    risk_distribution = {}
    for score in range(1, 6):
        count = sum(1 for a in site_analyses if a["overall_risk_score"] == score)
        if count > 0:
            risk_distribution[f"level_{score}"] = count

    # Critical sites (top 3 or any with score >= 4)
    critical_sites = [a for a in site_analyses if a["overall_risk_score"] >= 4]
    if len(critical_sites) < 3:
        critical_sites = site_analyses[:min(3, len(site_analyses))]

    # Revenue-weighted financial exposure
    revenue_at_risk_pct = 0.0
    for a in site_analyses:
        if a["overall_risk_score"] >= 3:
            revenue_at_risk_pct += a["revenue_share_pct"]

    # Recommendations
    recommendations = []
    if portfolio_risk_score >= 4:
        recommendations.append({
            "priority": "critical",
            "action": "Sofortige Klimaanpassungsstrategie fuer das gesamte Portfolio erforderlich",
            "esrs_ref": "E1-1, E1-7",
        })
    if portfolio_risk_score >= 3:
        recommendations.append({
            "priority": "high",
            "action": "Detaillierte Klimarisikoanalyse fuer alle Standorte mit Score >= 3 durchfuehren",
            "esrs_ref": "E1-2, E1-7",
        })
    if critical_sites:
        site_names = ", ".join(s["name"] for s in critical_sites[:3])
        recommendations.append({
            "priority": "high",
            "action": f"Standorte mit hoechstem Risiko priorisieren: {site_names}",
            "esrs_ref": "E1-7",
        })
    if total_financial_exposure > total_portfolio_revenue_eur_m * 0.05:
        recommendations.append({
            "priority": "high",
            "action": "Finanzielles Exposure ueberschreitet 5% des Portfolioumsatzes - Absicherung pruefen",
            "esrs_ref": "E1-7, E1-9",
        })
    if revenue_at_risk_pct > 30:
        recommendations.append({
            "priority": "medium",
            "action": f"Ueber {revenue_at_risk_pct:.0f}% des Umsatzes in Standorten mit mittlerem bis hohem Risiko",
            "esrs_ref": "E1-7",
        })
    if not recommendations:
        recommendations.append({
            "priority": "low",
            "action": "Portfolio-Risiko ist gering. Regelmaessiges Monitoring empfohlen",
            "esrs_ref": "E1-2",
        })

    result = {
        "portfolio_summary": {
            "total_sites": len(sites),
            "portfolio_risk_score": portfolio_risk_score,
            "portfolio_risk_label": risk_label(portfolio_risk_score),
            "portfolio_risk_color": risk_color(portfolio_risk_score),
            "weighted_average_risk": round(weighted_avg_risk, 2),
            "total_portfolio_revenue_eur_m": total_portfolio_revenue_eur_m,
            "total_financial_exposure_eur_m": round(total_financial_exposure, 2),
            "exposure_pct_of_revenue": round((total_financial_exposure / total_portfolio_revenue_eur_m * 100) if total_portfolio_revenue_eur_m > 0 else 0, 2),
            "revenue_at_risk_pct": round(revenue_at_risk_pct, 1),
            "year_horizon": year_horizon,
        },
        "risk_distribution": risk_distribution,
        "financial_exposure": {
            "total_annual_loss_estimate_eur_m": round(total_financial_exposure, 2),
            "methodology": "Sector-specific loss tables based on IPCC AR6, EIOPA 2022, NGFS scenarios",
            "unit": "EUR million per year",
        },
        "critical_sites": [
            {
                "rank": idx + 1,
                "name": s["name"],
                "sector": s["sector"],
                "overall_risk_score": s["overall_risk_score"],
                "overall_risk_label": s["overall_risk_label"],
                "financial_exposure_eur_m": s["financial_risk"]["annual_loss_eur_m"],
                "revenue_share_pct": s["revenue_share_pct"],
            }
            for idx, s in enumerate(critical_sites[:5])
        ],
        "site_analyses": site_analyses,
        "recommendations": recommendations,
        "methodology": (
            "Portfolio risk computed as revenue-weighted average of individual site climate risk scores. "
            "Financial exposure uses sector-specific loss functions from IPCC AR6 and EIOPA climate stress tests. "
            "Sites are assessed using the full 6-dimension physical risk model (flood, heat, drought, storm, SLR, wildfire)."
        ),
        "disclaimer": CSRD_DISCLAIMER,
    }
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 16: ngfs_scenarios
# ═══════════════════════════════════════════════════════════════════════

# NGFS scenario to RCP mapping
NGFS_TO_RCP = {
    "net_zero_2050": "rcp_2.6",
    "below_2c": "rcp_4.5",
    "ndcs": "rcp_7.0",
    "current_policies": "rcp_8.5",
}

NGFS_SCENARIO_LABELS = {
    "net_zero_2050": "Net Zero 2050 (NZ2050)",
    "below_2c": "Below 2C (B2C)",
    "ndcs": "Nationally Determined Contributions (NDCs)",
    "current_policies": "Current Policies (CP)",
}


@mcp.tool(
    name="ngfs_scenarios",
    description="Vergleicht Klimarisiken ueber verschiedene NGFS-Szenarien "
    "(Net Zero 2050, Below 2C, NDCs, Current Policies).",
)
async def ngfs_scenarios(
    lat: float, lon: float,
    location_name: str = "",
    year_horizon: int = 2050,
    revenue_exposure_eur_m: float = 0.0,
) -> dict[str, Any]:
    """
    Compare climate risks across four NGFS scenarios.

    Maps NGFS scenarios to RCP pathways:
      - Net Zero 2050 (NZ2050)  -> RCP 2.6  (Paris-aligned)
      - Below 2C (B2C)          -> RCP 4.5  (Moderate)
      - NDCs                    -> RCP 7.0  (High)
      - Current Policies (CP)   -> RCP 8.5  (Business-as-Usual)
    """
    lat, lon = validate_coordinates(lat, lon)
    cache = get_cache()
    cache_key = cache.make_key("ngfs", str(lat), str(lon), str(year_horizon))
    cached = cache.get(cache_key)
    if cached:
        logger.info(f"NGFS cache HIT for {lat},{lon}")
        return cached

    logger.info(f"Computing NGFS scenario comparison for {lat},{lon}")

    # Run all four NGFS scenarios in parallel
    scenario_tasks = {}
    for ngfs_key, rcp_key in NGFS_TO_RCP.items():
        scenario_tasks[ngfs_key] = assess_climate_risk(
            lat, lon,
            location_name=location_name or f"{lat:.4f}, {lon:.4f}",
            year_horizon=year_horizon,
            scenario=rcp_key,
        )

    # Gather all results
    scenario_results = {}
    for ngfs_key, task in scenario_tasks.items():
        scenario_results[ngfs_key] = await task

    # Build scenario comparison table
    scenarios = []
    for ngfs_key in NGFS_TO_RCP:
        rcp_key = NGFS_TO_RCP[ngfs_key]
        result = scenario_results[ngfs_key]
        overall = result["overall_risk"]

        # Get RCP metadata from utils
        rcp_info = {
            "rcp_key": rcp_key,
            "label": "",
            "temp_rise_c": 0,
            "co2_concentration_ppm": 0,
        }
        try:
            from .utils import get_rcp_scenario
            rcp_data = get_rcp_scenario(rcp_key)
            if "error" not in rcp_data:
                rcp_info["label"] = rcp_data.get("label", "")
                rcp_info["temp_rise_c"] = rcp_data.get("temp_rise_c", {}).get("mean", 0)
                rcp_info["co2_concentration_ppm"] = rcp_data.get("co2_concentration_ppm", {}).get("by_2050" if year_horizon >= 2050 else "by_2050", 0)
        except Exception:
            pass

        scenarios.append({
            "ngfs_scenario": ngfs_key,
            "ngfs_label": NGFS_SCENARIO_LABELS.get(ngfs_key, ngfs_key),
            "rcp_scenario": rcp_key,
            "rcp_info": rcp_info,
            "overall_risk_score": overall["score"],
            "overall_risk_label": overall["label"],
            "overall_risk_color": overall["color"],
            "flood_risk": result["flood_risk"]["class"],
            "heat_risk": {
                "class": result["heat_risk"]["class"],
                "projected_hot_days": result["heat_risk"]["projected_hot_days_per_year"],
            },
            "drought_risk": result["drought_risk"]["class"],
            "storm_risk": result["storm_risk"]["class"],
            "sea_level_rise_risk": {
                "class": result["sea_level_rise_risk"]["class"],
                "rise_cm": result["sea_level_rise_risk"].get("rise_cm", 0),
            },
            "wildfire_risk": result["wildfire_risk"]["class"],
            "detail": result,
        })

    # Find best and worst case
    scenarios_sorted = sorted(scenarios, key=lambda s: s["overall_risk_score"])
    best_case = scenarios_sorted[0]
    worst_case = scenarios_sorted[-1]

    # Risk spread analysis
    risk_spread = worst_case["overall_risk_score"] - best_case["overall_risk_score"]
    has_significant_spread = risk_spread >= 2

    # Identify hazards with most scenario sensitivity
    hazard_sensitivity = []
    for hazard_name in ["flood", "heat_risk", "drought", "storm", "sea_level_rise", "wildfire"]:
        values = []
        for s in scenarios:
            v = s.get(hazard_name, {})
            if isinstance(v, dict):
                val = v.get("class", 0)
            else:
                val = v
            values.append(val)
        if values:
            spread = max(values) - min(values)
            if spread >= 1:
                hazard_sensitivity.append({
                    "hazard": hazard_name.replace("_risk", "").replace("_", " ").title(),
                    "min_score": min(values),
                    "max_score": max(values),
                    "spread": spread,
                })

    hazard_sensitivity.sort(key=lambda h: h["spread"], reverse=True)

    # Recommendations
    recommendations = []
    if risk_spread >= 2:
        recommendations.append({
            "priority": "high",
            "action": f"Grosse Szenario-Abhaengigkeit ({risk_spread} Stufen) - "
                      "Klimarisiko stark von Emissionspfad abhaengig. Ambitionierte "
                      "Klimapolitik reduziert Risiko signifikant.",
            "esrs_ref": "E1-1, E1-7",
        })
    if worst_case["overall_risk_score"] >= 4:
        recommendations.append({
            "priority": "high",
            "action": f"Selbst im besten Szenario ({best_case['ngfs_label']}) "
                      "besteht erhoehtes Risiko. Anpassungsmassnahmen dringend empfohlen.",
            "esrs_ref": "E1-2, E1-7",
        })
    if hazard_sensitivity:
        top_hazard = hazard_sensitivity[0]["hazard"]
        recommendations.append({
            "priority": "medium",
            "action": f"Hoechste Szenario-Sensitivitaet bei '{top_hazard}' - "
                      "Monitoring und fruehzeitige Anpassung empfohlen.",
            "esrs_ref": "E1-2",
        })
    if not recommendations:
        recommendations.append({
            "priority": "low",
            "action": "Geringe Szenario-Varianz. Risiko ist robust ueber alle Pfade. Regelmaessige Ueberpruefung empfohlen.",
            "esrs_ref": "E1-2",
        })

    result = {
        "location": {
            "lat": lat,
            "lon": lon,
            "name": location_name or f"{lat:.4f}, {lon:.4f}",
            "year_horizon": year_horizon,
        },
        "scenario_comparison": scenarios,
        "best_case": {
            "scenario": best_case["ngfs_label"],
            "overall_risk_score": best_case["overall_risk_score"],
            "overall_risk_label": best_case["overall_risk_label"],
        },
        "worst_case": {
            "scenario": worst_case["ngfs_label"],
            "overall_risk_score": worst_case["overall_risk_score"],
            "overall_risk_label": worst_case["overall_risk_label"],
        },
        "risk_spread": {
            "difference": risk_spread,
            "has_significant_spread": has_significant_spread,
            "interpretation": (
                f"Der Risikounterschied zwischen {best_case['ngfs_label']} "
                f"und {worst_case['ngfs_label']} betraegt {risk_spread} Stufen."
            ),
        },
        "hazard_sensitivity": hazard_sensitivity,
        "recommendations": recommendations,
        "methodology": (
            "NGFS scenarios mapped to IPCC RCP pathways: "
            "NZ2050->RCP2.6, B2C->RCP4.5, NDCs->RCP7.0, CP->RCP8.5. "
            "Each scenario runs the full 6-dimension physical climate risk model. "
            "Scenario comparison based on Network for Greening the Financial System (NGFS) Phase 4 scenarios."
        ),
        "disclaimer": CSRD_DISCLAIMER,
    }
    cache.set(cache_key, result, category="ngfs")
    return result

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
# TOOL 17: get_tcfd_report
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_tcfd_report",
    description="Generates a TCFD-aligned climate report covering governance, "
    "strategy, risk management, and metrics/targets for climate-related financial disclosures.",
)
async def get_tcfd_report(
    company_name: str,
    sector: str = "manufacturing",
    revenue_eur_m: float = 0.0,
    risk_score: int = 3,
    flood_risk: int = 1,
    heat_risk: int = 1,
    drought_risk: int = 1,
) -> dict[str, Any]:
    """
    Generate a TCFD-aligned climate report covering the four TCFD pillars:
    governance, strategy, risk management, metrics and targets.
    """
    result = await asyncio.to_thread(
        src_tcfd.get_tcfd_report,
        company_name=company_name,
        sector=sector,
        revenue_eur_m=revenue_eur_m,
        risk_score=risk_score,
        flood_risk=flood_risk,
        heat_risk=heat_risk,
        drought_risk=drought_risk,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 18: get_tnfd_assessment
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_tnfd_assessment",
    description="Performs a TNFD-aligned LEAP (Locate, Evaluate, Assess, Prepare) "
    "biodiversity and nature-related risk assessment for a company or portfolio.",
)
async def get_tnfd_assessment(
    sector: str = "manufacturing",
    regions: list[str] | None = None,
    proximity_score: int = 3,
) -> dict[str, Any]:
    """
    Perform a TNFD-aligned LEAP assessment for nature-related risks.
    Covers Locate, Evaluate, Assess, and Prepare phases.
    """
    if regions is None:
        regions = ["global"]
    result = await asyncio.to_thread(
        src_tnfd.get_tnfd_report,
        sector=sector,
        regions=regions,
        proximity_score=proximity_score,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 19: check_sbti_target
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="check_sbti_target",
    description="Validates corporate emission reduction targets against "
    "Science Based Targets initiative (SBTi) criteria and sector-specific pathways.",
)
async def check_sbti_target(
    company_emissions: float,
    sector: str = "manufacturing",
    target_year: int = 2030,
    base_year: int = 2020,
    base_year_emissions: float = 0.0,
    temperature_goal: str = "1.5c",
) -> dict[str, Any]:
    """
    Validate corporate emission targets against SBTi criteria.
    Checks if targets are aligned with Paris Agreement temperature goals.
    """
    result = await asyncio.to_thread(
        src_sbti.check_sbti_target,
        company_emissions=company_emissions,
        sector=sector,
        target_year=target_year,
        base_year=base_year,
        base_year_emissions=base_year_emissions,
        temperature_goal=temperature_goal,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 20: calculate_cbam_obligation
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="calculate_cbam_obligation",
    description="Calculates CBAM (Carbon Border Adjustment Mechanism) "
    "obligations for imported goods, including carbon cost and certificate requirements.",
)
async def calculate_cbam_obligation(
    import_goods_tons: float,
    embedded_emissions: float,
    origin_country: str = "cn",
    sector: str = "cement",
    carbon_price_origin: float = 0.0,
) -> dict[str, Any]:
    """
    Calculate CBAM obligations for imported goods.
    Returns embedded emissions, certificate requirements, and total carbon cost.
    """
    result = await asyncio.to_thread(
        src_cbam.calculate_cbam_obligation,
        import_goods_tons=import_goods_tons,
        embedded_emissions=embedded_emissions,
        origin_country=origin_country,
        sector=sector,
        carbon_price_origin=carbon_price_origin,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 21: get_circular_economy_metrics
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_circular_economy_metrics",
    description="Assesses ESRS E5 circular economy performance including "
    "waste management, recycling rates, and renewable material usage.",
)
async def get_circular_economy_metrics(
    sector: str = "manufacturing",
    revenue_eur_m: float = 0.0,
    waste_tons: float = 0.0,
    recycled_pct: float = 0.0,
    renewable_material_pct: float = 0.0,
) -> dict[str, Any]:
    """
    Assess circular economy metrics aligned with ESRS E5.
    Evaluates waste, recycling, and renewable material circularity.
    """
    result = await src_e5.get_circular_economy_metrics(
        sector=sector,
        revenue_eur_m=revenue_eur_m,
        waste_tons=waste_tons,
        recycled_pct=recycled_pct,
        renewable_material_pct=renewable_material_pct,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 22: get_social_sustainability
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_social_sustainability",
    description="Assesses ESRS S1-S4 social sustainability including workforce "
    "indicators, turnover, injury rates, and gender pay equity.",
)
async def get_social_sustainability(
    sector: str = "manufacturing",
    employee_count: int = 0,
    turnover_rate: float = 0.0,
    injury_rate: float = 0.0,
    gender_pay_gap: float = 0.0,
) -> dict[str, Any]:
    """
    Assess social sustainability metrics aligned with ESRS S1-S4.
    Covers workforce, value chain, affected communities, and consumers.
    """
    result = await src_social.get_social_sustainability_score(
        sector=sector,
        employee_count=employee_count,
        turnover_rate=turnover_rate,
        injury_rate=injury_rate,
        gender_pay_gap=gender_pay_gap,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 23: get_csddd_assessment
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_csddd_assessment",
    description="Assesses CSDDD (Corporate Sustainability Due Diligence Directive) "
    "supply chain human rights and environmental due diligence obligations.",
)
async def get_csddd_assessment(
    sector: str = "manufacturing",
    regions: list[str] | None = None,
    revenue_eur_m: float = 0.0,
) -> dict[str, Any]:
    """
    Assess CSDDD supply chain due diligence obligations.
    Evaluates human rights and environmental risks across the supply chain.
    """
    if regions is None:
        regions = ["eu"]
    result = await src_csddd.get_csddd_due_diligence(
        sector=sector,
        regions=regions,
        revenue_eur_m=revenue_eur_m,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 24: get_de_specific_assessment
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_de_specific_assessment",
    description="Germany-specific sustainability assessments: BISKO (municipal "
    "carbon accounting), KSG (Climate Protection Act compliance), or LkSG "
    "(Supply Chain Due Diligence Act).",
)
async def get_de_specific_assessment(
    category: str = "bisko",
    sector: str = "manufacturing",
    energy_consumption_mwh: float = 0.0,
    target_year: int = 2045,
    reduction_pct: float = 0.0,
    regions: list[str] | None = None,
) -> dict[str, Any]:
    """
    Germany-specific sustainability assessment routing.
    Supports BISKO (municipal carbon accounting), KSG (Climate Protection Act),
    and LkSG (Supply Chain Due Diligence Act).
    """
    if regions is None:
        regions = ["de"]
    category = category.lower()
    if category == "bisko":
        result = await src_de.get_bisko_assessment(
            sector=sector,
            energy_consumption_mwh=energy_consumption_mwh,
        )
    elif category == "ksg":
        result = await src_de.get_ksg_compliance(
            sector=sector,
            energy_consumption_mwh=energy_consumption_mwh,
            target_year=target_year,
            reduction_pct=reduction_pct,
        )
    elif category == "lksg":
        result = await src_de.get_lksg_supply_chain_duty(
            sector=sector,
            regions=regions,
        )
    else:
        result = {"error": f"Unknown category '{category}'. Use 'bisko', 'ksg', or 'lksg'."}
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 25: get_esg_rating
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_esg_rating",
    description="Simulates ESG ratings using MSCI and/or Sustainalytics "
    "methodologies based on sector, emissions intensity, and risk profile.",
)
async def get_esg_rating(
    sector: str = "manufacturing",
    emissions_intensity: float = 0.0,
    risk_score: int = 3,
    methodology: str = "both",
) -> dict[str, Any]:
    """
    Simulate ESG ratings using MSCI and/or Sustainalytics methodologies.
    Returns rating scores, category labels, and improvement recommendations.
    """
    result = {}
    methodology = methodology.lower()
    if methodology in ("msci", "both"):
        msci_result = await asyncio.to_thread(
            src_esg.simulate_msci_rating,
            sector=sector,
            emissions_intensity=emissions_intensity,
            risk_score=risk_score,
        )
        result["msci"] = msci_result
    if methodology in ("sustainalytics", "both"):
        sust_result = await asyncio.to_thread(
            src_esg.simulate_sustainalytics_rating,
            sector=sector,
            emissions_intensity=emissions_intensity,
            risk_score=risk_score,
        )
        result["sustainalytics"] = sust_result
    result["methodology"] = methodology
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 26: get_real_estate_assessment
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="get_real_estate_assessment",
    description="Real estate sustainability assessment: energy performance "
    "certificates, renovation roadmaps, KfW efficiency house, or EPC benchmarks.",
)
async def get_real_estate_assessment(
    category: str = "energy_cert",
    building_type: str = "office",
    country: str = "de",
    current_class: str = "d",
    target_class: str = "a",
    efficiency_class: str = "kfw_55",
) -> dict[str, Any]:
    """
    Real estate sustainability assessment routing.
    Supports energy certificates, renovation roadmaps, KfW efficiency house, and EPC benchmarks.
    """
    category = category.lower()
    if category == "energy_cert":
        result = await src_re.get_energy_certificate(
            building_type=building_type,
            country=country,
            current_class=current_class,
        )
    elif category == "renovation_roadmap":
        result = await src_re.get_renovation_roadmap(
            building_type=building_type,
            country=country,
            current_class=current_class,
            target_class=target_class,
        )
    elif category == "kfw":
        result = await src_re.get_kfw_efficiency_house(
            building_type=building_type,
            efficiency_class=efficiency_class,
        )
    elif category == "epc_benchmarks":
        result = await src_re.get_epc_benchmarks(
            building_type=building_type,
            country=country,
        )
    else:
        result = {
            "error": f"Unknown category '{category}'. Use 'energy_cert', 'renovation_roadmap', 'kfw', or 'epc_benchmarks'."
        }
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# TOOL 27: generate_full_report_package
# ═══════════════════════════════════════════════════════════════════════

@mcp.tool(
    name="generate_full_report_package",
    description="Generates a complete CSRD reporting package including "
    "climate risk, ESRS materiality, emission benchmarks, and executive summary.",
)
async def generate_full_report_package(
    site_lat: float,
    site_lon: float,
    company_name: str = "Example GmbH",
    sector: str = "manufacturing",
    employees: int = 500,
    revenue: float = 100.0,
) -> dict[str, Any]:
    """
    Generate a complete CSRD reporting package combining climate risk analysis,
    ESRS applicability, emission benchmarks, and an executive summary.
    """
    result = await src_auto.generate_full_report_package(
        site_lat=site_lat,
        site_lon=site_lon,
        company_name=company_name,
        sector=sector,
        employees=employees,
        revenue=revenue,
    )
    result["disclaimer"] = CSRD_DISCLAIMER
    return result

# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Run the MCP server over stdio."""
    logger.info("Starting Climate CSRD MCP Server v2.0.0 (27 tools)")
    mcp.run(transport="stdio")


# ═══════════════════════════════════════════════════════════════════════
# HELPER: _prepare_report_text
# ═══════════════════════════════════════════════════════════════════════

def _prepare_report_text(report: dict) -> str:
    """
    Format a csrd_report() output dict as a structured markdown report.

    Args:
        report: The dict returned by the csrd_report tool.

    Returns:
        Formatted multi-line markdown text suitable for PDF export.
    """
    lines = []
    meta = report.get("report_metadata", {})
    risk = report.get("physical_climate_risk", {})
    esrs = report.get("esrs_applicability_matrix", [])
    recs = report.get("recommendations", [])
    disclaimer = report.get("disclaimer", "")
    csrd = report.get("csrd_applicability", {})
    bench = report.get("emission_benchmarks", {})
    comp = report.get("benchmark_comparison", {})
    triggers = report.get("location_specific_triggers", [])

    # ── Header ──
    lines.append("# CSRD Climate Risk Report")
    lines.append("")
    lines.append(f"**Company:** {meta.get('company', 'N/A')}")
    lines.append(f"**Site:** {meta.get('site', 'N/A')}")
    lines.append(f"**Sector:** {meta.get('sector', 'N/A')}")
    lines.append(f"**Report Date:** {meta.get('generated_at', 'N/A')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Physical Risk Summary ──
    lines.append("## 1. Physical Climate Risk Summary")
    lines.append("")
    overall = risk.get("overall_risk", {})
    loc = risk.get("location", {})
    lines.append(f"**Location:** {loc.get('name', 'N/A')} ({loc.get('lat', '?')}, {loc.get('lon', '?')})")
    lines.append(f"**Horizon:** {loc.get('year_horizon', 'N/A')}")
    lines.append("")

    risk_emoji = {1: "🟢 Very Low", 2: "🟡 Low", 3: "🟠 Medium", 4: "🔴 High", 5: "🔥 Very High"}
    overall_score = overall.get("score", 0)
    overall_label = overall.get("label", "Unknown")
    lines.append(f"**Overall Risk Score:** {overall_score}/5 - {risk_emoji.get(overall_score, overall_label)}")
    lines.append("")

    # Hazard table (emoji-style)
    hazards = [
        ("Flood", risk.get("flood_risk", {}).get("class", 0)),
        ("Heat", risk.get("heat_risk", {}).get("class", 0)),
        ("Drought", risk.get("drought_risk", {}).get("class", 0)),
        ("Storm", risk.get("storm_risk", {}).get("class", 0)),
        ("Sea Level Rise", risk.get("sea_level_rise_risk", {}).get("class", 0)),
        ("Wildfire", risk.get("wildfire_risk", {}).get("class", 0)),
    ]
    for name, score in hazards:
        emoji = risk_emoji.get(score, "⚪")
        lines.append(f"- **{name}:** {emoji} (Score {score}/5)")

    lines.append("")
    methodology = overall.get("methodology", "")
    if methodology:
        lines.append(f"*Methodology: {methodology}*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Financial Impact ──
    lines.append("## 2. Financial Exposure")
    lines.append("")
    # If the report has financial data, note it
    if comp:
        diff = comp.get("difference_pct", 0)
        status = comp.get("status", "unknown")
        own = comp.get("own_intensity", 0)
        avg = comp.get("sector_average", 0)
        lines.append(f"- **Own Emission Intensity:** {own} tCO2e/Em Revenue")
        lines.append(f"- **Sector Average:** {avg} tCO2e/Em Revenue")
        lines.append(f"- **Difference:** {diff}% ({'above' if diff > 0 else 'below'} sector average)")
        lines.append("")
    lines.append("---")
    lines.append("")

    # ── ESRS Applicability Matrix ──
    lines.append("## 3. ESRS Applicability Matrix")
    lines.append("")
    if csrd:
        lines.append(f"- **Entity Classification:** {csrd.get('entity_classification', 'N/A')}")
        lines.append(f"- **First Reporting:** {csrd.get('first_reporting', 'N/A')}")
        lines.append("")
    if esrs:
        lines.append("| Standard | Title | Mandatory | Location Triggered |")
        lines.append("|----------|-------|-----------|--------------------|")
        for entry in esrs:
            std = entry.get("standard", "")
            title = entry.get("title", "")
            mandatory = "✅ Yes" if entry.get("mandatory") else "No"
            loc_trig = "📍 Yes" if entry.get("location_triggered") else ""
            lines.append(f"| {std} | {title} | {mandatory} | {loc_trig} |")
    else:
        lines.append("No ESRS entries found.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Location-Specific Triggers ──
    if triggers:
        lines.append("## 4. Location-Specific Triggers")
        lines.append("")
        for t in triggers:
            trigger = t.get("trigger", "")
            reqs = t.get("requirements", [])
            lines.append(f"- **{trigger}** → {', '.join(reqs)}")
        lines.append("")
        lines.append("---")
        lines.append("")

    # ── Recommendations ──
    lines.append("## 5. Recommendations")
    lines.append("")
    if recs:
        for r in recs:
            priority = r.get("priority", "low")
            area = r.get("area", "")
            esrs_ref = r.get("esrs_ref", "")
            priority_emoji = {"high": "🔴", "medium": "🟠", "low": "🟢", "critical": "🔥"}
            emoji = priority_emoji.get(priority, "⚪")
            lines.append(f"- {emoji} **Priority: {priority.upper()}** - Area: {area}")
            if esrs_ref:
                lines.append(f"  - ESRS Reference: {esrs_ref}")
    else:
        lines.append("No specific recommendations generated.")
    lines.append("")

    # ── Data Sources ──
    lines.append("## 6. Data Sources")
    lines.append("")
    lines.append("- Copernicus Climate Data Store (flood, drought, storm, SLR, wildfire, NDVI)")
    lines.append("- DWD OpenData (heat days, climate reference)")
    lines.append("- EU ETS / EEA (emission benchmarks)")
    lines.append("- UBA Umweltbundesamt (air quality)")
    lines.append("- EUR-Lex (ESRS standards, CSRD requirements)")
    lines.append("- KfW / BAFA (funding programs)")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Disclaimer ──
    if disclaimer:
        lines.append(f"*{disclaimer}*" if disclaimer.startswith("Stand") else disclaimer)
    else:
        lines.append("*Disclaimer: This report is for informational purposes only. Not audited or verified.*")

    return "\n".join(lines)


if __name__ == "__main__":
    main()
