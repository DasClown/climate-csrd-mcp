"""
ESRS E5 — Resource Use and Circular Economy — CSRD Climate MCP.

Provides circular economy metrics per E5-3 (resource inflows) and E5-4
(resource outflows/waste), sector waste benchmarks, circularity scoring,
and full E5 disclosure templates.

Sources:
- ESRS E5 (EU 2023/2772, Annex I)
- EU Taxonomy DNSH criteria (EU 2021/2139)
- Eurostat waste generation statistics
- EC Circular Economy Action Plan
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ─── Sector Benchmarks (Eurostat/CEAP 2024) ───────────────────────────
SECTOR_CIRCULAR_BENCHMARKS: dict[str, dict[str, Any]] = {
    "manufacturing": {"waste_intensity_tons_per_em": 45.0, "recycling_rate_pct": 58.0,
                      "hazardous_waste_share_pct": 8.0, "circular_material_use_rate_pct": 12.5,
                      "renewable_material_share_pct": 15.0,
                      "waste_hierarchy": "Moderate — some landfilling persists"},
    "construction": {"waste_intensity_tons_per_em": 120.0, "recycling_rate_pct": 75.0,
                     "hazardous_waste_share_pct": 3.0, "circular_material_use_rate_pct": 8.0,
                     "renewable_material_share_pct": 10.0,
                     "waste_hierarchy": "Good — high CDW recycling"},
    "energy": {"waste_intensity_tons_per_em": 8.0, "recycling_rate_pct": 72.0,
               "hazardous_waste_share_pct": 25.0, "circular_material_use_rate_pct": 5.0,
               "renewable_material_share_pct": 3.0,
               "waste_hierarchy": "Variable — hazardous waste critical"},
    "agriculture": {"waste_intensity_tons_per_em": 25.0, "recycling_rate_pct": 45.0,
                    "hazardous_waste_share_pct": 1.5, "circular_material_use_rate_pct": 6.0,
                    "renewable_material_share_pct": 60.0,
                    "waste_hierarchy": "Improving — biowaste valorisation rising"},
    "transport": {"waste_intensity_tons_per_em": 5.0, "recycling_rate_pct": 65.0,
                  "hazardous_waste_share_pct": 10.0, "circular_material_use_rate_pct": 3.0,
                  "renewable_material_share_pct": 5.0,
                  "waste_hierarchy": "Moderate — ELV recycling improving"},
    "real_estate": {"waste_intensity_tons_per_em": 6.0, "recycling_rate_pct": 70.0,
                    "hazardous_waste_share_pct": 2.0, "circular_material_use_rate_pct": 4.0,
                    "renewable_material_share_pct": 8.0,
                    "waste_hierarchy": "Good — operational waste recycling mature"},
    "technology": {"waste_intensity_tons_per_em": 2.5, "recycling_rate_pct": 55.0,
                   "hazardous_waste_share_pct": 12.0, "circular_material_use_rate_pct": 2.0,
                   "renewable_material_share_pct": 12.0,
                   "waste_hierarchy": "Moderate — WEEE recycling improving"},
    "retail": {"waste_intensity_tons_per_em": 8.0, "recycling_rate_pct": 50.0,
               "hazardous_waste_share_pct": 3.0, "circular_material_use_rate_pct": 5.0,
               "renewable_material_share_pct": 20.0,
               "waste_hierarchy": "Improving — packaging reduction rising"},
    "finance": {"waste_intensity_tons_per_em": 1.0, "recycling_rate_pct": 60.0,
                "hazardous_waste_share_pct": 1.0, "circular_material_use_rate_pct": 1.0,
                "renewable_material_share_pct": 25.0,
                "waste_hierarchy": "Good — low waste intensity"},
    "healthcare": {"waste_intensity_tons_per_em": 3.5, "recycling_rate_pct": 35.0,
                   "hazardous_waste_share_pct": 18.0, "circular_material_use_rate_pct": 3.0,
                   "renewable_material_share_pct": 10.0,
                   "waste_hierarchy": "Challenging — clinical waste limits recycling"},
}

# ─── Circularity Score Bands ─────────────────────────────────────────
# Thresholds for sub-score scoring (1-5)
CIRCULARITY_BANDS: dict[str, list[float]] = {
    "renewable_pct": [5, 15, 30, 50, 70],
    "recycled_pct": [10, 30, 50, 70, 90],
    "waste_ratio": [100, 50, 20, 10, 5],
}

# ─── EU Taxonomy DNSH Criteria ───────────────────────────────────────
DNSH_CE: dict[str, list[str]] = {
    "manufacturing": ["≥70% non-hazardous waste reused/recycled", "Design for durability/recyclability"],
    "construction": ["≥70% CDW reuse/recycling", "≥15% recycled/reused materials by value"],
    "energy": ["End-of-life mgmt plan for decommissioned infrastructure"],
    "technology": ["WEEE collection ≥65%", "Design for repairability"],
    "default": ["Waste prevention plan", "≥50% non-hazardous waste recycled", "Hazardous waste compliant"],
}


def _score(val: float, thresholds: list[float], higher_better: bool = True) -> int:
    for i, t in enumerate(thresholds):
        if (higher_better and val < t) or (not higher_better and val > t):
            return i + 1
    return 5


def _circ_score(r_pct: float, rec_pct: float, w_ratio: float) -> dict[str, int]:
    return {"overall": max(1, min(5, round(sum([
        _score(r_pct, CIRCULARITY_BANDS["renewable_pct"]),
        _score(rec_pct, CIRCULARITY_BANDS["recycled_pct"]),
        _score(w_ratio, CIRCULARITY_BANDS["waste_ratio"], False)
    ]) / 3))), "inflows": _score(r_pct, CIRCULARITY_BANDS["renewable_pct"]),
             "outflows": _score(rec_pct, CIRCULARITY_BANDS["recycled_pct"]),
             "waste": _score(w_ratio, CIRCULARITY_BANDS["waste_ratio"], False)}


# ═══════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════

async def get_circular_economy_metrics(
    sector: str, revenue_eur_m: float, waste_tons: float,
    recycled_pct: float, renewable_material_pct: float,
) -> dict[str, Any]:
    """
    Calculate circular economy metrics per ESRS E5-3 (resource inflows)
    and E5-4 (resource outflows / waste).

    Returns circular material use rate, waste intensity, hierarchy
    assessment, circularity score, and EU Taxonomy DNSH check.
    """
    await asyncio.sleep(0)
    b = SECTOR_CIRCULAR_BENCHMARKS.get(sector, SECTOR_CIRCULAR_BENCHMARKS["manufacturing"])
    non_renew = round(100.0 - renewable_material_pct, 1)
    cmur = round(recycled_pct * 0.3 + renewable_material_pct * 0.2, 1)
    wi = round(waste_tons / revenue_eur_m, 1) if revenue_eur_m > 0 else 0.0
    non_rec = round(100.0 - recycled_pct, 1)
    hierarchy = ("Good — majority recycled" if recycled_pct >= 70 else
                 "Adequate — significant recycling" if recycled_pct >= 50 else
                 "Developing — opportunities to improve" if recycled_pct >= 30 else
                 "Weak — high reliance on disposal/landfill")
    cs = _circ_score(renewable_material_pct, recycled_pct, wi)
    dnsnsh = DNSH_CE.get(sector, DNSH_CE["default"])

    return {
        "esrs_e5_3_resource_inflows": {"renewable_pct": renewable_material_pct,
                                       "non_renewable_pct": non_renew,
                                       "circular_material_use_rate_pct": cmur,
                                       "sector_benchmark_pct": b["renewable_material_share_pct"]},
        "esrs_e5_4_resource_outflows": {"waste_tons": waste_tons,
                                        "waste_intensity_t_per_em": wi,
                                        "sector_benchmark_t_per_em": b["waste_intensity_tons_per_em"],
                                        "recycled_pct": recycled_pct, "non_recycled_pct": non_rec,
                                        "hazardous_share_pct": b["hazardous_waste_share_pct"],
                                        "waste_hierarchy": hierarchy},
        "circularity_score": {"overall": cs["overall"],
                              "sub_scores": {"inflows": cs["inflows"], "outflows": cs["outflows"],
                                             "waste_mgmt": cs["waste"]}},
        "eu_taxonomy_dnsh": {"criteria": dnsnsh,
                             "note": "Assess against applicable activity criteria for taxonomy alignment."},
    }


async def get_waste_benchmarks(sector: str, region: str = "EU") -> dict[str, Any]:
    """Get waste benchmarks: intensity, recycling rates, hazardous share, hierarchy alignment."""
    await asyncio.sleep(0)
    b = SECTOR_CIRCULAR_BENCHMARKS.get(sector)
    if b is None:
        return {"error": f"Sector '{sector}' not found.",
                "available": list(SECTOR_CIRCULAR_BENCHMARKS.keys())}
    alignment = ("Strong" if b["recycling_rate_pct"] >= 70 else
                 "Moderate" if b["recycling_rate_pct"] >= 50 else "Weak")
    return {
        "sector": sector, "region": region,
        "waste_intensity_t_per_em": b["waste_intensity_tons_per_em"],
        "recycling_rate_pct": b["recycling_rate_pct"],
        "circular_material_use_pct": b["circular_material_use_rate_pct"],
        "hazardous_waste_share_pct": b["hazardous_waste_share_pct"],
        "eu_2030_targets": "70% municipal recycling; 65% packaging; 80% CDW",
        "waste_hierarchy": {
            "alignment": alignment,
            "prevention": "Source reduction measures",
            "reuse": "Preparing for reuse initiatives",
            "recycling": b["waste_hierarchy"],
            "disposal": "Landfill as last resort",
        },
    }


async def get_circularity_score(
    sector: str, renewable_pct: float, recycled_pct: float, waste_ratio: float,
) -> dict[str, Any]:
    """
    Compute circularity score (1-5) with sub-scores, recommendations, targets.

    Args:
        sector: Business sector
        renewable_pct: % renewable material inputs
        recycled_pct: % waste recycled
        waste_ratio: Waste intensity (t/€M revenue)

    Returns:
        dict with scores, recommendations, target guidance
    """
    await asyncio.sleep(0)
    b = SECTOR_CIRCULAR_BENCHMARKS.get(sector, SECTOR_CIRCULAR_BENCHMARKS["manufacturing"])
    s = _circ_score(renewable_pct, recycled_pct, waste_ratio)
    labels = {1: "Very Low", 2: "Low", 3: "Medium", 4: "High", 5: "Very High"}

    recs = []
    if s["inflows"] <= 2:
        recs.append("Increase renewable material share +10pp within 3 years")
    if s["outflows"] <= 2:
        recs.append("Improve product design for recyclability/durability")
    if s["waste"] <= 2:
        recs.append(f"Reduce waste intensity below {b['waste_intensity_tons_per_em']} t/€M")
    if s["overall"] >= 4:
        recs.append("Maintain leadership — explore closed-loop models")
    elif s["overall"] >= 3:
        recs.append("Expand circular procurement and EPR schemes")

    return {
        "overall_score": s["overall"], "label": labels[s["overall"]],
        "sub_scores": {"inflows": {"score": s["inflows"], "desc": "Renewable vs non-renewable inputs"},
                       "outflows": {"score": s["outflows"], "desc": "Product circularity design"},
                       "waste_mgmt": {"score": s["waste"], "desc": "Waste hierarchy performance"}},
        "sector_benchmarks": {"avg_recycling_pct": b["recycling_rate_pct"],
                              "avg_waste_intensity": b["waste_intensity_tons_per_em"],
                              "avg_cmu_pct": b["circular_material_use_rate_pct"]},
        "recommendations": recs,
        "target_guidance": {
            "near_2027": "Match sector recycling average",
            "medium_2030": "70% recycling across all streams",
            "long_2050": "Full circularity — zero landfill, 50%+ renewable inputs",
        },
    }


async def get_esrs_e5_disclosure_template(
    sector: str, company_data_dict: dict[str, Any],
) -> dict[str, Any]:
    """
    Generate full ESRS E5 disclosure template (E5-1 to E5-5).

    Includes applicable metrics by sector, EU Taxonomy DNSH cross-reference,
    and target setting guidance for waste reduction and circularity improvement.

    Args:
        sector: Business sector key
        company_data_dict: Dict with revenue_eur_m, waste_tons, recycled_pct,
                          renewable_pct, hazardous_waste_tons, landfill_pct, incineration_pct

    Returns:
        dict with full disclosure structure
    """
    await asyncio.sleep(0)
    b = SECTOR_CIRCULAR_BENCHMARKS.get(sector, SECTOR_CIRCULAR_BENCHMARKS["manufacturing"])
    rev = company_data_dict.get("revenue_eur_m", 100.0)
    waste = company_data_dict.get("waste_tons", 1000.0)
    rec = company_data_dict.get("recycled_pct", 40.0)
    ren = company_data_dict.get("renewable_pct", 15.0)
    haz = company_data_dict.get("hazardous_waste_tons", waste * 0.05)
    land = company_data_dict.get("landfill_pct", 20.0)
    inc = company_data_dict.get("incineration_pct", 15.0)
    wi = round(waste / rev, 1) if rev > 0 else 0.0
    cmur = round(rec * 0.3 + ren * 0.2, 1)
    haz_pct = round(haz / waste * 100, 1) if waste > 0 else 0.0
    dnsh = DNSH_CE.get(sector, DNSH_CE["default"])
    other = round(100.0 - rec - land - inc, 1)

    return {
        "esrs_ref": "ESRS E5 — Resource Use and Circular Economy (EU 2023/2772)",
        "sections": {
            "E5-1_Policies": {
                "desc": "Resource use and circular economy policies",
                "examples": ["Waste management policy", "Circular procurement policy", "Eco-design policy"],
            },
            "E5-2_Targets": {
                "desc": "Resource use and circular economy targets",
                "examples": [
                    f"Reach {b['recycling_rate_pct']}% recycling by 2027",
                    f"Reduce waste intensity from {wi} to {max(1, round(wi*0.7,1))} t/€M by 2030",
                    "100% reusable/recyclable packaging by 2030",
                ],
            },
            "E5-3_Resource_Inflows": {
                "desc": "Material inputs — renewable vs non-renewable",
                "data": {"renewable_pct": ren, "non_renewable_pct": round(100.0 - ren, 1),
                         "circular_material_use_rate_pct": cmur},
            },
            "E5-4_Resource_Outflows": {
                "desc": "Products, materials, and waste",
                "data": {"total_waste_tons": waste, "waste_intensity_t_per_em": wi,
                         "waste_by_treatment": {"recycling_pct": rec, "landfill_pct": land,
                                                "incineration_pct": inc, "other_pct": other},
                         "hazardous_waste_tons": haz, "hazardous_share_pct": haz_pct},
            },
            "E5-5_Financial_Effects": {
                "desc": "Anticipated financial effects from resource use",
                "note": "May affect raw material costs, waste expenses, and circular product revenue.",
            },
        },
        "eu_taxonomy_dnsh": {"criteria": dnsh,
                             "note": "DNSH to circular economy must be demonstrated for taxonomy alignment."},
        "applicable_metrics": {
            "sector": sector,
            "key_metrics": ["CMUR (%)", "Waste intensity (t/€M)", "Recycling rate (%)", "Renewable share (%)"],
            "sector_specific": b["waste_hierarchy"],
        },
        "target_recommendations": {
            "reduce_waste": f"Target waste intensity ≤{round(wi*0.7,1)} t/€M by 2030",
            "increase_circularity": f"Target CMUR ≥{round(cmur*1.5,1)}% by 2027",
            "zero_landfill": "Target ≤5% landfill by 2035",
        },
        "data_source": "ESRS E5 (EU 2023/2772); EU Taxonomy (EU 2021/2139); Eurostat 2024",
        "disclaimer": "Template for informational use. Actual disclosures must follow ESRS E5 materiality requirements.",
    }
