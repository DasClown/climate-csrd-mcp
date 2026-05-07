"""
EUR-Lex / ESRS Standards client.

Provides ESRS (European Sustainability Reporting Standards) requirements
mapped by entity type, sector, and company size.

Sources:
- EUR-Lex: Regulation (EU) 2023/2772 (Delegated Regulation)
- ESRS 1: General requirements
- ESRS 2: General, strategy, governance, materiality
- Topic-specific ESRS: E1-E5, S1-S4, G1
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─── CSRD Thresholds (Art. 3 Directive 2022/2464) ────────────────────

CSRD_THRESHOLDS = {
    "large": {
        "description": "Large undertaking — full CSRD",
        "conditions": "Meets 2 of 3: >250 employees, >€50M revenue, >€25M assets",
        "required_esrs": ["ESRS_2", "E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"],
        "core_mandatory": ["E1", "ESRS_2"],
        "first_reporting": "FY 2025 (for FY 2024 for public-interest entities already under NFRD)",
    },
    "listed_sme": {
        "description": "Listed SME — proportionate CSRD",
        "conditions": "Listed on EU regulated market, <250 employees",
        "required_esrs": ["ESRS_2", "LSME_ESRS"],
        "core_mandatory": ["ESRS_2"],
        "first_reporting": "FY 2026 (opt-out until 2028 possible)",
    },
    "non_eu_group": {
        "description": "Non-EU group with EU presence",
        "conditions": ">€150M revenue in EU, or has EU subsidiary/branch exceeding thresholds",
        "required_esrs": ["ESRS_2", "E1", "E2", "E3", "E4", "E5", "S1", "G1"],
        "core_mandatory": ["ESRS_2", "E1"],
        "first_reporting": "FY 2028",
    },
}

# ─── Sector-Specific ESRS Materiality ────────────────────────────────
# Which ESRS topics are typically material by sector. Based on EFRAG
# implementation guidance and sector-specific materiality assessments.

SECTOR_MATERIALITY: dict[str, list[str]] = {
    "manufacturing": ["E1", "E2", "E5", "S1"],
    "energy": ["E1", "E2", "E4", "S1"],
    "construction": ["E1", "E3", "E4", "E5", "S1"],
    "transport": ["E1", "E2", "S1"],
    "agriculture": ["E1", "E3", "E4", "E5", "S1", "S2"],
    "real_estate": ["E1", "E3", "E4"],
    "finance": ["E1", "E4", "S1", "G1"],
    "technology": ["E1", "E2", "E5", "S1", "G1"],
    "healthcare": ["E1", "E2", "S1", "S3"],
    "retail": ["E1", "E2", "E5", "S1", "S2"],
}

# ─── Location-Specific ESRS Triggers ─────────────────────────────────
# Physical climate risk triggers additional ESRS E1 reporting

CLIMATE_RISK_TRIGGERS = {
    "flood_zone": {
        "risk_threshold": 4,
        "additional_requirements": [
            "E1-7: Financial effects from physical climate risks (flooding)",
            "E1-2: Climate adaptation policies (site-specific)",
            "E4-3: Biodiversity impacts from flood protection measures",
        ],
    },
    "heat_risk": {
        "threshold_hot_days": 20,
        "additional_requirements": [
            "E1-7: Financial effects from heat-related productivity loss",
            "S1-8: Occupational health & safety (heat stress)",
            "E1-3: Heat adaptation targets",
        ],
    },
    "water_stress": {
        "threshold_drought_index": 3,
        "additional_requirements": [
            "E3-1: Water policies (water-stressed region)",
            "E3-3: Detailed water consumption reporting",
            "E3-4: Financial effects from water scarcity",
        ],
    },
}


async def get_csrd_requirements(
    entity_type: str = "large",
    sector: str = "manufacturing",
    employees: int = 500,
    revenue: float = 100.0,
) -> dict:
    """
    Get CSRD / ESRS reporting requirements for an entity.

    Determines which ESRS standards apply based on:
    - Entity type (large, listed SME, non-EU group)
    - Sector (determines materiality)
    - Size thresholds

    Args:
        entity_type: 'large', 'listed_sme', or 'non_eu_group'
        sector: Business sector
        employees: Number of employees
        revenue: Annual revenue in €M

    Returns:
        dict with applicable ESRS standards and requirements
    """
    # Determine entity classification
    classification = CSRD_THRESHOLDS.get(entity_type, CSRD_THRESHOLDS["large"])

    # Get sector-specific material topics
    material_topics = SECTOR_MATERIALITY.get(sector, ["E1", "S1"])

    # Build requirement list
    all_requirements = list(classification["required_esrs"])
    core = list(classification["core_mandatory"])

    # Sector-specific additions
    for topic in material_topics:
        if topic not in all_requirements:
            all_requirements.append(topic)

    return {
        "entity_type": entity_type,
        "sector": sector,
        "classification": classification["description"],
        "conditions": classification["conditions"],
        "core_mandatory_standards": core,
        "all_applicable_standards": all_requirements,
        "sector_material_topics": material_topics,
        "first_reporting": classification["first_reporting"],
        "data_source": "EUR-Lex: Directive 2022/2464 (CSRD) + Regulation 2023/2772 (ESRS)",
        "size_details": {
            "employees": employees,
            "revenue_eur_m": revenue,
        },
    }


async def get_location_specific_triggers(
    flood_risk: int = 2,
    hot_days: int = 10,
    drought_index: int = 2,
) -> list[dict]:
    """
    Get location-specific ESRS triggers based on physical climate risk.

    Args:
        flood_risk: Flood risk class (1-5)
        hot_days: Projected annual hot days
        drought_index: Drought risk index (1-5)

    Returns:
        List of additional ESRS requirements triggered by location risk
    """
    triggers = []

    if flood_risk >= CLIMATE_RISK_TRIGGERS["flood_zone"]["risk_threshold"]:
        triggers.append({
            "trigger": "High flood risk",
            "severity": flood_risk,
            "requirements": CLIMATE_RISK_TRIGGERS["flood_zone"]["additional_requirements"],
        })

    if hot_days >= CLIMATE_RISK_TRIGGERS["heat_risk"]["threshold_hot_days"]:
        triggers.append({
            "trigger": "High heat stress risk",
            "severity": min(hot_days // 10 + 1, 5),
            "requirements": CLIMATE_RISK_TRIGGERS["heat_risk"]["additional_requirements"],
        })

    if drought_index >= CLIMATE_RISK_TRIGGERS["water_stress"]["threshold_drought_index"]:
        triggers.append({
            "trigger": "Water stress region",
            "severity": drought_index,
            "requirements": CLIMATE_RISK_TRIGGERS["water_stress"]["additional_requirements"],
        })

    return triggers
