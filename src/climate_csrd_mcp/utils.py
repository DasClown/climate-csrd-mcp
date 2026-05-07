"""
Shared utilities — risk scoring, ESRS mapping, climate scenarios, financial estimates, formatting helpers.
"""

from datetime import date
from math import sqrt
from typing import Any

# ─── Risk Scoring ─────────────────────────────────────────────────────

RISK_LABELS = {
    1: "Very Low",
    2: "Low",
    3: "Medium",
    4: "High",
    5: "Very High",
}

RISK_COLORS = {
    1: "\U0001f7e2",  # 🟢 Green
    2: "\U0001f7e1",  # 🟡 Yellow
    3: "\U0001f7e0",  # 🟠 Orange
    4: "\U0001f534",  # 🔴 Red
    5: "\U0001f525",  # 🔥 Fire
}

# Hazard dimension names (ordered to match weighted model)
HAZARD_DIMENSIONS = ["flood", "heat", "drought", "storm_wind", "sea_level_rise"]

# Default weights for multi-factor model
DEFAULT_WEIGHTS = {
    "flood": 0.30,
    "heat": 0.25,
    "drought": 0.25,
    "storm_wind": 0.10,
    "sea_level_rise": 0.10,
}


def risk_label(score: int) -> str:
    return RISK_LABELS.get(score, "Unknown")


def risk_color(score: int) -> str:
    return RISK_COLORS.get(score, "⚪")


def aggregate_risk(scores: list[int]) -> int:
    """Legacy aggregate: max-based with boost for 3+ dimensions >= 3."""
    if not scores:
        return 1
    max_score = max(scores)
    high_count = sum(1 for s in scores if s >= 3)
    if high_count >= 3 and max_score < 5:
        return min(max_score + 1, 5)
    return max_score


def weighted_aggregate_risk(
    scores_and_weights: list[tuple[int, float]],
) -> dict[str, Any]:
    """
    Weighted multi-factor risk aggregation.

    Args:
        scores_and_weights: List of (score, weight) tuples.
            Each score is 1-5, weights should sum to ~1.0.

    Returns:
        dict with:
            score: 1-5 weighted score (rounded)
            raw: continuous weighted average
            confidence: 'high'|'medium'|'low' based on score variance
            high_risk_dimensions: list of dimension names at score >= 4
            distribution: dict {score: count} for the input scores
            methodology: description string
    """
    if not scores_and_weights:
        return {
            "score": 1,
            "raw": 1.0,
            "confidence": "low",
            "high_risk_dimensions": [],
            "distribution": {},
            "methodology": "Weighted multi-factor model (no data → default score 1)",
        }

    total_weight = sum(w for _, w in scores_and_weights)
    if abs(total_weight - 1.0) > 0.01:
        # Normalise weights so they sum to 1
        scores_and_weights = [
            (s, w / total_weight) for s, w in scores_and_weights
        ]

    # Weighted average
    weighted_sum = sum(s * w for s, w in scores_and_weights)
    raw = weighted_sum

    # Round to nearest integer, clamp to [1, 5]
    score = max(1, min(5, round(raw)))

    # Distribution for context
    distribution: dict[str, int] = {}
    for s, w in scores_and_weights:
        key = f"score_{s}"
        distribution[key] = distribution.get(key, 0) + 1

    # Weighted variance-based confidence
    mean = raw
    variance = sum(w * ((s - mean) ** 2) for s, w in scores_and_weights)
    std_dev = sqrt(variance)

    if std_dev < 0.8:
        confidence = "high"
    elif std_dev < 1.4:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "score": score,
        "raw": round(raw, 2),
        "confidence": confidence,
        "high_risk_dimensions": [],  # caller fills dimension names
        "distribution": distribution,
        "methodology": (
            "Weighted multi-factor model: flood 30%, heat 25%, "
            "drought 25%, storm/wind 10%, sea level rise 10%"
        ),
    }


# ─── Financial Risk Estimates ─────────────────────────────────────────

# Sector-specific loss percentages by overall risk level (1-5)
# Based on literature review: IPCC AR6, EIOPA climate stress test 2022, NGFS scenarios
SECTOR_LOSS_TABLES: dict[str, dict[int, dict[str, float]]] = {
    "manufacturing": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        2: {"mean": 0.3, "lower": 0.1, "upper": 0.6},
        3: {"mean": 0.8, "lower": 0.3, "upper": 1.5},
        4: {"mean": 2.0, "lower": 0.8, "upper": 4.0},
        5: {"mean": 4.5, "lower": 2.0, "upper": 8.0},
    },
    "energy": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        2: {"mean": 0.4, "lower": 0.1, "upper": 0.8},
        3: {"mean": 1.2, "lower": 0.5, "upper": 2.5},
        4: {"mean": 3.0, "lower": 1.0, "upper": 6.0},
        5: {"mean": 6.0, "lower": 2.5, "upper": 12.0},
    },
    "construction": {
        1: {"mean": 0.2, "lower": 0.0, "upper": 0.5},
        2: {"mean": 0.5, "lower": 0.2, "upper": 1.0},
        3: {"mean": 1.5, "lower": 0.5, "upper": 3.0},
        4: {"mean": 3.5, "lower": 1.5, "upper": 7.0},
        5: {"mean": 7.0, "lower": 3.0, "upper": 15.0},
    },
    "transport": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        2: {"mean": 0.3, "lower": 0.1, "upper": 0.8},
        3: {"mean": 0.8, "lower": 0.3, "upper": 2.0},
        4: {"mean": 2.0, "lower": 0.8, "upper": 5.0},
        5: {"mean": 5.0, "lower": 2.0, "upper": 10.0},
    },
    "agriculture": {
        1: {"mean": 0.5, "lower": 0.1, "upper": 1.0},
        2: {"mean": 1.5, "lower": 0.5, "upper": 3.0},
        3: {"mean": 4.0, "lower": 1.5, "upper": 8.0},
        4: {"mean": 8.0, "lower": 3.0, "upper": 15.0},
        5: {"mean": 15.0, "lower": 8.0, "upper": 30.0},
    },
    "real_estate": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        2: {"mean": 0.3, "lower": 0.1, "upper": 0.6},
        3: {"mean": 0.8, "lower": 0.3, "upper": 2.0},
        4: {"mean": 2.5, "lower": 1.0, "upper": 5.0},
        5: {"mean": 5.0, "lower": 2.0, "upper": 12.0},
    },
    "finance": {
        1: {"mean": 0.0, "lower": 0.0, "upper": 0.1},
        2: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        3: {"mean": 0.3, "lower": 0.1, "upper": 0.8},
        4: {"mean": 0.8, "lower": 0.3, "upper": 2.0},
        5: {"mean": 2.0, "lower": 0.8, "upper": 5.0},
    },
    "technology": {
        1: {"mean": 0.0, "lower": 0.0, "upper": 0.1},
        2: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        3: {"mean": 0.3, "lower": 0.1, "upper": 0.8},
        4: {"mean": 1.0, "lower": 0.3, "upper": 2.5},
        5: {"mean": 2.5, "lower": 1.0, "upper": 5.0},
    },
    "healthcare": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.2},
        2: {"mean": 0.2, "lower": 0.1, "upper": 0.5},
        3: {"mean": 0.5, "lower": 0.2, "upper": 1.0},
        4: {"mean": 1.0, "lower": 0.5, "upper": 2.5},
        5: {"mean": 2.5, "lower": 1.0, "upper": 5.0},
    },
    "retail": {
        1: {"mean": 0.1, "lower": 0.0, "upper": 0.3},
        2: {"mean": 0.3, "lower": 0.1, "upper": 0.6},
        3: {"mean": 0.8, "lower": 0.3, "upper": 1.5},
        4: {"mean": 2.0, "lower": 0.8, "upper": 4.0},
        5: {"mean": 4.0, "lower": 2.0, "upper": 8.0},
    },
}

# Default table for sectors not explicitly listed
DEFAULT_SECTOR_LOSS = {
    1: {"mean": 0.2, "lower": 0.0, "upper": 0.4},
    2: {"mean": 0.4, "lower": 0.1, "upper": 0.8},
    3: {"mean": 1.0, "lower": 0.3, "upper": 2.0},
    4: {"mean": 2.5, "lower": 1.0, "upper": 5.0},
    5: {"mean": 5.0, "lower": 2.0, "upper": 10.0},
}


def financial_risk_estimate(
    overall_risk: int,
    sector: str,
    revenue_eur_m: float,
) -> dict[str, Any]:
    """
    Estimate potential financial impact from physical climate risk.

    Args:
        overall_risk: Aggregated climate risk score (1-5)
        sector: Business sector key (e.g. 'manufacturing', 'agriculture')
        revenue_eur_m: Annual revenue in millions of EUR

    Returns:
        dict with annual_loss_pct, annual_loss_eur, confidence_interval, methodology
    """
    loss_table = SECTOR_LOSS_TABLES.get(sector, DEFAULT_SECTOR_LOSS)
    clamped_risk = max(1, min(5, overall_risk))
    estimates = loss_table.get(clamped_risk, loss_table[3])

    annual_loss_pct = estimates["mean"]
    annual_loss_eur = round(revenue_eur_m * (annual_loss_pct / 100), 2)
    lower_eur = round(revenue_eur_m * (estimates["lower"] / 100), 2)
    upper_eur = round(revenue_eur_m * (estimates["upper"] / 100), 2)

    return {
        "annual_loss_pct": annual_loss_pct,
        "annual_loss_eur_m": annual_loss_eur,
        "confidence_interval": {
            "lower_eur_m": lower_eur,
            "upper_eur_m": upper_eur,
            "unit": "€M",
        },
        "methodology": (
            "Sector-specific loss tables based on IPCC AR6, EIOPA 2022 "
            "climate stress test, and NGFS scenario analysis. "
            f"Loss estimates for {sector} at risk level {clamped_risk}: "
            f"mean {annual_loss_pct}% of annual revenue."
        ),
    }


def insurance_premium_estimate(
    lat: float,
    lon: float,
    overall_risk: int,
    sector: str,
) -> dict[str, Any]:
    """
    Estimate business interruption insurance premium range.

    Premium expressed as € per €M of revenue (basis points of revenue).

    Args:
        lat: Latitude (for regional adjustment)
        lon: Longitude (for regional adjustment)
        overall_risk: Aggregated climate risk score (1-5)
        sector: Business sector key

    Returns:
        dict with premium_range_low, premium_range_high, unit, factors
    """
    clamped_risk = max(1, min(5, overall_risk))

    # Base premium rates (€ per €M revenue) by risk level
    base_rates = {1: 500, 2: 1500, 3: 3500, 4: 7500, 5: 15000}

    # Sector multipliers (more exposed sectors pay more)
    sector_multipliers: dict[str, float] = {
        "agriculture": 2.0,
        "construction": 1.8,
        "manufacturing": 1.3,
        "energy": 1.5,
        "transport": 1.2,
        "real_estate": 1.4,
        "retail": 1.1,
        "healthcare": 0.9,
        "finance": 0.6,
        "technology": 0.7,
    }

    sector_mult = sector_multipliers.get(sector, 1.0)

    # Regional multiplier (approximate coastal/mountain adjustment)
    # Coastal areas (near sea) have higher flood/storm risk
    regional_mult = 1.0
    if abs(lat) < 0.5:  # Near equator — higher heat/humidity risk
        regional_mult = 1.15
    elif abs(lat) < 30:  # Subtropical
        regional_mult = 1.1
    elif abs(lat) > 60:  # Nordic — lower heat risk
        regional_mult = 0.85

    # Determine if coastal (approximate: within ~50km of coast affects premium)
    # Simple heuristic: longitude near ocean boundaries
    is_coastal = False
    if -10 <= lon <= 30 and 35 <= lat <= 70:  # European coastal band
        # Major European coastal zones
        coastal_zones = [
            (-10, 2, 48, 52),   # English Channel / North Sea west
            (2, 10, 50, 56),     # North Sea / Denmark
            (8, 14, 54, 58),     # Baltic Sea
            (-10, -2, 36, 44),   # Atlantic coast Spain/Portugal
            (10, 16, 42, 46),    # Adriatic
            (20, 30, 36, 42),    # Eastern Mediterranean
        ]
        for w, e, s, n in coastal_zones:
            if w <= lon <= e and s <= lat <= n:
                is_coastal = True
                break

    if is_coastal:
        regional_mult *= 1.25

    base = base_rates[clamped_risk]
    low = round(base * sector_mult * regional_mult * 0.7, 0)
    high = round(base * sector_mult * regional_mult * 1.3, 0)

    factors = [
        f"Risk level {clamped_risk}: base rate €{base}/€M revenue",
        f"Sector ({sector}): multiplier {sector_mult}",
        f"Regional adjustment: multiplier {regional_mult:.2f}",
    ]
    if is_coastal:
        factors.append("Coastal location: +25% flood/storm premium adjustment")

    return {
        "premium_range_low_eur_per_em_revenue": low,
        "premium_range_high_eur_per_em_revenue": high,
        "unit": "€ per €M revenue",
        "factors": factors,
        "methodology": (
            "Insurance premium estimates based on industry benchmarks "
            "(Marsh, Aon, Swiss Re 2024). Regional adjustments account "
            "for coastal proximity and latitude-based climate exposure."
        ),
    }


# ─── ESRS Mapping ─────────────────────────────────────────────────────

ESRS_STANDARDS = {
    "E1": {
        "title": "ESRS E1 — Climate Change",
        "description": "Climate change mitigation, adaptation, energy",
        "sub_topics": [
            "E1-1: Transition plan for climate change mitigation",
            "E1-2: Climate change mitigation and adaptation policies",
            "E1-3: Targets related to climate change",
            "E1-4: Energy consumption and mix",
            "E1-5: GHG emissions (Scope 1, 2, 3)",
            "E1-6: GHG removals and GHG mitigation projects",
            "E1-7: Financial effects from physical and transition risks",
            "E1-8: Internal carbon pricing",
            "E1-9: Anticipated financial effects from climate opportunities",
        ],
    },
    "E2": {
        "title": "ESRS E2 — Pollution",
        "description": "Air, water, soil pollution, substances of concern",
        "sub_topics": [
            "E2-1: Pollution policies",
            "E2-2: Targets related to pollution",
            "E2-3: Air, water, soil pollutant emissions",
            "E2-4: Substances of concern",
            "E2-5: Financial effects from pollution",
        ],
    },
    "E3": {
        "title": "ESRS E3 — Water and Marine Resources",
        "description": "Water consumption, withdrawals, marine resources",
        "sub_topics": [
            "E3-1: Water policies",
            "E3-2: Targets related to water",
            "E3-3: Water consumption",
            "E3-4: Financial effects from water issues",
        ],
    },
    "E4": {
        "title": "ESRS E4 — Biodiversity and Ecosystems",
        "description": "Biodiversity, ecosystem services, land use",
        "sub_topics": [
            "E4-1: Biodiversity policies",
            "E4-2: Targets related to biodiversity",
            "E4-3: Biodiversity and ecosystem impacts",
            "E4-4: Financial effects from biodiversity",
        ],
    },
    "E5": {
        "title": "ESRS E5 — Resource Use and Circular Economy",
        "description": "Resource inflows/outflows, waste, circularity",
        "sub_topics": [
            "E5-1: Resource use policies",
            "E5-2: Targets related to resource use",
            "E5-3: Resource inflows",
            "E5-4: Resource outflows",
            "E5-5: Financial effects from resource use",
        ],
    },
    "S1": {
        "title": "ESRS S1 — Own Workforce",
        "description": "Working conditions, health & safety, diversity, equal treatment",
        "sub_topics": [
            "S1-1: Policies related to own workforce",
            "S1-2: Processes for engaging own workforce and representatives",
            "S1-3: Remediation of negative impacts on own workforce",
            "S1-4: Taking action on material impacts on own workforce",
            "S1-5: Targets related to managing material negative impacts on own workforce",
            "S1-6: Characteristics of the undertaking's employees",
            "S1-7: Non-employees in own workforce",
            "S1-8: Collective bargaining coverage and social dialogue",
            "S1-9: Diversity metrics",
            "S1-10: Adequate wages",
            "S1-11: Social protection",
            "S1-12: Persons with disabilities",
            "S1-13: Training and skills development metrics",
            "S1-14: Health and safety metrics",
            "S1-15: Work-life balance metrics",
            "S1-16: Remuneration metrics (gender pay gap)",
            "S1-17: Incidents, complaints and severe human rights impacts",
        ],
    },
    "S2": {
        "title": "ESRS S2 — Workers in the Value Chain",
        "description": "Value chain labor practices, human rights",
        "sub_topics": [
            "S2-1: Policies related to value chain workers",
            "S2-2: Processes for engaging value chain workers",
            "S2-3: Remediation of negative impacts on value chain workers",
            "S2-4: Taking action on material impacts on value chain workers",
            "S2-5: Targets related to value chain workers",
        ],
    },
    "S3": {
        "title": "ESRS S3 — Affected Communities",
        "description": "Impact on local communities, indigenous rights, economic impacts",
        "sub_topics": [
            "S3-1: Policies related to affected communities",
            "S3-2: Processes for engaging affected communities",
            "S3-3: Remediation of negative impacts on affected communities",
            "S3-4: Taking action on material impacts on affected communities",
            "S3-5: Targets related to affected communities",
        ],
    },
    "S4": {
        "title": "ESRS S4 — Consumers and End-Users",
        "description": "Consumer safety, privacy, accessibility, responsible marketing",
        "sub_topics": [
            "S4-1: Policies related to consumers and end-users",
            "S4-2: Processes for engaging consumers and end-users",
            "S4-3: Remediation of negative impacts on consumers and end-users",
            "S4-4: Taking action on material impacts on consumers and end-users",
            "S4-5: Targets related to consumers and end-users",
        ],
    },
    "G1": {
        "title": "ESRS G1 — Business Conduct",
        "description": "Corporate governance, ethics, anti-corruption, lobbying",
        "sub_topics": [
            "G1-1: Corporate culture and business conduct policies",
            "G1-2: Supplier relationship management",
            "G1-3: Corruption and bribery prevention",
            "G1-4: Lobbying and political engagement",
            "G1-5: Payment practices",
        ],
    },
}


def get_esrs_ref(standard: str, sub: str = "") -> str:
    """Get formatted ESRS reference."""
    std = ESRS_STANDARDS.get(standard)
    if not std:
        return f"ESRS {standard}"
    if sub:
        return f"**{std['title']}** → {sub}"
    return f"**{std['title']}**"


def get_esrs_data_points(standard: str, topic: str = "") -> list[dict[str, Any]]:
    """
    Get specific disclosure requirements / data points for an ESRS standard or topic.

    Args:
        standard: ESRS standard key (e.g. 'E1', 'S1', 'G1')
        topic: Optional sub-topic to filter (e.g. 'E1-5')

    Returns:
        List of dicts with: id, title, description, data_type, mandatory
    """
    # Comprehensive data point definitions per ESRS standard
    DATA_POINTS: dict[str, list[dict[str, Any]]] = {
        "E1": [
            {
                "id": "E1-1",
                "title": "Transition plan for climate change mitigation",
                "description": "Disclosure of the undertaking's transition plan for climate change mitigation, including scope 1, 2, and 3 GHG emission reduction targets and decarbonisation levers.",
                "data_type": "narrative_and_quantitative",
                "mandatory": True,
            },
            {
                "id": "E1-2",
                "title": "Climate change mitigation and adaptation policies",
                "description": "Policies adopted to manage climate change mitigation and adaptation, including any exclusions from EU Taxonomy delegated acts.",
                "data_type": "narrative",
                "mandatory": True,
            },
            {
                "id": "E1-3",
                "title": "Targets related to climate change",
                "description": "Time-bound climate targets, base year, target year, and progress metrics (e.g., % reduction in GHG emissions vs base year).",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "E1-4",
                "title": "Energy consumption and mix",
                "description": "Total energy consumption in MWh, split by fossil, nuclear, and renewable sources; share of renewable energy.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "E1-5",
                "title": "GHG emissions (Scope 1, 2, 3)",
                "description": "Gross Scope 1, 2 (location & market-based), and 3 GHG emissions in tCO₂e; intensity ratios per €M revenue.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "E1-6",
                "title": "GHG removals and GHG mitigation projects",
                "description": "GHG removals in tCO₂e and related mitigation projects funded through carbon credits.",
                "data_type": "quantitative",
                "mandatory": False,
            },
            {
                "id": "E1-7",
                "title": "Financial effects from physical and transition risks",
                "description": "Anticipated financial effects from material physical and transition climate risks: asset values, revenue exposure, adaptation costs.",
                "data_type": "narrative_and_quantitative",
                "mandatory": True,
            },
            {
                "id": "E1-8",
                "title": "Internal carbon pricing",
                "description": "Internal carbon pricing schemes, price per tonne, scope of application.",
                "data_type": "narrative_and_quantitative",
                "mandatory": False,
            },
            {
                "id": "E1-9",
                "title": "Anticipated financial effects from climate opportunities",
                "description": "Financial effects from material climate opportunities: low-carbon products, energy efficiency savings, green revenues.",
                "data_type": "narrative_and_quantitative",
                "mandatory": False,
            },
        ],
        "S1": [
            {
                "id": "S1-1",
                "title": "Policies related to own workforce",
                "description": "Policies adopted to manage material impacts on own workforce, including human rights policy commitments.",
                "data_type": "narrative",
                "mandatory": True,
            },
            {
                "id": "S1-5",
                "title": "Targets related to own workforce",
                "description": "Time-bound targets for workforce-related material impacts (e.g., injury reduction, diversity targets).",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "S1-6",
                "title": "Characteristics of the undertaking's employees",
                "description": "Headcount by gender, contract type, region; employee turnover rate.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "S1-9",
                "title": "Diversity metrics",
                "description": "Gender distribution at top management, age distribution of employees.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "S1-14",
                "title": "Health and safety metrics",
                "description": "Work-related injuries, fatalities, lost days, and health coverage for own workforce.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "S1-16",
                "title": "Remuneration metrics (gender pay gap)",
                "description": "Gender pay gap (unadjusted) and ratio of annual total remuneration of highest-paid individual to median.",
                "data_type": "quantitative",
                "mandatory": True,
            },
            {
                "id": "S1-17",
                "title": "Incidents, complaints and severe human rights impacts",
                "description": "Number of work-related incidents, discrimination cases, whistleblower reports, and fines.",
                "data_type": "quantitative",
                "mandatory": True,
            },
        ],
        "G1": [
            {
                "id": "G1-1",
                "title": "Corporate culture and business conduct policies",
                "description": "Description of corporate culture, codes of conduct, and business ethics policies.",
                "data_type": "narrative",
                "mandatory": True,
            },
            {
                "id": "G1-3",
                "title": "Corruption and bribery prevention",
                "description": "Anti-corruption policies, procedures, training coverage, and reported incidents/fines.",
                "data_type": "narrative_and_quantitative",
                "mandatory": True,
            },
            {
                "id": "G1-4",
                "title": "Lobbying and political engagement",
                "description": "Political contributions, lobbying activities, and alignment with climate transition goals.",
                "data_type": "narrative",
                "mandatory": True,
            },
            {
                "id": "G1-5",
                "title": "Payment practices",
                "description": "Standard payment terms, late payment rates, and legal proceedings for late payments.",
                "data_type": "quantitative",
                "mandatory": True,
            },
        ],
    }

    standard_data = DATA_POINTS.get(standard, [])
    if topic:
        return [dp for dp in standard_data if dp["id"] == topic]
    return standard_data


def sector_to_nace(sector: str) -> str:
    """
    Map a human-readable sector name to its NACE (European industry classification) code.

    Returns the NACE section letter, or 'Unknown' if no mapping exists.
    """
    # Extended mapping beyond the core one used for rapid lookup
    mapping = {
        "agriculture": "A",
        "farming": "A",
        "forestry": "A",
        "fishing": "A",
        "mining": "B",
        "quarrying": "B",
        "manufacturing": "C",
        "industrial": "C",
        "production": "C",
        "energy": "D",
        "electricity": "D",
        "gas": "D",
        "utilities": "D",
        "water": "E",
        "waste": "E",
        "construction": "F",
        "building": "F",
        "retail": "G",
        "wholesale": "G",
        "trade": "G",
        "transport": "H",
        "logistics": "H",
        "shipping": "H",
        "aviation": "H",
        "accommodation": "I",
        "hospitality": "I",
        "tourism": "I",
        "food_service": "I",
        "technology": "J",
        "it": "J",
        "telecommunications": "J",
        "software": "J",
        "finance": "K",
        "banking": "K",
        "insurance": "K",
        "real_estate": "L",
        "property": "L",
        "scientific": "M",
        "legal": "M",
        "consulting": "M",
        "administrative": "N",
        "public": "O",
        "government": "O",
        "education": "P",
        "healthcare": "Q",
        "medical": "Q",
        "arts": "R",
        "entertainment": "R",
        "sports": "R",
        "other_services": "S",
        "ngo": "S",
    }
    return mapping.get(sector.strip().lower(), "Unknown")


def map_double_materiality(
    sector: str,
    location_risks: dict[str, int] | None = None,
) -> dict[str, list[str]]:
    """
    Map sector and location risks to double materiality assessment.

    Double materiality covers both:
    - Impact materiality: how the company affects the environment/society
    - Financial materiality: how climate/environment affects the company

    Args:
        sector: Business sector key
        location_risks: Optional dict of hazard -> risk score (1-5)

    Returns:
        dict with 'impact_materiality' and 'financial_materiality' lists
    """
    if location_risks is None:
        location_risks = {}

    # Sector-materiality mapping (based on EFRAG implementation guidance)
    SECTOR_IMPACT_MATERIALITY: dict[str, list[str]] = {
        "manufacturing": [
            "E1-5 (GHG emissions)",
            "E2-3 (Pollutant emissions)",
            "E5-3 (Resource inflows)",
            "E5-4 (Resource outflows)",
            "S1-14 (Workforce health & safety)",
        ],
        "energy": [
            "E1-5 (GHG emissions)",
            "E1-4 (Energy mix)",
            "E4-3 (Biodiversity impacts)",
            "E2-3 (Pollutant emissions)",
            "S3-1 (Community impacts)",
        ],
        "construction": [
            "E4-3 (Biodiversity/land use)",
            "E5-3 (Resource consumption)",
            "E1-5 (GHG emissions)",
            "E2-3 (Pollution)",
            "S1-14 (Workplace safety)",
        ],
        "transport": [
            "E1-5 (GHG emissions)",
            "E2-3 (Pollutant emissions)",
            "E1-4 (Energy consumption)",
            "S1-14 (Worker safety)",
            "S3-1 (Community noise/emissions)",
        ],
        "agriculture": [
            "E4-3 (Biodiversity/land use)",
            "E3-3 (Water consumption)",
            "E2-3 (Soil/water pollution)",
            "E1-5 (GHG emissions)",
            "S3-1 (Land rights/communities)",
        ],
        "real_estate": [
            "E1-5 (Building emissions)",
            "E1-4 (Energy consumption)",
            "E5-4 (Waste)",
            "E2-3 (Indoor pollution)",
            "S4-1 (Tenant safety)",
        ],
        "finance": [
            "E1-5 (Financed emissions)",
            "E1-3 (Portfolio alignment)",
            "S1-1 (Human rights due diligence)",
            "G1-3 (Anti-corruption)",
            "G1-4 (Lobbying)",
        ],
        "technology": [
            "E1-5 (Energy/emissions from data centers)",
            "E2-3 (E-waste)",
            "S4-4 (Data privacy)",
            "S1-14 (Ergonomics/health)",
            "G1-1 (AI ethics)",
        ],
        "healthcare": [
            "E2-4 (Substances of concern)",
            "E5-4 (Medical waste)",
            "S1-14 (Workplace safety)",
            "S4-1 (Patient safety)",
            "S3-1 (Access to medicine)",
        ],
        "retail": [
            "E5-3 (Packaging/resources)",
            "E5-4 (Waste)",
            "E1-5 (Logistics emissions)",
            "S2-1 (Supply chain labor)",
            "S4-4 (Consumer safety)",
        ],
    }

    SECTOR_FINANCIAL_MATERIALITY: dict[str, list[str]] = {
        "manufacturing": [
            "E1-7 (Physical risk to facilities)",
            "E1-7 (Supply chain disruption risk)",
            "E1-9 (Green product opportunities)",
            "E5-5 (Resource cost volatility)",
            "E1-8 (Carbon pricing exposure)",
        ],
        "energy": [
            "E1-7 (Transition risk: carbon pricing)",
            "E1-9 (Renewable energy opportunities)",
            "E1-7 (Physical risk to infrastructure)",
            "E4-4 (Regulatory risk from biodiversity)",
            "E2-5 (Pollution liability)",
        ],
        "construction": [
            "E1-7 (Asset value exposure)",
            "E1-7 (Supply chain disruption)",
            "E3-4 (Water availability)",
            "E4-4 (Biodiversity-related permit risk)",
            "E1-9 (Green building premiums)",
        ],
        "transport": [
            "E1-7 (Fuel price volatility)",
            "E1-7 (Infrastructure disruption)",
            "E1-9 (EV/fleet transition opportunities)",
            "E1-8 (Carbon pricing exposure)",
            "E2-5 (Emission zone restrictions)",
        ],
        "agriculture": [
            "E1-7 (Crop yield / physical risk)",
            "E3-4 (Water scarcity costs)",
            "E1-7 (Supply chain disruption)",
            "E1-9 (Regenerative ag opportunities)",
            "E4-4 (Biodiversity compliance costs)",
        ],
        "real_estate": [
            "E1-7 (Physical risk to property values)",
            "E1-7 (Insurance cost increases)",
            "E1-9 (Energy efficiency premiums)",
            "E1-8 (Carbon compliance for buildings)",
            "E3-4 (Water efficiency savings)",
        ],
        "finance": [
            "E1-7 (Credit/physical risk exposure)",
            "E1-7 (Transition risk in portfolio)",
            "E1-9 (Green finance opportunities)",
            "G1-5 (Payment practices)",
            "G1-3 (Corruption risk)",
        ],
        "technology": [
            "E1-7 (Data center energy costs)",
            "E1-9 (Climate tech opportunities)",
            "E5-5 (Rare earth / supply chain)",
            "E1-7 (Regulatory compliance)",
            "S4-5 (Digital accessibility)",
        ],
        "healthcare": [
            "E1-7 (Heat/weather impact on facilities)",
            "E1-7 (Supply chain for medicines)",
            "E1-9 (Climate-health product opportunities)",
            "E2-5 (Pollution compliance costs)",
            "S1-10 (Workforce adequacy wages)",
        ],
        "retail": [
            "E1-7 (Supply chain disruption)",
            "E1-7 (Physical stores exposure)",
            "E1-9 (Sustainable product demand)",
            "E5-5 (Packaging cost volatility)",
            "E1-8 (Carbon pricing in logistics)",
        ],
    }

    impact = SECTOR_IMPACT_MATERIALITY.get(sector, [])
    financial = SECTOR_FINANCIAL_MATERIALITY.get(sector, [])

    # If we have location-level risk data, append location-triggered items
    for hazard, risk_score in location_risks.items():
        if risk_score >= 4:
            financial.append(
                f"E1-7 ({hazard.title()} risk at location — physical risk assessment required)"
            )
        if risk_score >= 3:
            impact.append(
                f"E1-6 / E4-3 (Location impacts from {hazard} exposure)"
            )

    return {
        "impact_materiality": impact,
        "financial_materiality": financial,
    }


# ─── Climate Scenarios (RCP) ──────────────────────────────────────────

RCP_SCENARIOS = {
    "rcp_2.6": {
        "label": "RCP 2.6 — Paris-aligned",
        "description": "Stringent mitigation scenario — global warming likely below 2°C, aiming for 1.5°C by 2100",
        "temp_rise_c": {
            "mean": 1.5,
            "range_low": 1.0,
            "range_high": 2.0,
            "by_2050": 1.3,
            "by_2100": 1.5,
        },
        "co2_concentration_ppm": {
            "by_2050": 440,
            "by_2100": 420,
        },
        "sea_level_rise_cm": {
            "by_2050": 15,
            "by_2100": 35,
        },
        "probability": "Low (requires rapid & sustained emission reductions)",
        "basis": "IPCC AR6 SSP1-1.9 / SSP1-2.6",
    },
    "rcp_4.5": {
        "label": "RCP 4.5 — Moderate",
        "description": "Intermediate scenario — emissions peak around 2040, then decline",
        "temp_rise_c": {
            "mean": 2.7,
            "range_low": 2.0,
            "range_high": 3.5,
            "by_2050": 1.8,
            "by_2100": 2.7,
        },
        "co2_concentration_ppm": {
            "by_2050": 480,
            "by_2100": 540,
        },
        "sea_level_rise_cm": {
            "by_2050": 20,
            "by_2100": 50,
        },
        "probability": "Moderate (current policy trajectory with some acceleration)",
        "basis": "IPCC AR6 SSP2-4.5",
    },
    "rcp_7.0": {
        "label": "RCP 7.0 — High",
        "description": "High emission scenario — continued fossil fuel reliance, emissions double by 2100",
        "temp_rise_c": {
            "mean": 3.6,
            "range_low": 2.8,
            "range_high": 4.5,
            "by_2050": 2.2,
            "by_2100": 3.6,
        },
        "co2_concentration_ppm": {
            "by_2050": 520,
            "by_2100": 700,
        },
        "sea_level_rise_cm": {
            "by_2050": 25,
            "by_2100": 65,
        },
        "probability": "Moderate-high (current NDC trajectory)",
        "basis": "IPCC AR6 SSP3-7.0",
    },
    "rcp_8.5": {
        "label": "RCP 8.5 — Business-as-Usual",
        "description": "High-end baseline scenario — rapid fossil fuel growth with no mitigation",
        "temp_rise_c": {
            "mean": 4.4,
            "range_low": 3.3,
            "range_high": 5.7,
            "by_2050": 2.5,
            "by_2100": 4.4,
        },
        "co2_concentration_ppm": {
            "by_2050": 550,
            "by_2100": 940,
        },
        "sea_level_rise_cm": {
            "by_2050": 30,
            "by_2100": 80,
        },
        "probability": "Low (unlikely but worst-case reference)",
        "basis": "IPCC AR6 SSP5-8.5",
    },
}


def get_rcp_scenario(scenario_key: str) -> dict[str, Any]:
    """Get a specific RCP scenario by key (e.g. 'rcp_4.5')."""
    scenario = RCP_SCENARIOS.get(scenario_key)
    if scenario is None:
        return {"error": f"Unknown scenario: {scenario_key}", "available": list(RCP_SCENARIOS.keys())}
    return scenario


# ─── Supply Chain Risk ────────────────────────────────────────────────

# Regional climate vulnerability scores (1-5) by broad region
REGIONAL_VULNERABILITY: dict[str, dict[str, float | int]] = {
    "northern_europe": {
        "label": "Northern Europe",
        "vulnerability_score": 2,
        "primary_hazards": ["flooding", "heat", "storm"],
        "adaptive_capacity": "high",
    },
    "western_europe": {
        "label": "Western Europe",
        "vulnerability_score": 3,
        "primary_hazards": ["heat", "drought", "flooding"],
        "adaptive_capacity": "high",
    },
    "southern_europe": {
        "label": "Southern Europe",
        "vulnerability_score": 4,
        "primary_hazards": ["heat", "drought", "wildfire"],
        "adaptive_capacity": "medium",
    },
    "eastern_europe": {
        "label": "Eastern Europe",
        "vulnerability_score": 3,
        "primary_hazards": ["heat", "drought", "flooding"],
        "adaptive_capacity": "medium",
    },
    "north_america": {
        "label": "North America",
        "vulnerability_score": 3,
        "primary_hazards": ["hurricane", "wildfire", "heat"],
        "adaptive_capacity": "high",
    },
    "south_america": {
        "label": "South America",
        "vulnerability_score": 4,
        "primary_hazards": ["drought", "flooding", "heat"],
        "adaptive_capacity": "low",
    },
    "south_asia": {
        "label": "South Asia",
        "vulnerability_score": 5,
        "primary_hazards": ["heat", "flooding", "cyclone"],
        "adaptive_capacity": "low",
    },
    "southeast_asia": {
        "label": "Southeast Asia",
        "vulnerability_score": 4,
        "primary_hazards": ["flooding", "typhoon", "heat"],
        "adaptive_capacity": "medium",
    },
    "east_asia": {
        "label": "East Asia",
        "vulnerability_score": 3,
        "primary_hazards": ["typhoon", "flooding", "heat"],
        "adaptive_capacity": "medium",
    },
    "africa": {
        "label": "Africa",
        "vulnerability_score": 5,
        "primary_hazards": ["drought", "heat", "flooding"],
        "adaptive_capacity": "low",
    },
    "oceania": {
        "label": "Oceania",
        "vulnerability_score": 3,
        "primary_hazards": ["heat", "drought", "wildfire"],
        "adaptive_capacity": "high",
    },
}

# Sector-specific supply chain sensitivity multipliers
SECTOR_SC_SENSITIVITY: dict[str, float] = {
    "manufacturing": 1.3,
    "energy": 1.2,
    "construction": 1.4,
    "transport": 1.1,
    "agriculture": 1.5,
    "real_estate": 1.0,
    "finance": 0.7,
    "technology": 1.2,
    "healthcare": 1.3,
    "retail": 1.2,
}


def supply_chain_risk_score(
    sectors: list[str],
    regions: list[str],
) -> dict[str, Any]:
    """
    Analyse supply chain climate risk based on supplier locations and sectors.

    Args:
        sectors: List of sector keys for suppliers (e.g. ['manufacturing', 'agriculture'])
        regions: List of region keys for supplier locations
            (e.g. ['southern_europe', 'south_asia'])

    Returns:
        dict with overall score, regional breakdown, sector breakdown,
        hot spots, and recommendations
    """
    if not sectors and not regions:
        return {
            "error": "At least one sector or region must be provided",
            "overall_score": None,
        }

    # Score each region
    region_scores: list[dict[str, Any]] = []
    for region_key in regions:
        vuln = REGIONAL_VULNERABILITY.get(region_key)
        if vuln is None:
            continue
        region_scores.append({
            "region": region_key,
            "label": vuln["label"],
            "vulnerability_score": vuln["vulnerability_score"],
            "primary_hazards": vuln["primary_hazards"],
            "adaptive_capacity": vuln["adaptive_capacity"],
        })

    # Score each sector
    sector_scores: list[dict[str, Any]] = []
    for sector_key in sectors:
        sensitivity = SECTOR_SC_SENSITIVITY.get(sector_key, 1.0)
        nace = sector_to_nace(sector_key)
        sector_scores.append({
            "sector": sector_key,
            "nace_code": nace,
            "sensitivity_multiplier": sensitivity,
        })

    # Compute overall supply chain risk
    # Average region vulnerability * average sector sensitivity
    if region_scores:
        avg_region_score = sum(
            r["vulnerability_score"] for r in region_scores
        ) / len(region_scores)
    else:
        avg_region_score = 2.0  # default if no regions

    if sector_scores:
        avg_sector_sensitivity = sum(
            s["sensitivity_multiplier"] for s in sector_scores
        ) / len(sector_scores)
    else:
        avg_sector_sensitivity = 1.0

    raw_score = avg_region_score * avg_sector_sensitivity
    overall_score = max(1, min(5, round(raw_score)))

    # Identify hot spots (region + sector combos with highest risk)
    hot_spots: list[dict[str, Any]] = []
    for rs in region_scores:
        if rs["vulnerability_score"] >= 4:
            for ss in sector_scores:
                if ss["sensitivity_multiplier"] >= 1.2:
                    hot_spots.append({
                        "region": rs["label"],
                        "sector": ss["sector"],
                        "combined_risk": max(
                            1, min(5, round(rs["vulnerability_score"] * ss["sensitivity_multiplier"]))
                        ),
                        "hazards": rs["primary_hazards"],
                    })

    # Build recommendations
    recommendations: list[str] = []
    if overall_score >= 4:
        recommendations.append(
            "Critical supply chain risk: develop climate-resilient "
            "sourcing strategy, diversify suppliers away from high-risk regions."
        )
        recommendations.append(
            "Conduct detailed climate vulnerability assessment for each "
            "hot spot supplier location (ESRS E1-7, S2)."
        )
    elif overall_score >= 3:
        recommendations.append(
            "Moderate supply chain risk: assess supplier adaptation plans "
            "and include climate criteria in procurement policies."
        )
    else:
        recommendations.append(
            "Supply chain risk is manageable. Continue monitoring "
            "and include climate risk in annual supplier reviews."
        )

    if hot_spots:
        recommendations.append(
            f"{len(hot_spots)} high-risk supplier combination(s) identified — "
            "prioritise engagement and alternative sourcing."
        )

    return {
        "overall_score": overall_score,
        "raw_score": round(raw_score, 2),
        "label": risk_label(overall_score),
        "color": risk_color(overall_score),
        "region_analysis": region_scores,
        "sector_analysis": sector_scores,
        "hot_spots": hot_spots,
        "recommendations": recommendations,
        "methodology": (
            "Supply chain risk computed as weighted combination of "
            "regional climate vulnerability (IPCC AR6 WGII) and sector-specific "
            "supply chain sensitivity. Hot spots identified where both "
            "region vulnerability >= 4 and sector sensitivity >= 1.2."
        ),
    }


# ─── Date / Disclaimer ────────────────────────────────────────────────

TODAY = date.today().isoformat()

CSRD_DISCLAIMER = (
    f"*Stand: {TODAY}. Dies ist keine geprüfte Berichterstattung. "
    "Die finale Verantwortung für Richtigkeit und Vollständigkeit "
    "liegt beim Unternehmen.*"
)


def today_iso() -> str:
    return TODAY


# ─── Location Validation ──────────────────────────────────────────────

def validate_coordinates(lat: float, lon: float) -> tuple[float, float]:
    """Validate and normalize lat/lon."""
    if not (-90 <= lat <= 90):
        raise ValueError(f"Latitude {lat} out of range [-90, 90]")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Longitude {lon} out of range [-180, 180]")
    return (round(lat, 4), round(lon, 4))


# ─── Sector Mapping (instant lookup) ──────────────────────────────────

SECTOR_NACE_MAP = {
    "manufacturing": "C",
    "energy": "D",
    "construction": "F",
    "transport": "H",
    "agriculture": "A",
    "real_estate": "L",
    "finance": "K",
    "technology": "J",
    "healthcare": "Q",
    "retail": "G",
}

EU_ETS_SECTORS = [
    "power_generation",
    "cement",
    "steel",
    "refineries",
    "chemicals",
    "pulp_paper",
    "glass",
    "ceramics",
    "aviation",
]
