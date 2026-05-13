"""
ESG Rating Simulation Module
=============================

Provides simulated ESG ratings approximating MSCI and Sustainalytics methodologies.
Designed for the CSRD climate MCP server to generate realistic ESG assessments
when actual rating data is unavailable.

Key methodologies approximated:
    - MSCI ESG Ratings (AAA–CCC, 0–10 score)
    - Sustainalytics ESG Risk Ratings (Negligible–Severe, 0–100 risk score)

Data sources referenced:
    - MSCI ESG Ratings Methodology (2023)
    - Sustainalytics ESG Risk Ratings Methodology (2023)

All outputs are simulated estimates based on sector, emissions intensity,
climate risk, and controversy inputs — NOT actual third-party ratings.
"""

from __future__ import annotations

import math
import statistics
from typing import Any


# ═══════════════════════════════════════════════════════════════════════════════
# Constants & Configuration Data
# ═══════════════════════════════════════════════════════════════════════════════

# MSCI ESG rating scale: letter grade → numerical score (0–10)
# Mapping based on MSCI's published letter-to-score equivalents.
MSCI_RATING_SCALE: dict[str, float] = {
    "AAA": 10.0,  # Leader — best-in-class ESG management
    "AA": 8.5,    # Leader — strong ESG management
    "A": 7.0,     # Average — moderate ESG management
    "BBB": 5.5,   # Average — adequate ESG management
    "BB": 4.0,    # Laggard — weak ESG management
    "B": 2.5,     # Laggard — very weak ESG management
    "CCC": 1.0,   # Laggard — worst-in-class ESG management
}

# Reverse: numerical score range → MSCI letter grade
# Each grade covers a band; float scores interpolate between thresholds.
MSCI_SCORE_THRESHOLDS: list[tuple[float, str]] = [
    (9.25, "AAA"),
    (7.75, "AA"),
    (6.25, "A"),
    (4.75, "BBB"),
    (3.25, "BB"),
    (1.75, "B"),
    (0.0, "CCC"),
]

# Sustainalytics ESG Risk Rating categories and score ranges.
# Lower scores = lower unmanaged risk (better).
SUSTAINALYTICS_CATEGORIES: list[dict[str, Any]] = [
    {"name": "Negligible", "min": 0, "max": 10},
    {"name": "Low",       "min": 10, "max": 20},
    {"name": "Medium",    "min": 20, "max": 30},
    {"name": "High",      "min": 30, "max": 40},
    {"name": "Severe",    "min": 40, "max": 100},
]

# Sector-level ESG pillar weights.
# Reflects how much each pillar contributes to overall ESG exposure.
# Manufacturing: heavy environmental footprint, moderate social, moderate governance.
# Finance: moderate environmental (financed emissions), moderate social, high governance.
SECTOR_ESG_WEIGHTS: dict[str, dict[str, float]] = {
    "manufacturing":  {"E": 0.40, "S": 0.35, "G": 0.25},
    "finance":        {"E": 0.30, "S": 0.30, "G": 0.40},
    "energy":         {"E": 0.45, "S": 0.30, "G": 0.25},
    "technology":     {"E": 0.25, "S": 0.35, "G": 0.40},
    "healthcare":     {"E": 0.20, "S": 0.45, "G": 0.35},
    "real_estate":    {"E": 0.35, "S": 0.30, "G": 0.35},
    "transportation": {"E": 0.40, "S": 0.30, "G": 0.30},
    "agriculture":    {"E": 0.45, "S": 0.30, "G": 0.25},
    "retail":         {"E": 0.30, "S": 0.35, "G": 0.35},
    "default":        {"E": 0.33, "S": 0.33, "G": 0.34},
}

# MSCI Key Issues by sector with weightings (sums to 1.0 per sector).
# These represent the material ESG issues MSCI tracks for each industry.
MSCI_KEY_ISSUES: dict[str, list[dict[str, Any]]] = {
    "manufacturing": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.20},
        {"issue": "Toxic Emissions & Waste",    "pillar": "E", "weight": 0.15},
        {"issue": "Water Stress",               "pillar": "E", "weight": 0.10},
        {"issue": "Labor Management",           "pillar": "S", "weight": 0.15},
        {"issue": "Health & Safety",            "pillar": "S", "weight": 0.12},
        {"issue": "Supply Chain Labor",         "pillar": "S", "weight": 0.08},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.12},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.08},
    ],
    "finance": [
        {"issue": "Carbon Emissions (Financed)","pillar": "E", "weight": 0.12},
        {"issue": "Environmental Opportunities","pillar": "E", "weight": 0.10},
        {"issue": "Access to Finance",          "pillar": "S", "weight": 0.15},
        {"issue": "Privacy & Data Security",    "pillar": "S", "weight": 0.12},
        {"issue": "Human Capital",              "pillar": "S", "weight": 0.10},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.20},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.12},
        {"issue": "Systemic Risk Management",   "pillar": "G", "weight": 0.09},
    ],
    "energy": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.25},
        {"issue": "Biodiversity & Land Use",    "pillar": "E", "weight": 0.12},
        {"issue": "Water Stress",               "pillar": "E", "weight": 0.10},
        {"issue": "Health & Safety",            "pillar": "S", "weight": 0.15},
        {"issue": "Community Relations",        "pillar": "S", "weight": 0.12},
        {"issue": "Labor Management",           "pillar": "S", "weight": 0.08},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.10},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.08},
    ],
    "technology": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.08},
        {"issue": "E-Waste Management",         "pillar": "E", "weight": 0.10},
        {"issue": "Privacy & Data Security",    "pillar": "S", "weight": 0.22},
        {"issue": "Human Capital",              "pillar": "S", "weight": 0.15},
        {"issue": "Supply Chain Labor",         "pillar": "S", "weight": 0.10},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.18},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.12},
        {"issue": "Innovation Management",      "pillar": "G", "weight": 0.05},
    ],
    "healthcare": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.08},
        {"issue": "Water Stress",               "pillar": "E", "weight": 0.06},
        {"issue": "Access to Healthcare",       "pillar": "S", "weight": 0.20},
        {"issue": "Product Safety & Quality",   "pillar": "S", "weight": 0.18},
        {"issue": "Human Capital",              "pillar": "S", "weight": 0.12},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.15},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.12},
        {"issue": "Privacy & Data Security",    "pillar": "G", "weight": 0.09},
    ],
    "real_estate": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.18},
        {"issue": "Energy Efficiency",          "pillar": "E", "weight": 0.15},
        {"issue": "Sustainable Construction",   "pillar": "E", "weight": 0.10},
        {"issue": "Community Relations",        "pillar": "S", "weight": 0.12},
        {"issue": "Affordable Housing",         "pillar": "S", "weight": 0.10},
        {"issue": "Occupational Safety",        "pillar": "S", "weight": 0.08},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.15},
        {"issue": "Risk Management",            "pillar": "G", "weight": 0.12},
    ],
    "transportation": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.25},
        {"issue": "Air Quality",                "pillar": "E", "weight": 0.12},
        {"issue": "Fuel Efficiency",            "pillar": "E", "weight": 0.10},
        {"issue": "Safety Management",          "pillar": "S", "weight": 0.18},
        {"issue": "Labor Relations",            "pillar": "S", "weight": 0.10},
        {"issue": "Community Impact",           "pillar": "S", "weight": 0.05},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.12},
        {"issue": "Regulatory Compliance",      "pillar": "G", "weight": 0.08},
    ],
    "agriculture": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.18},
        {"issue": "Water Stress",               "pillar": "E", "weight": 0.15},
        {"issue": "Biodiversity & Land Use",    "pillar": "E", "weight": 0.14},
        {"issue": "Labor Management",           "pillar": "S", "weight": 0.15},
        {"issue": "Supply Chain Labor",         "pillar": "S", "weight": 0.10},
        {"issue": "Food Safety",                "pillar": "S", "weight": 0.08},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.12},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.08},
    ],
    "retail": [
        {"issue": "Carbon Emissions",           "pillar": "E", "weight": 0.10},
        {"issue": "Packaging & Waste",          "pillar": "E", "weight": 0.12},
        {"issue": "Supply Chain Management",    "pillar": "S", "weight": 0.20},
        {"issue": "Labor Management",           "pillar": "S", "weight": 0.15},
        {"issue": "Product Safety",             "pillar": "S", "weight": 0.08},
        {"issue": "Corporate Governance",       "pillar": "G", "weight": 0.15},
        {"issue": "Business Ethics",            "pillar": "G", "weight": 0.12},
        {"issue": "Privacy & Data Security",    "pillar": "G", "weight": 0.08},
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Internal Helper Functions
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_sector(sector: str) -> str:
    """Normalize a sector string to our internal keys.

    Falls back to 'default' if no match is found, allowing any
    input to produce a reasonable rating estimate.
    """
    s = sector.lower().strip().replace(" ", "_")
    if s in SECTOR_ESG_WEIGHTS:
        return s
    # Try fuzzy matching for common alternatives
    mapping = {
        "tech": "technology",
        "it": "technology",
        "software": "technology",
        "bank": "finance",
        "banking": "finance",
        "insurance": "finance",
        "industrial": "manufacturing",
        "industrials": "manufacturing",
        "auto": "manufacturing",
        "automotive": "manufacturing",
        "pharma": "healthcare",
        "pharmaceutical": "healthcare",
        "biotech": "healthcare",
        "logistics": "transportation",
        "shipping": "transportation",
        "farming": "agriculture",
        "food": "agriculture",
    }
    return mapping.get(s, "default")


def _get_pillar_weights(sector: str) -> dict[str, float]:
    """Retrieve ESG pillar weights for a given sector."""
    key = _normalize_sector(sector)
    return SECTOR_ESG_WEIGHTS.get(key, SECTOR_ESG_WEIGHTS["default"])


def _get_key_issues(sector: str) -> list[dict[str, Any]]:
    """Retrieve MSCI key issues for a given sector, or default if unknown."""
    key = _normalize_sector(sector)
    return MSCI_KEY_ISSUES.get(key, MSCI_KEY_ISSUES["manufacturing"])


def _clamp(value: float, lo: float, hi: float) -> float:
    """Clamp a value between lo and hi (inclusive)."""
    return max(lo, min(hi, value))


def _score_to_msci_letter(score: float) -> str:
    """Convert a numerical ESG score (0–10) to an MSCI letter grade (AAA–CCC).

    Uses the thresholds defined in MSCI_SCORE_THRESHOLDS, where each grade
    spans a band. The highest threshold the score meets determines the grade.
    """
    clamped_score = _clamp(score, 0.0, 10.0)
    for threshold, grade in MSCI_SCORE_THRESHOLDS:
        if clamped_score >= threshold:
            return grade
    return "CCC"  # Should not be reached due to clamp


def _score_to_sustainalytics_category(risk_score: float) -> dict[str, Any]:
    """Map a Sustainalytics risk score (0–100) to its risk category.

    Returns the category dict with name, min, and max boundaries.
    """
    clamped = _clamp(risk_score, 0.0, 100.0)
    for cat in SUSTAINALYTICS_CATEGORIES:
        if cat["min"] <= clamped < cat["max"]:
            return cat
    return SUSTAINALYTICS_CATEGORIES[-1]  # Severe (40–100)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — Controversy Assessment
# ═══════════════════════════════════════════════════════════════════════════════

def get_esg_controversy_score(risk_score: float, sector: str) -> dict[str, Any]:
    """Estimate ESG controversy level based on risk score and sector.

    Controversy assessment models how severe ongoing ESG-related controversies
    are for the entity. Higher risk scores and certain sectors (energy, mining)
    tend to face more severe controversies.

    Parameters
    ----------
    risk_score : float
        Climate or overall ESG risk score (0–100 scale).
    sector : str
        Industry sector (e.g., 'manufacturing', 'energy', 'finance').

    Returns
    -------
    dict
        - level (int): Controversy level 1–5 (1=minor, 5=severe)
        - label (str): Human-readable label
        - breakdown (dict): Scores by pillar (environmental, social, governance)
        - severity (float): Estimated severity 0–10
        - scope (str): Geographic scope (local, regional, global)
        - remediability (str): How remediable (easy, moderate, difficult)
    """
    sector_key = _normalize_sector(sector)
    clamped_risk = _clamp(risk_score, 0.0, 100.0)

    # Sector risk multipliers — certain sectors face inherently higher controversy
    sector_risk_multipliers: dict[str, float] = {
        "energy":         1.35,
        "manufacturing":  1.15,
        "agriculture":    1.20,
        "mining":         1.40,
        "transportation": 1.10,
        "finance":        0.90,
        "technology":     0.85,
        "healthcare":     0.80,
        "real_estate":    0.85,
        "retail":         0.90,
    }
    multiplier = sector_risk_multipliers.get(sector_key, 1.0)
    adjusted_risk = _clamp(clamped_risk * multiplier, 0.0, 100.0)

    # Map adjusted risk to controversy level (1–5)
    if adjusted_risk < 15:
        level = 1
    elif adjusted_risk < 30:
        level = 2
    elif adjusted_risk < 50:
        level = 3
    elif adjusted_risk < 75:
        level = 4
    else:
        level = 5

    labels = {1: "Minor", 2: "Moderate", 3: "Significant", 4: "High", 5: "Severe"}
    scope_map: dict[int, str] = {
        1: "Local",
        2: "Local/Regional",
        3: "Regional",
        4: "Regional/Global",
        5: "Global",
    }
    remediability_map: dict[int, str] = {
        1: "Easy",
        2: "Easy",
        3: "Moderate",
        4: "Moderate",
        5: "Difficult",
    }

    # Pillar-specific breakdown — controversy often concentrates unevenly
    # Environmental pillar drives most controversy in high-emission sectors
    if sector_key in ("energy", "manufacturing", "agriculture", "transportation"):
        env_frac, soc_frac, gov_frac = 0.50, 0.30, 0.20
    elif sector_key == "finance":
        env_frac, soc_frac, gov_frac = 0.20, 0.35, 0.45
    elif sector_key == "technology":
        env_frac, soc_frac, gov_frac = 0.15, 0.40, 0.45
    else:
        env_frac, soc_frac, gov_frac = 0.33, 0.33, 0.34

    # Severity scaled 0–10 based on level
    severity = _clamp(level * 2.0 - 1.0, 0.0, 10.0)

    return {
        "level": level,
        "label": labels[level],
        "breakdown": {
            "environmental": round(adjusted_risk * env_frac / 10, 2),
            "social": round(adjusted_risk * soc_frac / 10, 2),
            "governance": round(adjusted_risk * gov_frac / 10, 2),
        },
        "severity": round(severity, 1),
        "scope": scope_map[level],
        "remediability": remediability_map[level],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — MSCI ESG Rating Simulation
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_msci_rating(
    sector: str,
    emissions_intensity: float,
    risk_score: float,
    controversy_level: int = 1,
) -> dict[str, Any]:
    """Simulate an MSCI ESG rating (AAA–CCC) based on available data inputs.

    The methodology approximates MSCI's approach:
    1. Weighted Key Issues Score — each issue gets scored based on inputs
    2. ESG Exposure — derived from sector weights and emissions intensity
    3. Management Score — how well the company manages its ESG risks
    4. Controversy Deduction — controversies knock down the final score

    Parameters
    ----------
    sector : str
        Industry sector (e.g., 'manufacturing', 'finance', 'energy').
    emissions_intensity : float
        Tons CO₂e per million EUR revenue (scope 1+2).
    risk_score : float
        Climate risk score from physical/transition risk models (0–100).
    controversy_level : int, optional
        Controversy severity level 1–5 (default: 1).

    Returns
    -------
    dict
        - rating_letter (str): AAA–CCC grade
        - score (float): Numerical score 0–10
        - key_issues (list): Key issues with individual scores
        - exposure (float): Industry ESG exposure score (0–10)
        - management_score (float): ESG management score (0–10)
        - peer_comparison (dict): Estimated peer benchmark summary
    """
    # ── Step 1: Normalise inputs ──────────────────────────────────────────
    sector_key = _normalize_sector(sector)
    pillar_weights = _get_pillar_weights(sector_key)
    key_issues = _get_key_issues(sector_key)

    # Clamp inputs to reasonable operating ranges
    ei_clamped = _clamp(emissions_intensity, 0.0, 5000.0)
    risk_clamped = _clamp(risk_score, 0.0, 100.0)
    controversy_clamped = _clamp(float(controversy_level), 1.0, 5.0)

    # ── Step 2: ESG Exposure Score (0–10) ─────────────────────────────────
    # Emissions intensity: lower is better for exposure
    # Normalise: 0 tCO₂e/M€ → 10 (best), 5000+ tCO₂e/M€ → 0 (worst)
    emissions_exposure = 10.0 - (ei_clamped / 5000.0) * 10.0

    # Risk score component: lower risk → lower exposure (better)
    # Invert so 0 risk → 10 exposure (good), 100 risk → 0 (bad)
    risk_exposure = 10.0 - (risk_clamped / 100.0) * 10.0

    # Blend according to pillar weights (E pillar gets emissions + risk)
    e_score = emissions_exposure * 0.6 + risk_exposure * 0.4
    # Social pillar driven primarily by risk (controversy proxy)
    s_score = risk_exposure * 0.7 + 10.0 * 0.3
    # Governance pillar — mostly independent of emissions
    g_score = risk_exposure * 0.5 + 10.0 * 0.5

    exposure = (
        e_score * pillar_weights["E"]
        + s_score * pillar_weights["S"]
        + g_score * pillar_weights["G"]
    )
    exposure = _clamp(exposure, 0.0, 10.0)

    # ── Step 3: Management Score (0–10) ───────────────────────────────────
    # Management quality inversely related to risk and controversy
    management_score = 10.0 - (risk_clamped / 100.0) * 4.0  # risk erodes up to 4 pts
    management_score -= (controversy_clamped - 1.0) * 1.0  # controversy erodes 1 pt/level
    management_score = _clamp(management_score, 0.0, 10.0)

    # ── Step 4: Key Issues Scoring ────────────────────────────────────────
    scored_issues = []
    for issue in key_issues:
        pillar = issue["pillar"]
        weight = issue["weight"]

        # Base score for each key issue (0–10)
        if pillar == "E":
            # Environmental issues driven by emissions and risk
            base = 10.0 - (ei_clamped / 5000.0) * 8.0 - (risk_clamped / 100.0) * 2.0
        elif pillar == "S":
            base = 10.0 - (risk_clamped / 100.0) * 5.0 - (controversy_clamped - 1.0) * 1.5
        else:
            # Governance — less affected by emissions, more by controversy
            base = 10.0 - (controversy_clamped - 1.0) * 2.0 - (risk_clamped / 100.0) * 2.0

        issue_score = _clamp(base, 0.0, 10.0)
        scored_issues.append({
            "issue": issue["issue"],
            "pillar": pillar,
            "weight": weight,
            "score": round(issue_score, 2),
            "contribution": round(issue_score * weight, 2),
        })

    # ── Step 5: Overall ESG Score ──────────────────────────────────────────
    # Weighted average of key issue scores
    weighted_score = sum(item["contribution"] for item in scored_issues)

    # Apply management overlay: good management amplifies, poor management dampens
    management_factor = management_score / 5.0  # 0 at score 0, 2 at score 10
    final_score = weighted_score * (0.5 + 0.5 * management_factor / 2.0)
    final_score = _clamp(final_score, 0.0, 10.0)

    # ── Step 6: Determine Letter Grade ────────────────────────────────────
    rating_letter = _score_to_msci_letter(final_score)

    # ── Step 7: Peer Comparison ───────────────────────────────────────────
    peer_comparison = get_peer_benchmark(sector, final_score)

    return {
        "rating_letter": rating_letter,
        "score": round(final_score, 2),
        "key_issues": scored_issues,
        "exposure": round(exposure, 2),
        "management_score": round(management_score, 2),
        "peer_comparison": peer_comparison,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — Sustainalytics ESG Risk Rating Simulation
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_sustainalytics_rating(
    sector: str,
    emissions_intensity: float,
    risk_score: float,
    controversy_level: int = 1,
) -> dict[str, Any]:
    """Simulate a Sustainalytics ESG Risk Rating (Negligible–Severe).

    Methodology approximation:
    - ESG Risk Score (0–100) = Exposure - Managed Risk + Controversy Deduction
    - Exposure: inherent industry- and company-level ESG risk
    - Managed Risk: how much risk the company manages through policies & practices
    - Controversy Deduction: penalty for ongoing controversies (0–15 pts)

    Parameters
    ----------
    sector : str
        Industry sector.
    emissions_intensity : float
        Tons CO₂e per million EUR revenue (scope 1+2).
    risk_score : float
        Climate risk score (0–100).
    controversy_level : int, optional
        Controversy level 1–5 (default: 1).

    Returns
    -------
    dict
        - risk_score (float): Overall ESG risk score (0–100)
        - risk_category (str): Negligible, Low, Medium, High, or Severe
        - exposure (float): Unmanaged risk exposure (0–100)
        - management_score (float): Management score (0–100)
        - controversy_deduction (float): Points deducted for controversy
        - breakdown (dict): E/S/G component risk scores
    """
    # ── Step 1: Normalise inputs ──────────────────────────────────────────
    sector_key = _normalize_sector(sector)
    pillar_weights = _get_pillar_weights(sector_key)

    ei_clamped = _clamp(emissions_intensity, 0.0, 5000.0)
    risk_clamped = _clamp(risk_score, 0.0, 100.0)
    controversy_clamped = _clamp(float(controversy_level), 1.0, 5.0)

    # ── Step 2: Industry/Company Exposure (0–100) ─────────────────────────
    # Higher emissions intensity = higher inherent exposure
    # Normalise emissions: 0 → 0 pts added, 5000 → 50 pts added
    emissions_exposure = (ei_clamped / 5000.0) * 50.0

    # Climate risk adds to exposure (higher risk = higher exposure)
    risk_exposure_component = risk_clamped * 0.40  # up to 40 pts

    # Sector base exposure — some sectors inherently riskier
    sector_base_exposure: dict[str, float] = {
        "energy":          40.0,
        "manufacturing":   35.0,
        "agriculture":     38.0,
        "transportation":  35.0,
        "finance":         28.0,
        "technology":      22.0,
        "healthcare":      20.0,
        "real_estate":     25.0,
        "retail":          20.0,
    }
    base_exposure = sector_base_exposure.get(sector_key, 28.0)

    # Total exposure (capped at 100)
    exposure = _clamp(base_exposure + emissions_exposure + risk_exposure_component, 0.0, 100.0)

    # ── Step 3: Management Score (0–100) ──────────────────────────────────
    # Management reduces risk; lower risk scores indicate better management
    # Base management = 100 - risk_score (simple inverse)
    management_score = 100.0 - risk_clamped * 0.60
    management_score -= (controversy_clamped - 1.0) * 5.0  # controversy erodes management
    management_score = _clamp(management_score, 0.0, 100.0)

    # ── Step 4: Controversy Deduction (0–15 pts) ─────────────────────────
    # Sustainalytics adds a deduction for severe controversies
    controversy_deduction = (controversy_clamped - 1.0) * 3.75  # level 5 → 15 pts
    controversy_deduction = _clamp(controversy_deduction, 0.0, 15.0)

    # ── Step 5: Unmanaged Risk Score (0–100) ──────────────────────────────
    # Formula: Unmanaged Risk = Exposure - Managed Risk + Controversy Deduction
    # Managed risk = exposure * (management_score / 100)
    managed_risk = exposure * (management_score / 100.0)
    unmanaged_risk = exposure - managed_risk + controversy_deduction
    unmanaged_risk = _clamp(unmanaged_risk, 0.0, 100.0)

    # ── Step 6: Risk Category ─────────────────────────────────────────────
    risk_category_info = _score_to_sustainalytics_category(unmanaged_risk)

    # ── Step 7: Pillar Breakdown ──────────────────────────────────────────
    # Distribute the unmanaged risk into E/S/G components
    env_exposure = base_exposure * 0.4 + emissions_exposure * 0.6
    soc_exposure = base_exposure * 0.3 + risk_exposure_component * 0.4
    gov_exposure = base_exposure * 0.3 + risk_exposure_component * 0.2

    # Apply pillar-level management and controversy deductions
    env_unmanaged = _clamp(env_exposure - env_exposure * (management_score / 100.0) + controversy_deduction * 0.4, 0.0, 100.0)
    soc_unmanaged = _clamp(soc_exposure - soc_exposure * (management_score / 100.0) + controversy_deduction * 0.3, 0.0, 100.0)
    gov_unmanaged = _clamp(gov_exposure - gov_exposure * (management_score / 100.0) + controversy_deduction * 0.3, 0.0, 100.0)

    return {
        "risk_score": round(unmanaged_risk, 2),
        "risk_category": risk_category_info["name"],
        "exposure": round(exposure, 2),
        "management_score": round(management_score, 2),
        "controversy_deduction": round(controversy_deduction, 2),
        "breakdown": {
            "environmental": round(env_unmanaged, 2),
            "social": round(soc_unmanaged, 2),
            "governance": round(gov_unmanaged, 2),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — Peer Benchmarking
# ═══════════════════════════════════════════════════════════════════════════════

def get_peer_benchmark(sector: str, own_score: float) -> dict[str, Any]:
    """Provide an estimated peer distribution and percentile for a given score.

    Uses sector-level distributions calibrated from typical ESG rating data.
    For MSCI scoring (0–10), generates a plausible distribution and places
    the own_score within it.

    Parameters
    ----------
    sector : str
        Industry sector.
    own_score : float
        The entity's ESG score (0–10 scale) to benchmark.

    Returns
    -------
    dict
        - percentile (float): Estimated percentile rank (0–100)
        - top_quartile (float): Score at the 75th percentile
        - median (float): Score at the 50th percentile
        - bottom_quartile (float): Score at the 25th percentile
        - distribution (dict): Estimated % of peers in each MSCI tier
        - recommendations (list): Suggested improvement actions by pillar
    """
    sector_key = _normalize_sector(sector)
    score_clamped = _clamp(own_score, 0.0, 10.0)

    # Sector-specific distribution parameters (mean, stddev on 0–10 scale)
    # Derived from typical ESG-rating dispersion by industry
    sector_distributions: dict[str, dict[str, float]] = {
        "manufacturing":  {"mean": 5.0, "std": 1.8},
        "finance":        {"mean": 6.0, "std": 1.5},
        "energy":         {"mean": 4.0, "std": 2.0},
        "technology":     {"mean": 6.5, "std": 1.6},
        "healthcare":     {"mean": 6.0, "std": 1.7},
        "real_estate":    {"mean": 5.5, "std": 1.8},
        "transportation": {"mean": 4.5, "std": 1.9},
        "agriculture":    {"mean": 4.5, "std": 1.8},
        "retail":         {"mean": 5.5, "std": 1.6},
        "default":        {"mean": 5.3, "std": 1.7},
    }
    dist = sector_distributions.get(sector_key, sector_distributions["default"])
    mu, sigma = dist["mean"], dist["std"]

    # Estimate percentile using a normal CDF approximation (error function)
    # z-score tells us how many stddevs the score is from the mean
    z_score = (score_clamped - mu) / sigma if sigma > 0 else 0.0
    # Approximate CDF using the error function
    erf_approx = math.erf(z_score / math.sqrt(2.0))
    percentile = _clamp((erf_approx + 1.0) / 2.0 * 100.0, 0.0, 100.0)

    # Compute quartile benchmarks from the distribution
    # Use statistics.NormalDist for accurate quantile computation (Python 3.8+)
    try:
        norm_dist = statistics.NormalDist(mu=mu, sigma=sigma)
        bottom_quartile = _clamp(norm_dist.inv_cdf(0.25), 0.0, 10.0)
        median = _clamp(norm_dist.inv_cdf(0.50), 0.0, 10.0)
        top_quartile = _clamp(norm_dist.inv_cdf(0.75), 0.0, 10.0)
    except AttributeError:
        # Fallback for older Python: use rational approximation of inverse CDF
        # (Abramowitz and Stegun approximation for normal quantile function)
        def _norm_quantile(p: float) -> float:
            """Approximate normal quantile (inverse CDF) using rational function."""
            if p <= 0.0 or p >= 1.0:
                return mu if p == 0.5 else (mu + 5.0 * sigma if p > 0.5 else mu - 5.0 * sigma)
            t = math.sqrt(-2.0 * math.log(1.0 - p) if p > 0.5 else -2.0 * math.log(p))
            # Rational approximation coefficients
            c = [2.515517, 0.802853, 0.010328]
            d = [1.432788, 0.189269, 0.001308]
            z = t - (c[0] + c[1] * t + c[2] * t * t) / (1.0 + d[0] * t + d[1] * t * t + d[2] * t * t * t)
            if p > 0.5:
                z = -z
            return mu + sigma * z

        bottom_quartile = _clamp(_norm_quantile(0.25), 0.0, 10.0)
        median = _clamp(_norm_quantile(0.50), 0.0, 10.0)
        top_quartile = _clamp(_norm_quantile(0.75), 0.0, 10.0)

    # Distribution of peers across MSCI tiers (estimated %)
    # Based on typical sector-level dispersion
    # Z-score thresholds for each tier boundary (in order AAA through CCC)
    if sigma > 0.001:
        tier_z = {
            "AAA": 2.5,    # Top 0.6% — best-in-class
            "AA":  1.5,    # Top 6.7%
            "A":   0.5,    # Top 31%
            "BBB": -0.5,   # Bottom 31%
            "BB":  -1.5,   # Bottom 6.7%
            "B":   -2.5,   # Bottom 0.6%
            "CCC": -5.0,   # Below — worst
        }
    else:
        # Degenerate case: zero variance → all peers at mean
        tier_z = {t: 0.0 for t in ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]}

    # Compute proportion in each tier using CDF differences
    tiers_ordered = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]
    tier_props = {}
    for i, tier in enumerate(tiers_ordered):
        upper_z = tier_z[tiers_ordered[i - 1]] if i > 0 else 5.0  # cap at ~5 sigma
        lower_z = tier_z[tier]
        upper_cdf = 0.5 * (1.0 + math.erf(upper_z / math.sqrt(2.0))) if upper_z < 5.0 else 1.0
        lower_cdf = 0.5 * (1.0 + math.erf(lower_z / math.sqrt(2.0)))
        proportion = max(0.0, upper_cdf - lower_cdf)
        tier_props[tier] = round(proportion * 100.0, 1)

    # Normalise to exactly 100%
    total = sum(tier_props.values())
    if total > 0:
        tier_props = {k: round(v / total * 100.0, 1) for k, v in tier_props.items()}

    # Generate improvement recommendations based on score gaps
    recommendations = _generate_peer_recommendations(sector_key, score_clamped, top_quartile)

    return {
        "percentile": round(percentile, 1),
        "top_quartile": round(top_quartile, 2),
        "median": round(median, 2),
        "bottom_quartile": round(bottom_quartile, 2),
        "distribution": tier_props,
        "recommendations": recommendations,
    }


def _generate_peer_recommendations(
    sector_key: str,
    score: float,
    top_quartile: float,
) -> list[dict[str, Any]]:
    """Generate targeted improvement recommendations by ESG pillar.

    Produces actionable suggestions when the score is below the top quartile,
    indicating room for relative improvement against peers.
    """
    recommendations = []
    gap = top_quartile - score

    if gap <= 0:
        # Already at or above 75th percentile
        recommendations.append({
            "pillar": "E",
            "action": "Maintain current environmental performance; focus on innovation leadership",
            "priority": "low",
        })
        recommendations.append({
            "pillar": "S",
            "action": "Sustain social practices; consider best-practice reporting",
            "priority": "low",
        })
        recommendations.append({
            "pillar": "G",
            "action": "Continue strong governance; explore emerging disclosure frameworks",
            "priority": "low",
        })
        return recommendations

    if gap > 0:
        recommendations.append({
            "pillar": "E",
            "action": "Reduce emissions intensity by 15-25% through energy efficiency and renewable energy sourcing",
            "priority": "high" if gap > 3.0 else "medium",
        })
        recommendations.append({
            "pillar": "S",
            "action": "Strengthen labor practices and supply chain due diligence; improve safety metrics",
            "priority": "high" if gap > 2.5 else "medium",
        })
        recommendations.append({
            "pillar": "G",
            "action": "Enhance board oversight of ESG issues and align executive compensation with sustainability targets",
            "priority": "medium",
        })

    # Sector-specific recommendations
    if sector_key in ("manufacturing", "energy", "transportation"):
        recommendations.append({
            "pillar": "E",
            "action": "Implement science-based targets (SBTi) and invest in low-carbon process innovation",
            "priority": "high",
        })
    elif sector_key == "finance":
        recommendations.append({
            "pillar": "E",
            "action": "Set financed emissions reduction targets (PCAF-aligned) and expand green finance products",
            "priority": "high",
        })
    elif sector_key == "technology":
        recommendations.append({
            "pillar": "S",
            "action": "Strengthen data privacy frameworks and AI ethics governance",
            "priority": "high",
        })

    return recommendations


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — ESG Improvement Plan
# ═══════════════════════════════════════════════════════════════════════════════

def get_esg_improvement_plan(
    sector: str,
    current_score: float,
    target_score: float,
) -> dict[str, Any]:
    """Generate an actionable ESG improvement plan to bridge a score gap.

    Provides concrete actions per pillar (Environmental, Social, Governance)
    with estimated timelines, costs, and expected benefits. The plan is
    calibrated to the size of the gap and the sector context.

    Parameters
    ----------
    sector : str
        Industry sector.
    current_score : float
        Current ESG score (0–10 scale).
    target_score : float
        Target ESG score (0–10 scale). Must be > current_score.

    Returns
    -------
    dict
        - current_score (float): Starting ESG score
        - target_score (float): Target ESG score
        - gap (float): Score gap to close
        - feasibility (str): Feasibility assessment
        - estimated_timeline (str): Overall timeline estimate
        - actions (list): Per-pillar action items with details
        - cost_estimate (dict): Estimated total cost range
    """
    current_clamped = _clamp(current_score, 0.0, 10.0)
    target_clamped = _clamp(target_score, current_clamped, 10.0)
    gap = target_clamped - current_clamped
    sector_key = _normalize_sector(sector)
    pillar_weights = _get_pillar_weights(sector_key)

    # ── Feasibility Assessment ──────────────────────────────────────────────
    if gap <= 0:
        return {
            "current_score": current_clamped,
            "target_score": target_clamped,
            "gap": 0.0,
            "feasibility": "No gap to close — current score meets or exceeds target.",
            "estimated_timeline": "N/A",
            "actions": [],
            "cost_estimate": {"min_eur": 0, "max_eur": 0},
        }

    if gap <= 1.5:
        feasibility = "Achievable — incremental improvements across pillars"
        timeline = "Short-term (6–18 months)"
    elif gap <= 3.0:
        feasibility = "Moderately challenging — requires targeted investment"
        timeline = "Medium-term (1–3 years)"
    elif gap <= 5.0:
        feasibility = "Challenging — requires significant transformation"
        timeline = "Medium-to-long-term (2–5 years)"
    else:
        feasibility = "Very challenging — requires fundamental restructuring"
        timeline = "Long-term (3–7 years)"

    # ── Build Actions Per Pillar ────────────────────────────────────────────
    actions: list[dict[str, Any]] = []

    # Environmental pillar actions
    env_share = pillar_weights["E"]
    env_gap_target = gap * env_share
    if env_gap_target > 0.1:
        actions.extend(_env_actions(sector_key, env_gap_target, gap))

    # Social pillar actions
    soc_share = pillar_weights["S"]
    soc_gap_target = gap * soc_share
    if soc_gap_target > 0.1:
        actions.extend(_social_actions(sector_key, soc_gap_target, gap))

    # Governance pillar actions
    gov_share = pillar_weights["G"]
    gov_gap_target = gap * gov_share
    if gov_gap_target > 0.1:
        actions.extend(_governance_actions(sector_key, gov_gap_target, gap))

    # ── Cost Estimate ────────────────────────────────────────────────────────
    cost_estimate = _estimate_plan_cost(gap, sector_key, actions)

    return {
        "current_score": current_clamped,
        "target_score": target_clamped,
        "gap": round(gap, 2),
        "feasibility": feasibility,
        "estimated_timeline": timeline,
        "actions": actions,
        "cost_estimate": cost_estimate,
    }


def _env_actions(sector_key: str, target_potential: float, total_gap: float) -> list[dict[str, Any]]:
    """Generate environmental improvement actions."""
    actions = []

    # Core carbon reduction — applicable to all sectors
    if target_potential > 0.3:
        timeline = "medium" if target_potential < 1.0 else "long"
        cost = "€500K–€2M" if timeline == "medium" else "€2M–€10M"
        actions.append({
            "pillar": "E",
            "action": "Implement comprehensive decarbonisation programme with SBTi-aligned targets",
            "timeline": timeline,
            "estimated_cost": cost,
            "expected_benefit": f"Reduce emissions intensity by 30-50%, contributing ~{round(target_potential, 1)} pts to ESG score",
            "priority": "high",
        })

    # Energy efficiency — quick wins
    actions.append({
        "pillar": "E",
        "action": "Conduct energy audits and implement efficiency measures (lighting, HVAC, process optimisation)",
        "timeline": "short",
        "estimated_cost": "€100K–€500K",
        "expected_benefit": "5-15% reduction in energy costs and emissions",
        "priority": "high",
    })

    # Renewable energy adoption
    actions.append({
        "pillar": "E",
        "action": "Increase renewable energy procurement (PPAs, on-site solar) to 60%+ of total consumption",
        "timeline": "medium",
        "estimated_cost": "€500K–€5M depending on scale",
        "expected_benefit": "20-40% scope 2 emissions reduction, improved ESG exposure score",
        "priority": "medium",
    })

    # Water/waste management — sector-specific
    if sector_key in ("manufacturing", "agriculture", "energy"):
        actions.append({
            "pillar": "E",
            "action": "Implement water recycling systems and zero-waste-to-landfill programme",
            "timeline": "medium",
            "estimated_cost": "€200K–€2M",
            "expected_benefit": "30-50% water use reduction, improved waste diversion rate",
            "priority": "medium",
        })

    return actions


def _social_actions(sector_key: str, target_potential: float, total_gap: float) -> list[dict[str, Any]]:
    """Generate social pillar improvement actions."""
    actions = []

    # Workforce and labour
    actions.append({
        "pillar": "S",
        "action": "Enhance workforce DEI (Diversity, Equity, Inclusion) programme with measurable targets",
        "timeline": "short",
        "estimated_cost": "€50K–€300K",
        "expected_benefit": "Improved talent attraction/retention, +0.2–0.5 ESG score contribution",
        "priority": "high",
    })

    # Health & safety
    if sector_key in ("manufacturing", "energy", "transportation"):
        actions.append({
            "pillar": "S",
            "action": "Deploy advanced safety management system with real-time monitoring and AI-based risk prediction",
            "timeline": "medium",
            "estimated_cost": "€300K–€2M",
            "expected_benefit": "40-60% reduction in lost-time injury rate, +0.3–0.8 score contribution",
            "priority": "high",
        })
        actions.append({
            "pillar": "S",
            "action": "Implement comprehensive supply chain human rights due diligence programme",
            "timeline": "medium",
            "estimated_cost": "€200K–€1M",
            "expected_benefit": "ESG controversy risk reduction, alignment with CSDDD requirements",
            "priority": "medium",
        })

    # Privacy & data security — critical for tech/finance
    if sector_key in ("technology", "finance"):
        actions.append({
            "pillar": "S",
            "action": "Strengthen data privacy framework with ISO 27701 certification and privacy-by-design approach",
            "timeline": "medium",
            "estimated_cost": "€500K–€3M",
            "expected_benefit": "Reduced data breach risk, improved stakeholder trust, +0.5–1.0 score contribution",
            "priority": "high",
        })

    # Community engagement
    actions.append({
        "pillar": "S",
        "action": "Establish structured community engagement and social impact measurement programme",
        "timeline": "short",
        "estimated_cost": "€50K–€200K",
        "expected_benefit": "Improved social license to operate, supply chain resilience",
        "priority": "low",
    })

    return actions


def _governance_actions(sector_key: str, target_potential: float, total_gap: float) -> list[dict[str, Any]]:
    """Generate governance pillar improvement actions."""
    actions = []

    # Board oversight
    actions.append({
        "pillar": "G",
        "action": "Establish board-level ESG/sustainability committee with clear charter and KPIs",
        "timeline": "short",
        "estimated_cost": "€20K–€100K",
        "expected_benefit": "Direct governance score improvement, stronger ESG accountability",
        "priority": "high",
    })

    # Executive compensation alignment
    actions.append({
        "pillar": "G",
        "action": "Link executive compensation to ESG performance targets (15-25% weight)",
        "timeline": "short",
        "estimated_cost": "€10K–€50K (consulting/advisory fees)",
        "expected_benefit": "Demonstrated commitment to ESG integration, +0.2–0.4 score contribution",
        "priority": "high",
    })

    # Reporting and disclosure
    if target_potential > 0.5:
        actions.append({
            "pillar": "G",
            "action": "Adopt integrated reporting aligned with TCFD, TNFD, ISSB, and ESRS (CSRD) standards",
            "timeline": "medium",
            "estimated_cost": "€100K–€500K (reporting systems, assurance, training)",
            "expected_benefit": "Full regulatory compliance, improved transparency score, +0.3–0.7 contribution",
            "priority": "high",
        })

    # Business ethics
    actions.append({
        "pillar": "G",
        "action": "Implement enhanced whistleblower programme, anti-corruption training, and third-party due diligence",
        "timeline": "short",
        "estimated_cost": "€30K–€150K",
        "expected_benefit": "Reduced controversy risk, stronger ethics culture",
        "priority": "medium",
    })

    # Risk management
    if sector_key in ("finance", "real_estate"):
        actions.append({
            "pillar": "G",
            "action": "Integrate climate scenario analysis into enterprise risk management framework",
            "timeline": "medium",
            "estimated_cost": "€200K–€1M",
            "expected_benefit": "Enhanced risk identification, regulatory preparedness, +0.3–0.6 contribution",
            "priority": "medium",
        })

    return actions


def _estimate_plan_cost(
    gap: float,
    sector_key: str,
    actions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Estimate total plan cost based on gap magnitude and actions."""
    # Rough base cost estimate
    if gap <= 1.0:
        base_min, base_max = 200_000, 1_000_000
    elif gap <= 2.5:
        base_min, base_max = 1_000_000, 5_000_000
    elif gap <= 4.0:
        base_min, base_max = 5_000_000, 20_000_000
    else:
        base_min, base_max = 20_000_000, 100_000_000

    # Adjust for capital-intensive sectors
    sector_multipliers = {
        "energy": 2.0,
        "manufacturing": 1.5,
        "transportation": 1.5,
        "agriculture": 1.3,
    }
    mult = sector_multipliers.get(sector_key, 1.0)

    total_min = int(base_min * mult)
    total_max = int(base_max * mult)

    return {
        "min_eur": total_min,
        "max_eur": total_max,
        "currency": "EUR",
        "note": "Estimated total investment required across all pillars (capex + opex over timeline)",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Module-Level Convenience
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "simulate_msci_rating",
    "simulate_sustainalytics_rating",
    "get_esg_controversy_score",
    "get_peer_benchmark",
    "get_esg_improvement_plan",
]
