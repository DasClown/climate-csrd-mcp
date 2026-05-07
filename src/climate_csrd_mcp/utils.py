"""
Shared utilities — risk scoring, ESRS mapping, formatting helpers.
"""

from datetime import date
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


def risk_label(score: int) -> str:
    return RISK_LABELS.get(score, "Unknown")


def risk_color(score: int) -> str:
    return RISK_COLORS.get(score, "⚪")


def aggregate_risk(scores: list[int]) -> int:
    """Aggregate multiple risk scores into one (max-based with boost for 4+)."""
    if not scores:
        return 1
    max_score = max(scores)
    # If 3+ dimensions are at 3+, boost by 1
    high_count = sum(1 for s in scores if s >= 3)
    if high_count >= 3 and max_score < 5:
        return min(max_score + 1, 5)
    return max_score


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
        "description": "Working conditions, health & safety, diversity",
        "sub_topics": [],
    },
    "S2": {
        "title": "ESRS S2 — Workers in the Value Chain",
        "description": "Value chain labor practices",
        "sub_topics": [],
    },
    "G1": {
        "title": "ESRS G1 — Business Conduct",
        "description": "Corporate governance, ethics, anti-corruption",
        "sub_topics": [],
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


# ─── Sector Mapping ───────────────────────────────────────────────────

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
