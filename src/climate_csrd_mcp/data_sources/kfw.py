"""
KfW (Kreditanstalt für Wiederaufbau) / BAFA funding programs.

Provides information on German federal funding programs for
climate adaptation, energy efficiency, and sustainability measures.

Sources:
- KfW: https://www.kfw.de/inlandsfoerderung/
- BAFA: https://www.bafa.de/
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─── KfW Funding Programs ────────────────────────────────────────────
# Source: kfw.de / bafa.de (current as of 2025)

KFW_PROGRAMS: list[dict] = [
    # ── Energy Efficiency & Climate ──
    {
        "id": "KfW-270",
        "name": "KfW 270 — Erneuerbare Energien Standard",
        "type": "loan",
        "max_amount_eur": 100_000_000,
        "interest_rate": "0.01% (effektiv)",
        "duration_years": 20,
        "tilt_period": "bis zu 3 Jahre tilgungsfrei",
        "eligible_for": ["energy", "manufacturing", "agriculture"],
        "measures": ["renewable_energy", "solar_pv", "wind", "biomass", "geothermal"],
        "detail": "Finanzierung von Anlagen zur Nutzung erneuerbarer Energien",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "KfW-271",
        "name": "KfW 271 — Erneuerbare Energien Premium",
        "type": "loan",
        "max_amount_eur": 25_000_000,
        "interest_rate": "0.01% (effektiv)",
        "duration_years": 20,
        "tilt_period": "bis zu 3 Jahre tilgungsfrei",
        "eligible_for": ["energy", "manufacturing", "agriculture", "transport"],
        "measures": ["renewable_energy", "battery_storage", "grid_infrastructure", "heat_pump"],
        "detail": "Ergänzende Förderung für Speicher, Netze und Wärmepumpen",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "KfW-290",
        "name": "KfW 290 — Klimaschutzoffensive für Unternehmen",
        "type": "loan",
        "max_amount_eur": 50_000_000,
        "interest_rate": "ab 0.01% (effektiv)",
        "duration_years": 20,
        "tilt_period": "bis zu 5 Jahre tilgungsfrei",
        "eligible_for": ["manufacturing", "construction", "transport", "retail", "technology"],
        "measures": ["energy_efficiency", "process_optimization", "heat_recovery", "building_retrofit"],
        "detail": "CO2-Minderungsinvestitionen: energieeffiziente Produktion, Abwärmenutzung, Kältetechnik",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "KfW-293",
        "name": "KfW 293 — Energieeffizienz und Prozesswärme aus EE",
        "type": "loan",
        "max_amount_eur": 25_000_000,
        "interest_rate": "ab 0.01% (effektiv)",
        "duration_years": 20,
        "tilt_period": "bis zu 5 Jahre tilgungsfrei",
        "eligible_for": ["manufacturing", "construction", "agriculture"],
        "measures": ["process_heat_renewable", "solar_thermal", "heat_pump_industrial"],
        "detail": "Umstellung von Prozesswärme auf erneuerbare Energien",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    # ── Building / Real Estate ──
    {
        "id": "KfW-261",
        "name": "KfW 261 — Wohngebäude – Kredit für Klimafreundlichen Neubau",
        "type": "loan",
        "max_amount_eur": 150_000,
        "interest_rate": "ab 0.75% (effektiv)",
        "duration_years": 30,
        "tilt_period": "bis zu 5 Jahre tilgungsfrei",
        "eligible_for": ["real_estate"],
        "measures": ["new_building_efficient", "kfw_40", "kfw_40_plus"],
        "detail": "Neubau oder Ersterwerb von klimafreundlichen Wohngebäuden (Effizienzhaus 40)",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "KfW-358",
        "name": "KfW 358 — Klimafreundlichen Nichtwohngebäude",
        "type": "loan",
        "max_amount_eur": 10_000_000,
        "interest_rate": "ab 0.75% (effektiv)",
        "duration_years": 20,
        "tilt_period": "bis zu 3 Jahre tilgungsfrei",
        "eligible_for": ["real_estate", "manufacturing", "retail"],
        "measures": ["new_building_efficient", "non_residential", "kfw_40_nwg"],
        "detail": "Klimafreundlicher Neubau und Ersterwerb von Nichtwohngebäuden",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    # ── Climate Adaptation ──
    {
        "id": "KfW-441",
        "name": "KfW 441 — Anpassung an den Klimawandel (Kommune)",
        "type": "loan",
        "max_amount_eur": 10_000_000,
        "interest_rate": "ab 0.01%",
        "duration_years": 20,
        "tilt_period": "bis zu 5 Jahre tilgungsfrei",
        "eligible_for": ["public", "real_estate"],
        "measures": ["flood_protection", "heat_adaptation", "green_infrastructure", "water_management"],
        "detail": "Investitionen in klimaresiliente Infrastruktur: Hochwasserschutz, Grünflächen, Entsiegelung, Regenwassermanagement",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "BAFA-Biomasse",
        "name": "BAFA — Bundesförderung Biomasse (Biomasseanlagen)",
        "type": "grant",
        "max_amount_eur": 500_000,
        "interest_rate": "Zuschuss",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["energy", "manufacturing", "agriculture"],
        "measures": ["biomass", "biogas", "wood_chips", "pellet"],
        "detail": "Investitionszuschuss für Biomasseanlagen bis 5 MW",
        "source": "BAFA, Stand 2025",
    },
    {
        "id": "BAFA-EW",
        "name": "BAFA — Bundesförderung Energieeffizienz (EEW)",
        "type": "grant",
        "max_amount_eur": 15_000_000,
        "interest_rate": "Zuschuss (35-55%)",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["manufacturing", "construction", "transport", "retail"],
        "measures": [
            "energy_audit", "process_optimization", "compressed_air",
            "pump_systems", "ventilation", "lighting", "refrigeration",
        ],
        "detail": "Investitionszuschuss für Querschnittstechnologien und Prozesswärme (Modul 1-4)",
        "source": "BAFA, Stand 2025",
    },
    {
        "id": "BAFA-EBW",
        "name": "BAFA — Bundesförderung Energieberatung (EBW)",
        "type": "grant",
        "max_amount_eur": 80_000,
        "interest_rate": "Zuschuss (80%)",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["manufacturing", "construction", "transport", "retail", "real_estate"],
        "measures": ["energy_audit", "consulting", "decarbonization_plan"],
        "detail": "Zuschuss für Energieberatung und Dekarbonisierungspläne im Mittelstand",
        "source": "BAFA, Stand 2025",
    },
]

# ─── Standort-specific recommendations ───────────────────────────────

STANDORT_MAP: dict[str, list[str]] = {
    "produktion": ["manufacturing", "construction"],
    "buero": ["technology", "finance", "real_estate"],
    "logistik": ["transport", "retail"],
    "landwirtschaft": ["agriculture"],
    "energie": ["energy"],
    "handel": ["retail"],
    "wohnen": ["real_estate"],
    "kommune": ["public"],
}


async def get_funding_programs(
    standort_art: str = "produktion",
    sector: str = "manufacturing",
    measure: str = "energy_efficiency",
) -> list[dict]:
    """
    Get matching KfW / BAFA funding programs.

    Args:
        standort_art: Type of location (produktion, buero, logistik, landwirtschaft, etc.)
        sector: Business sector
        measure: Type of measure (energy_efficiency, renewable_energy, flood_protection, etc.)

    Returns:
        List of matching funding programs with details
    """
    eligible_sectors = STANDORT_MAP.get(standort_art, ["manufacturing"])
    if sector not in eligible_sectors:
        eligible_sectors.append(sector)

    matches = []
    for prog in KFW_PROGRAMS:
        sector_match = any(s in prog["eligible_for"] for s in eligible_sectors)
        measure_match = measure in prog["measures"]
        if sector_match and measure_match:
            matches.append(prog)

    if not matches:
        # Return all eligible programs for the sector as fallback
        for prog in KFW_PROGRAMS:
            if any(s in prog["eligible_for"] for s in eligible_sectors):
                matches.append(prog)

    return matches
