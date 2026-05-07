"""
KfW (Kreditanstalt für Wiederaufbau) / BAFA funding programs — EU funds.

Provides:
- KfW funding programs (energy, building, climate adaptation)
- BAFA energy consulting (EBW) detailed programs
- EU Innovation Fund programs
- EU Modernisation Fund programs
- Just Transition Fund programs
- State-level (Bundesland) specific programs
- EU Taxonomy-alignment check for funding eligibility

Sources:
- KfW: https://www.kfw.de/inlandsfoerderung/
- BAFA: https://www.bafa.de/
- EU Innovation Fund: https://climate.ec.europa.eu/eu-action/eu-funding-climate-action/innovation-fund
- EU Modernisation Fund: https://modernisationfund.eu/
- Just Transition Fund: https://ec.europa.eu/regional_policy/funding/just-transition-fund
- EU Taxonomy Regulation (EU) 2020/852
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

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
        "fund_source": "KfW / Federal Ministry for Economic Affairs",
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
        "fund_source": "KfW / Federal Ministry for Economic Affairs",
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
        "fund_source": "KfW / Federal Ministry for Economic Affairs",
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
        "fund_source": "KfW / Federal Ministry for Economic Affairs",
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
        "fund_source": "KfW / Federal Ministry for Housing",
        "source": "KfW Bankengruppe, Stand 2025",
    },
    {
        "id": "KfW-264",
        "name": "KfW 264 — Wohngebäude – Kredit für Klimafreundlichen Neubau (FH)",
        "type": "loan",
        "max_amount_eur": 100_000,
        "interest_rate": "ab 0.75% (effektiv)",
        "duration_years": 30,
        "tilt_period": "bis zu 5 Jahre tilgungsfrei",
        "eligible_for": ["real_estate"],
        "measures": ["new_building_efficient", "kfw_40", "kfw_fh"],
        "detail": "Neubau von klimafreundlichen Wohngebäuden mit fossilfreiem Heizsystem",
        "fund_source": "KfW / Federal Ministry for Housing",
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
        "fund_source": "KfW / Federal Ministry for Housing",
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
        "fund_source": "KfW / Federal Ministry for Environment",
        "source": "KfW Bankengruppe, Stand 2025",
    },
]

# ─── BAFA Programs (Detailed) ────────────────────────────────────────
# Source: BAFA (Bundesamt für Wirtschaft und Ausfuhrkontrolle)

BAFA_PROGRAMS: list[dict] = [
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
        "modules": [
            {"code": "Modul 1", "name": "Querschnittstechnologien", "max_grant_pct": 35},
            {"code": "Modul 2", "name": "Prozesswärme aus Erneuerbaren", "max_grant_pct": 45},
            {"code": "Modul 3", "name": "Mess- und Regelungstechnik", "max_grant_pct": 35},
            {"code": "Modul 4", "name": "Energieeffizienz- und Klimaschutznetzwerke", "max_grant_pct": 55},
        ],
        "detail": "Investitionszuschuss für Querschnittstechnologien und Prozesswärme (Modul 1-4)",
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
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
        "modules": [
            {"code": "EBW-B1", "name": "Energieberatung KMU (Basis)", "max_grant_eur": 2_500, "coverage": "80% der Kosten"},
            {"code": "EBW-B2", "name": "Energieberatung KMU (Erweitert)", "max_grant_eur": 5_000, "coverage": "80% der Kosten"},
            {"code": "EBW-D", "name": "Energieberatung mit Dekarbonisierungsplan", "max_grant_eur": 10_000, "coverage": "80% der Kosten"},
            {"code": "EBW-C", "name": "Contracting-Beratung", "max_grant_eur": 5_000, "coverage": "50% der Kosten"},
        ],
        "detail": "Zuschuss für Energieberatung und Dekarbonisierungspläne im Mittelstand",
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
        "source": "BAFA, Stand 2025",
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
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
        "source": "BAFA, Stand 2025",
    },
    {
        "id": "BAFA-KWK",
        "name": "BAFA — Kraft-Wärme-Kopplung (KWK)",
        "type": "grant",
        "max_amount_eur": 10_000_000,
        "interest_rate": "Zuschuss",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["manufacturing", "energy", "real_estate"],
        "measures": ["chp", "combined_heat_power", "district_heating"],
        "detail": "Zuschuss für hocheffiziente KWK-Anlagen und Wärme-/Kältenetze",
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
        "source": "BAFA, Stand 2025",
    },
    {
        "id": "BAFA-Kälte",
        "name": "BAFA — Kälte-Klima-Bereich (Kälte/Klima)",
        "type": "grant",
        "max_amount_eur": 200_000,
        "interest_rate": "Zuschuss (35%)",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["manufacturing", "retail", "real_estate"],
        "measures": ["cooling", "refrigeration", "heat_pump", "air_conditioning"],
        "detail": "Förderung von hocheffizienten Kälte- und Klimaanlagen mit natürlichen Kältemitteln",
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
        "source": "BAFA, Stand 2025",
    },
    {
        "id": "BAFA-Wärmepumpe",
        "name": "BAFA — Wärmepumpen (Einzelmaßnahme)",
        "type": "grant",
        "max_amount_eur": 60_000,
        "interest_rate": "Zuschuss (25-50%)",
        "duration_years": None,
        "tilt_period": None,
        "eligible_for": ["real_estate", "manufacturing", "retail"],
        "measures": ["heat_pump", "heat_pump_industrial", "building_retrofit"],
        "detail": "Austausch von Heizungsanlagen gegen Wärmepumpen (Einzelmaßnahme im Gebäudebereich)",
        "fund_source": "BAFA / Federal Ministry for Economic Affairs",
        "source": "BAFA, Stand 2025",
    },
]

# ─── EU Innovation Fund Programs ─────────────────────────────────────
# Source: European Commission, Innovation Fund (2020-2030)

EU_INNOVATION_FUND_PROGRAMS: list[dict] = [
    {
        "id": "EU-IF-Large",
        "name": "EU Innovation Fund — Large Scale Projects",
        "type": "grant",
        "budget_eur": "~4 billion (2020-2030, per call varies)",
        "max_grant_pct": "Up to 60% of eligible costs",
        "eligible_sectors": ["energy", "manufacturing", "chemicals", "steel", "cement", "refineries", "transport"],
        "measures": ["ccs", "ccu", "renewable_hydrogen", "energy_storage", "low_carbon_processes", "breakthrough_tech"],
        "project_scale": "CAPEX > €7.5M (large-scale call)",
        "detail": "Flagship EU fund for demonstration of innovative low-carbon technologies. Covers CCUS, renewable hydrogen, energy storage, and breakthrough industrial processes.",
        "fund_source": "EU ETS Revenues (Innovation Fund, 450M allowances)",
        "source": "European Commission Innovation Fund, CINEA, Stand 2025",
    },
    {
        "id": "EU-IF-Small",
        "name": "EU Innovation Fund — Small Scale Projects",
        "type": "grant",
        "budget_eur": "~100M per call",
        "max_grant_pct": "Up to 60% of eligible costs",
        "eligible_sectors": ["energy", "manufacturing", "chemicals", "cement", "transport", "construction"],
        "measures": ["ccs", "ccu", "renewable_hydrogen", "energy_storage", "low_carbon_processes", "circular_economy"],
        "project_scale": "CAPEX < €7.5M",
        "detail": "Smaller-scale innovation projects. Rolling calls with lower administrative burden. Covers clean tech, circular economy, and carbon removal.",
        "fund_source": "EU ETS Revenues (Innovation Fund, 450M allowances)",
        "source": "European Commission Innovation Fund, CINEA, Stand 2025",
    },
    {
        "id": "EU-IF-Pilot",
        "name": "EU Innovation Fund — Net-Zero Technologies (NZIA Pilot)",
        "type": "grant",
        "budget_eur": "€4 billion (estimated 2025-2030)",
        "max_grant_pct": "Up to 65% of eligible costs",
        "eligible_sectors": ["manufacturing", "energy", "technology"],
        "measures": ["solar_pv", "wind", "battery", "heat_pump", "electrolyzer", "grid_tech"],
        "project_scale": "Varies per call",
        "detail": "New pilot call under Net-Zero Industry Act (NZIA) for strategic net-zero technology manufacturing in EU.",
        "fund_source": "EU ETS Revenues + EU Budget (NZIA)",
        "source": "European Commission NZIA / Innovation Fund, Stand 2025",
    },
]

# ─── EU Modernisation Fund Programs ──────────────────────────────────
# Source: Modernisation Fund (EU ETS Art. 10d)

EU_MODERNISATION_FUND_PROGRAMS: list[dict] = [
    {
        "id": "EU-ModFund",
        "name": "EU Modernisation Fund",
        "type": "grant",
        "budget_eur": "~€24 billion (2021-2030, total)",
        "max_grant_pct": "Up to 70% of eligible costs",
        "eligible_countries": ["BG", "CZ", "EE", "HR", "HU", "LV", "LT", "PL", "RO", "SK", "SI"],
        "eligible_sectors": ["energy", "manufacturing", "transport", "construction"],
        "measures": [
            "energy_efficiency", "renewable_energy", "grid_modernization",
            "building_retrofit", "clean_transport", "district_heating",
        ],
        "detail": "Funds energy efficiency, renewable energy, and grid modernization in lower-income EU member states. Managed by EIB and national authorities.",
        "fund_source": "EU ETS Revenues (2% of total allowances auctioned, 2021-2030)",
        "source": "European Commission Modernisation Fund, EIB, Stand 2025",
    },
]

# ─── Just Transition Fund Programs ────────────────────────────────────
# Source: European Commission, Just Transition Mechanism (2021-2027)

EU_JUST_TRANSITION_PROGRAMS: list[dict] = [
    {
        "id": "EU-JTF",
        "name": "EU Just Transition Fund (JTF)",
        "type": "grant",
        "budget_eur": "€17.5 billion (2021-2027, total EU budget)",
        "max_grant_pct": "Up to 85% (less-developed regions), 70% (transition), 50% (more-developed)",
        "eligible_sectors": ["energy", "manufacturing", "technology", "agriculture"],
        "eligible_regions": "Regions with territorial just transition plans (TJTPs) approved by EC",
        "measures": [
            "clean_energy", "smme_support", "digitalization", "circular_economy",
            "workforce_retraining", "diversification", "green_innovation",
        ],
        "detail": "Supports regions heavily dependent on fossil fuels or high-carbon industries. Covers retraining, SME support, clean energy, and economic diversification.",
        "fund_source": "EU Multiannual Financial Framework 2021-2027",
        "source": "European Commission Just Transition Mechanism, Stand 2025",
    },
    {
        "id": "EU-JT-PIL",
        "name": "EU Just Transition — Public Sector Loan Facility",
        "type": "loan",
        "budget_eur": "€1.5 billion (EIB loans) + €525M EU grant component",
        "max_amount_eur": "Varies per project",
        "eligible_sectors": ["public", "energy", "transport", "technology"],
        "eligible_regions": "Same as JTF regions",
        "measures": [
            "district_heating", "grid_infrastructure", "energy_efficiency_public",
            "clean_transport", "digital_infrastructure", "social_infrastructure",
        ],
        "detail": "Blended finance (EIB loan + EU grant) for public sector climate transition investments in JTF regions.",
        "fund_source": "EIB + EU Budget (Public Sector Loan Facility)",
        "source": "European Commission / EIB, Just Transition Mechanism, Stand 2025",
    },
]

# ─── State-Level (Bundesland) Programs ───────────────────────────────
# Source: Federal State funding databases (Förderdatenbank)

BUNDESLAND_PROGRAMS: dict[str, list[dict]] = {
    "Bayern": [
        {
            "id": "BY-Klimabonus",
            "name": "Bayern — Klimabonus für Unternehmen",
            "type": "grant",
            "max_amount_eur": 500_000,
            "detail": "Investitionskostenzuschuss für Energieeffizienz, EE und Klimaanpassung im bayerischen Mittelstand",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "heat_recovery"],
            "source": "Bayerisches Staatsministerium für Wirtschaft, Stand 2025",
        },
    ],
    "Nordrhein-Westfalen": [
        {
            "id": "NRW-Klimaschutz",
            "name": "NRW — Klimaschutz-Offensive für Unternehmen",
            "type": "grant",
            "max_amount_eur": 200_000,
            "detail": "Zuschuss für Energieeffizienzmaßnahmen und erneuerbare Energien in NRW-Unternehmen",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "process_optimization"],
            "source": "NRW.BANK / Landesregierung NRW, Stand 2025",
        },
        {
            "id": "NRW-progress",
            "name": "NRW — progres.nrw (Programm Energieeffizienz)",
            "type": "grant",
            "max_amount_eur": 100_000,
            "detail": "Förderung von Energieeffizienz, Kraft-Wärme-Kopplung und erneuerbaren Energien",
            "eligible_measures": ["chp", "energy_efficiency", "renewable_energy", "heat_recovery"],
            "source": "NRW.BANK / Landesregierung NRW, Stand 2025",
        },
    ],
    "Baden-Württemberg": [
        {
            "id": "BW-Klimaschutz",
            "name": "BW — Klimaschutz-Plus (Unternehmen)",
            "type": "grant",
            "max_amount_eur": 200_000,
            "detail": "Zuschuss für Energieeffizienz, erneuerbare Prozesswärme und Abwärmenutzung",
            "eligible_measures": ["energy_efficiency", "process_heat_renewable", "heat_recovery"],
            "source": "Ministerium für Umwelt, Klima und Energiewirtschaft BW, Stand 2025",
        },
    ],
    "Hessen": [
        {
            "id": "HE-Klima",
            "name": "Hessen — Klimaschutzförderung für Unternehmen",
            "type": "grant",
            "max_amount_eur": 150_000,
            "detail": "Zuschuss für Energieeffizienz, erneuerbare Energien und betriebliches Mobilitätsmanagement",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "building_retrofit"],
            "source": "Hessisches Ministerium für Wirtschaft, Stand 2025",
        },
    ],
    "Sachsen": [
        {
            "id": "SN-RL-Klima",
            "name": "Sachsen — RL Klima (Klimaschutzförderung)",
            "type": "grant",
            "max_amount_eur": 300_000,
            "detail": "Zuschuss für Energieeffizienz, erneuerbare Energien und klimafreundliche Produktionsverfahren",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "process_optimization"],
            "source": "Sächsisches Staatsministerium für Energie, Stand 2025",
        },
    ],
    "Berlin": [
        {
            "id": "BE-Umwelt",
            "name": "Berlin — Umwelt- und Klimaschutzförderung",
            "type": "grant",
            "max_amount_eur": 100_000,
            "detail": "Zuschuss für Klimaschutzinvestitionen, Energieeffizienz und Kreislaufwirtschaft in Berliner Unternehmen",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "circular_economy"],
            "source": "Senatsverwaltung für Umwelt, Verkehr und Klimaschutz Berlin, Stand 2025",
        },
    ],
    "Niedersachsen": [
        {
            "id": "NI-Klima",
            "name": "Niedersachsen — Klimaschutz- und Energieagentur Förderung",
            "type": "grant",
            "max_amount_eur": 150_000,
            "detail": "Zuschuss für Energieberatung, Energieeffizienz und erneuerbare Energien",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "consulting"],
            "source": "Niedersächsische Klimaschutz- und Energieagentur, Stand 2025",
        },
    ],
    "Hamburg": [
        {
            "id": "HH-Klima",
            "name": "Hamburg — Hamburger Klimafonds",
            "type": "grant",
            "max_amount_eur": 100_000,
            "detail": "Zuschuss für Klimaschutzmaßnahmen in Hamburger Unternehmen, Gebäude und Infrastruktur",
            "eligible_measures": ["energy_efficiency", "renewable_energy", "building_retrofit", "green_infrastructure"],
            "source": "Behörde für Umwelt, Klima, Energie und Agrarwirtschaft Hamburg, Stand 2025",
        },
    ],
}

# ─── EU Taxonomy Technical Screening Criteria (Simplified) ────────────
# Source: EU Taxonomy Regulation (EU) 2020/852, Delegated Acts 2021/2139, 2023/2485

TAXONOMY_OBJECTIVES = [
    {"code": "CCM", "name": "Climate Change Mitigation", "regulation": "Climate Delegated Act (EU) 2021/2139"},
    {"code": "CCA", "name": "Climate Change Adaptation", "regulation": "Climate Delegated Act (EU) 2021/2139"},
    {"code": "WTR", "name": "Water & Marine Resources", "regulation": "Environmental Delegated Act (EU) 2023/2486"},
    {"code": "CE", "name": "Circular Economy", "regulation": "Environmental Delegated Act (EU) 2023/2486"},
    {"code": "PPC", "name": "Pollution Prevention & Control", "regulation": "Environmental Delegated Act (EU) 2023/2486"},
    {"code": "BIO", "name": "Biodiversity & Ecosystems", "regulation": "Environmental Delegated Act (EU) 2023/2486"},
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


# ─── Public API Functions ─────────────────────────────────────────────


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


async def get_bafa_programs(
    sector: str = "manufacturing",
    measure: str = "energy_audit",
) -> list[dict]:
    """
    Get matching BAFA funding programs.

    Args:
        sector: Business sector
        measure: Type of measure

    Returns:
        List of matching BAFA programs with module details
    """
    matches = []
    for prog in BAFA_PROGRAMS:
        sector_match = sector in prog["eligible_for"]
        measure_match = measure in prog["measures"] or any(
            m.startswith(measure) for m in prog["measures"]
        )
        if sector_match and measure_match:
            matches.append(prog)

    return matches


async def get_eu_innovation_fund() -> dict[str, Any]:
    """
    Get EU Innovation Fund program information.

    Returns:
        dict with available Innovation Fund programs
    """
    return {
        "programs": EU_INNOVATION_FUND_PROGRAMS,
        "total_programs": len(EU_INNOVATION_FUND_PROGRAMS),
        "fund_source": "EU ETS Revenues (450M allowances)",
        "total_budget_2020_2030": "~€40 billion (estimated at €75/tCO2)",
        "managing_authority": "CINEA (European Climate, Infrastructure and Environment Executive Agency)",
        "application_portal": "https://ec.europa.eu/info/funding-tenders/opportunities/",
        "data_source": "European Commission Innovation Fund, Stand 2025",
    }


async def get_eu_modernisation_fund() -> dict[str, Any]:
    """
    Get EU Modernisation Fund program information.

    Returns:
        dict with Modernisation Fund details
    """
    return {
        "programs": EU_MODERNISATION_FUND_PROGRAMS,
        "total_programs": len(EU_MODERNISATION_FUND_PROGRAMS),
        "fund_source": "EU ETS Revenues (2% of total allowances auctioned)",
        "total_budget_2021_2030": "~€24 billion (estimated at €75/tCO2)",
        "eligible_countries": EU_MODERNISATION_FUND_PROGRAMS[0]["eligible_countries"],
        "managing_authority": "EIB (European Investment Bank) + National Authorities",
        "application_portal": "https://modernisationfund.eu/",
        "data_source": "European Commission Modernisation Fund, EIB, Stand 2025",
    }


async def get_eu_just_transition_fund(region: Optional[str] = None) -> dict[str, Any]:
    """
    Get EU Just Transition Fund program information.

    Args:
        region: Optional specific region for just transition plans

    Returns:
        dict with Just Transition Fund programs
    """
    result = {
        "programs": EU_JUST_TRANSITION_PROGRAMS,
        "total_programs": len(EU_JUST_TRANSITION_PROGRAMS),
        "fund_source": "EU Multiannual Financial Framework 2021-2027",
        "total_budget_2021_2027": "€17.5 billion",
        "application_portal": "https://ec.europa.eu/regional_policy/funding/just-transition-fund",
        "data_source": "European Commission Just Transition Mechanism, Stand 2025",
        "note": "Requires an approved Territorial Just Transition Plan (TJTP) for eligible regions",
    }

    if region:
        result["requested_region"] = region
        result["check_tjtp"] = (
            f"Check if {region} has an approved TJTP: "
            "https://ec.europa.eu/regional_policy/funding/just-transition-fund/just-transition-platform"
        )

    return result


async def get_bundesland_programs(bundesland: Optional[str] = None) -> dict[str, Any]:
    """
    Get state-level (Bundesland) specific funding programs.

    Args:
        bundesland: Optional specific Bundesland (e.g., 'Bayern', 'NRW', 'Berlin')

    Returns:
        dict with state-level programs
    """
    if bundesland:
        programs = BUNDESLAND_PROGRAMS.get(bundesland)
        if programs is None:
            return {
                "error": f"Bundesland '{bundesland}' not found",
                "available_states": list(BUNDESLAND_PROGRAMS.keys()),
            }
        return {
            "bundesland": bundesland,
            "programs": programs,
            "total_programs": len(programs),
            "data_source": f"Förderdatenbank {bundesland}, Stand 2025",
        }

    return {
        "programs_by_state": BUNDESLAND_PROGRAMS,
        "total_states": len(BUNDESLAND_PROGRAMS),
        "total_programs": sum(len(v) for v in BUNDESLAND_PROGRAMS.values()),
        "data_source": "Förderdatenbank der Bundesländer, Stand 2025",
    }


async def check_eu_taxonomy_alignment(
    sector: str = "manufacturing",
    activity_type: str = "energy_efficiency",
    ghg_reduction_pct: float = 40.0,
    turnover_share_pct: float = 10.0,
    capex_share_pct: float = 15.0,
) -> dict[str, Any]:
    """
    Check EU Taxonomy-alignment of an economic activity for funding eligibility.

    Based on simplified technical screening criteria from EU Taxonomy Delegated Acts.

    Args:
        sector: Business sector
        activity_type: Type of activity (energy_efficiency, renewable_energy, etc.)
        ghg_reduction_pct: Estimated GHG reduction percentage from activity
        turnover_share_pct: Share of taxonomy-eligible turnover
        capex_share_pct: Share of taxonomy-eligible CAPEX

    Returns:
        dict with taxonomy eligibility, alignment assessment, and DNSH considerations
    """
    # Simplified technical screening criteria per activity type
    activity_criteria = {
        "energy_efficiency": {
            "description": "Energy efficiency improvements in industrial processes and buildings",
            "substantial_contribution": "≥30% energy savings or ≥30% GHG reduction vs baseline",
            "ghg_threshold": 30.0,
            "relevant_objectives": ["CCM"],
        },
        "renewable_energy": {
            "description": "Installation of renewable energy generation capacity",
            "substantial_contribution": "Electricity generation from renewable sources (solar, wind, hydro, geothermal, biomass)",
            "ghg_threshold": 0,
            "relevant_objectives": ["CCM"],
        },
        "building_retrofit": {
            "description": "Building renovation and energy efficiency upgrades",
            "substantial_contribution": "≥30% primary energy demand reduction, or compliance with NZEB standard",
            "ghg_threshold": 30.0,
            "relevant_objectives": ["CCM"],
        },
        "flood_protection": {
            "description": "Climate adaptation infrastructure",
            "substantial_contribution": "Reduction of physical climate risk by ≥1 class on 5-point scale",
            "ghg_threshold": 0,
            "relevant_objectives": ["CCA"],
        },
        "clean_transport": {
            "description": "Zero-emission transport and supporting infrastructure",
            "substantial_contribution": "Zero tailpipe CO2 emissions",
            "ghg_threshold": 100.0,
            "relevant_objectives": ["CCM"],
        },
    }

    criteria = activity_criteria.get(activity_type, {
        "description": "General economic activity",
        "substantial_contribution": "GHG reduction or environmental improvement",
        "ghg_threshold": 10.0,
        "relevant_objectives": ["CCM"],
    })

    # Check substantial contribution
    ghg_meets = ghg_reduction_pct >= criteria["ghg_threshold"]

    # Simplified DNSH (Do No Significant Harm) assessment
    dnsh_considerations = [
        {"principle": "Climate adaptation", "status": "Consider climate resilience of activity"},
        {"principle": "Water resources", "status": "Avoid water pollution in operation"},
        {"principle": "Circular economy", "status": "Design for durability, reparability, recyclability"},
        {"principle": "Pollution prevention", "status": "Comply with EU environmental standards"},
        {"principle": "Biodiversity", "status": "Avoid Natura 2000 sites, conduct EIA if needed"},
    ]

    # Minimum safeguards (OECD Guidelines, UN Guiding Principles)
    minimum_safeguards = [
        "Anti-corruption policy in place",
        "Human rights due diligence process",
        "Tax transparency",
        "Fair competition compliance",
    ]

    is_eligible = ghg_meets

    return {
        "activity": {
            "sector": sector,
            "activity_type": activity_type,
            "activity_description": criteria["description"],
        },
        "taxonomy_eligibility": {
            "is_eligible": is_eligible,
            "eligible_objectives": criteria["relevant_objectives"],
            "substantial_contribution_criteria": criteria["substantial_contribution"],
            "your_ghg_reduction_pct": ghg_reduction_pct,
            "required_ghg_reduction_pct": criteria["ghg_threshold"],
            "ghg_threshold_met": ghg_meets,
        },
        "taxonomy_kpis": {
            "turnover_eligibility_pct": turnover_share_pct,
            "turnover_alignment_pct": turnover_share_pct if is_eligible else 0,
            "capex_eligibility_pct": capex_share_pct,
            "capex_alignment_pct": capex_share_pct if is_eligible else 0,
            "opex_eligibility_pct": turnover_share_pct * 0.7 if turnover_share_pct > 0 else 0,
        },
        "dnsh_assessment": dnsh_considerations,
        "minimum_safeguards": minimum_safeguards,
        "taxonomy_regulation": "Regulation (EU) 2020/852, Climate Delegated Act (EU) 2021/2139",
        "data_source": "EU Taxonomy Regulation (EU) 2020/852, Delegated Acts 2021/2139, 2023/2485",
        "disclaimer": (
            "This is a simplified assessment for funding eligibility screening. "
            "Full taxonomy alignment requires detailed technical review against "
            "all applicable DNSH criteria and minimum safeguards."
        ),
    }


async def get_all_funding_programs(
    standort_art: str = "produktion",
    sector: str = "manufacturing",
    measure: str = "energy_efficiency",
    bundesland: Optional[str] = None,
) -> dict[str, Any]:
    """
    Get ALL available funding programs across all fund sources.

    Combines KfW, BAFA, EU Innovation Fund, EU Modernisation Fund,
    Just Transition Fund, and state-level programs.

    Args:
        standort_art: Type of location
        sector: Business sector
        measure: Type of measure
        bundesland: Optional Bundesland for state-level programs

    Returns:
        dict with all matching programs grouped by source
    """
    kfw = await get_funding_programs(standort_art, sector, measure)
    bafa = await get_bafa_programs(sector, measure)

    result = {
        "kfw_programs": kfw,
        "bafa_programs": bafa,
        "eu_innovation_fund": EU_INNOVATION_FUND_PROGRAMS,
        "eu_modernisation_fund": EU_MODERNISATION_FUND_PROGRAMS,
        "eu_just_transition_fund": EU_JUST_TRANSITION_PROGRAMS,
        "summary": {
            "kfw_count": len(kfw),
            "bafa_count": len(bafa),
            "eu_innovation_fund_count": len(EU_INNOVATION_FUND_PROGRAMS),
            "eu_modernisation_fund_count": len(EU_MODERNISATION_FUND_PROGRAMS),
            "eu_just_transition_fund_count": len(EU_JUST_TRANSITION_PROGRAMS),
            "total_programs": len(kfw) + len(bafa) + len(EU_INNOVATION_FUND_PROGRAMS)
                + len(EU_MODERNISATION_FUND_PROGRAMS) + len(EU_JUST_TRANSITION_PROGRAMS),
        },
        "data_source": "KfW, BAFA, European Commission (Innovation Fund, Modernisation Fund, JTF), Stand 2025",
    }

    if bundesland:
        bl_programs = await get_bundesland_programs(bundesland)
        if "programs" in bl_programs:
            result["bundesland_programs"] = bl_programs["programs"]
            result["summary"]["bundesland_count"] = bl_programs["total_programs"]
            result["summary"]["total_programs"] += bl_programs["total_programs"]

    return result
