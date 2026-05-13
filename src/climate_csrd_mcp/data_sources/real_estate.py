"""
Real Estate Climate & Energy Module — EPC Certificates, Renovation Roadmaps,
Building Climate Resilience, KfW Efficiency House, and CRREM/EU Taxonomy benchmarks.

Provides:
- Energy Performance Certificate (EPC) class estimation per national scheme
- Step-by-step renovation roadmaps with cost, savings, and funding eligibility
- Building climate resilience adaptation measures (flood, heat, storm)
- KfW Effizienzhaus funding details (EH 40, 55, 70, 85, 100, Denkmal)
- EPC industry benchmarks, CRREM stranding risk, and EU Taxonomy DNSH alignment

Sources:
- GEG 2024 (Gebäudeenergiegesetz): EH 40 = 40 % reference, EH 55 = 55 %, etc.
- KfW 261: €15K grant for EH 40 / QNG; KfW 358: up to €150K loan + €17.5K grant
- EnEV / GEG (DE), DPE (FR), EPC (UK), APE (IT) national schemes
- CRREM Consortium v2 pathways; DGNB / LEED / BREEAM certification criteria
- EU Taxonomy Delegated Acts (EU) 2021/2139, 2023/2485
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. ENERGY PERFORMANCE CERTIFICATE DATA
# ═══════════════════════════════════════════════════════════════════════════════

# EPC class thresholds in kWh/m²a (primary energy demand) — Deutschland (GEG 2024)
# Source: GEG §43, Anlage 1 — Energieausweis
# A+ = < 30, A = 30-49, B = 50-74, C = 75-99, D = 100-129,
# E = 130-159, F = 160-199, G = 200-249, H = ≥ 250
EPC_THRESHOLDS_DE: dict[str, float] = {
    "A+": 30,
    "A": 50,
    "B": 75,
    "C": 100,
    "D": 130,
    "E": 160,
    "F": 200,
    "G": 250,
    "H": float("inf"),
}

# National scheme references mapped to countries
EPC_SCHEMES: dict[str, dict[str, str]] = {
    "DE": {
        "scheme": "EnEV / GEG (Gebäudeenergiegesetz)",
        "certificate": "Energieausweis",
        "classes": "A+ to H (primary energy demand kWh/m²a)",
        "regulation": "GEG 2024 (BGBl. I 2023, Nr. 269)",
        "source": "https://www.gesetze-im-internet.de/geg/",
    },
    "FR": {
        "scheme": "DPE (Diagnostic de Performance Énergétique)",
        "certificate": "DPE",
        "classes": "A to G (primary energy + GHG emissions)",
        "regulation": "Arrêté du 31 mars 2021",
        "source": "https://www.ecologie.gouv.fr/dpe",
    },
    "UK": {
        "scheme": "EPC (Energy Performance Certificate)",
        "certificate": "EPC",
        "classes": "A to G (SAP / RdSAP rating)",
        "regulation": "The Energy Performance of Buildings (England and Wales) Regulations 2012",
        "source": "https://www.gov.uk/buy-sell-home-with-an-epc",
    },
    "IT": {
        "scheme": "APE (Attestato di Prestazione Energetica)",
        "certificate": "APE",
        "classes": "A4 to G (global energy performance index EPgl)",
        "regulation": "DM 26 giugno 2015 (recepimento EPBD)",
        "source": "https://www.efficienzaenergetica.enea.it/",
    },
}

# Primary energy demand baselines by building type (kWh/m²a)
# Source: dena Gebäudereport 2024, BMWSB, IWU
PRIMARY_ENERGY_BASELINE: dict[str, float] = {
    "residential_old": 220,      # Pre-1978, unrenovated
    "residential_modern": 120,    # 1995-2005, moderate insulation
    "residential_new": 65,       # Post-2020, current building code
    "office_old": 200,
    "office_modern": 110,
    "office_new": 60,
    "retail_old": 250,
    "retail_modern": 140,
    "retail_new": 80,
    "logistics_old": 180,
    "logistics_modern": 100,
    "logistics_new": 50,
    "hotel_old": 240,
    "hotel_modern": 140,
    "hotel_new": 75,
}

# CO₂ emission factors by energy source (kg CO₂/kWh)
# Source: GEMIS 5.0, UBA 2024
EMISSION_FACTORS: dict[str, float] = {
    "natural_gas": 0.240,
    "heating_oil": 0.310,
    "district_heating_mixed": 0.180,
    "district_heating_renewable": 0.050,
    "heat_pump_air": 0.120,       # Based on grid mix COP ~3.5
    "heat_pump_ground": 0.080,    # Based on grid mix COP ~4.5
    "biomass": 0.020,
    "solar_thermal": 0.000,
    "electricity_grid": 0.420,    # German grid mix 2024
    "electricity_green": 0.000,
}

# CO₂ emission class thresholds (kg CO₂/m²a) — based on DPE/EPC analogue
CO2_CLASS_THRESHOLDS: dict[str, float] = {
    "A+": 5,
    "A": 10,
    "B": 20,
    "C": 35,
    "D": 55,
    "E": 80,
    "F": 110,
    "G": 150,
    "H": float("inf"),
}


def _classify_epc(value_kwh_sqm: float, thresholds: dict[str, float]) -> str:
    """Map a numeric primary energy demand to an EPC letter class."""
    # Reverse-sort thresholds so we start from H (largest) down to A+
    sorted_classes = sorted(thresholds.keys(), key=lambda c: thresholds[c])
    for cls in sorted_classes:
        if value_kwh_sqm < thresholds[cls]:
            return cls
    return sorted_classes[-1]  # Fallback


async def get_energy_certificate(
    building_type: str = "residential_modern",
    country: str = "DE",
    energy_kwh_sqm: Optional[float] = None,
) -> dict[str, Any]:
    """
    Estimate energy performance certificate class for a building.

    Args:
        building_type: Building type (residential_old, residential_modern,
                       residential_new, office_*, retail_*, logistics_*, hotel_*)
        country: ISO 3166-1 alpha-2 country code (DE, FR, UK, IT)
        energy_kwh_sqm: Actual primary energy demand in kWh/m²a.
                         If None, uses baseline estimate for building type.

    Returns:
        dict with epc_class, primary_energy_kwh_sqm, co2_class,
              co2_kg_per_sqm, national_scheme, lifecycle_stage, data_source
    """
    cache = get_cache()
    cache_key = cache.make_key(
        "epc_cert", building_type, country,
        str(energy_kwh_sqm) if energy_kwh_sqm else "baseline",
    )
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Resolve baseline if no explicit value given
    if energy_kwh_sqm is None:
        energy_kwh_sqm = PRIMARY_ENERGY_BASELINE.get(building_type, 150.0)

    # Classify EPC class using DE thresholds (extensible)
    # For non-DE countries this gives an estimated equivalent
    epc_class = _classify_epc(energy_kwh_sqm, EPC_THRESHOLDS_DE)

    # Estimate CO₂ emissions using a typical fuel mix assumption:
    # Mix of 40% gas + 30% grid electricity + 30% district heating
    co2_per_sqm = (
        0.40 * EMISSION_FACTORS["natural_gas"]
        + 0.30 * EMISSION_FACTORS["electricity_grid"]
        + 0.30 * EMISSION_FACTORS["district_heating_mixed"]
    ) * energy_kwh_sqm
    co2_per_sqm = round(co2_per_sqm, 1)
    co2_class = _classify_epc(co2_per_sqm, CO2_CLASS_THRESHOLDS)

    # National scheme reference
    scheme = EPC_SCHEMES.get(
        country.upper(),
        {"scheme": "Unknown national scheme", "certificate": "N/A", "classes": "N/A",
         "regulation": "N/A", "source": ""},
    )

    # Determine lifecycle stage relevance
    if "new" in building_type:
        lifecycle_stage = "embodied_carbon_significant + operational_energy"
        lifecycle_note = (
            "New builds: operational energy at design target, "
            "but embodied carbon from construction materials dominates upfront"
        )
    elif "old" in building_type:
        lifecycle_stage = "operational_energy_dominant"
        lifecycle_note = (
            "Existing buildings: operational energy is the primary concern; "
            "retrofit reduces both operational energy and lifecycle carbon"
        )
    else:
        lifecycle_stage = "operational_energy"
        lifecycle_note = "Balanced focus — moderate embodied, significant operational"

    result = {
        "building_type": building_type,
        "country": country.upper(),
        "epc_class": epc_class,
        "epc_numeric_hint": f"~{energy_kwh_sqm:.0f} kWh/m²a primary energy",
        "primary_energy_kwh_sqm": round(energy_kwh_sqm, 0),
        "co2_emission_class": co2_class,
        "co2_emissions_kg_sqm_a": co2_per_sqm,
        "national_scheme": scheme["scheme"],
        "certificate_type": scheme["certificate"],
        "class_range": scheme["classes"],
        "regulation": scheme["regulation"],
        "scheme_source": scheme["source"],
        "lifecycle_stage": lifecycle_stage,
        "lifecycle_note": lifecycle_note,
        "data_source": (
            f"GEG 2024 / {scheme['scheme']} — "
            "primary energy baseline from dena Gebäudereport 2024; "
            "emission factors from GEMIS 5.0 / UBA 2024"
        ),
    }
    cache.set(cache_key, result, category="real_estate", ttl_days=30)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 2. RENOVATION ROADMAP DATA
# ═══════════════════════════════════════════════════════════════════════════════

# Renovation phases with typical measures, cost ranges, and savings
# Source: dena Sanierungsfahrplan, BEG/BAFA, KfW, BPIE
RENOVATION_PHASES: list[dict[str, Any]] = [
    {
        "phase": 1,
        "name": "Energy Audit & Planning",
        "measures": [
            "Professional energy audit (BAFA EBW funded)",
            "Individual renovation roadmap (iSFP)",
            "Building performance simulation",
        ],
        "cost_eur_m2": (5, 15),
        "savings_pct": (0, 0),
        "description": "Detailed assessment of building fabric, HVAC, and energy usage. "
                       "Foundation for all subsequent measures.",
    },
    {
        "phase": 2,
        "name": "Building Envelope — Roof & Top Floor Ceiling",
        "measures": [
            "Roof insulation (top floor ceiling)",
            "Roof replacement with integrated insulation",
            "Airtightness sealing",
        ],
        "cost_eur_m2": (80, 180),
        "savings_pct": (15, 25),
        "description": "Heat rises — insulating the top of the building yields the "
                       "highest savings per euro invested. Typical U-value target: ≤ 0.14 W/m²K.",
    },
    {
        "phase": 3,
        "name": "Building Envelope — Exterior Walls",
        "measures": [
            "External thermal insulation composite system (ETICS / WDVS)",
            "Cavity wall insulation",
            "Interior insulation (for listed buildings)",
        ],
        "cost_eur_m2": (120, 280),
        "savings_pct": (20, 35),
        "description": "External wall insulation reduces heat loss by 60-80%. "
                       "U-value target: ≤ 0.20 W/m²K. Payback typically 8-15 years.",
    },
    {
        "phase": 4,
        "name": "Building Envelope — Windows & Doors",
        "measures": [
            "Triple-glazed windows (U-value ≤ 0.8 W/m²K)",
            "Insulated entry doors",
            "Window frame and reveal insulation",
        ],
        "cost_eur_m2": (250, 600),  # Per m² of window area
        "savings_pct": (10, 20),
        "description": "Modern triple-glazed windows reduce heat loss by 70% vs old single-glazed. "
                       "Combined with exterior wall insulation for best envelope performance.",
    },
    {
        "phase": 5,
        "name": "HVAC & Renewable Energy Systems",
        "measures": [
            "Heat pump installation (air-source or ground-source)",
            "Solar thermal for domestic hot water",
            "PV system for on-site renewable electricity",
            "Mechanical ventilation with heat recovery",
            "Biomass boiler replacement",
        ],
        "cost_eur_m2": (150, 400),
        "savings_pct": (20, 40),
        "description": "Replace fossil-fuel heating (gas/oil) with renewable systems. "
                       "Heat pumps reduce CO₂ by 60-80% vs gas. BAFA BEG grants available.",
    },
    {
        "phase": 6,
        "name": "Basement Floor Insulation & Commissioning",
        "measures": [
            "Basement ceiling / ground floor insulation",
            "Hydronic balancing of heating system",
            "Smart thermostats and building automation",
            "Final blower-door test & commissioning",
        ],
        "cost_eur_m2": (40, 120),
        "savings_pct": (5, 12),
        "description": "Floor insulation reduces heat loss to ground. "
                       "Hydronic balancing alone saves 10-15% on heating energy. "
                       "Final commissioning ensures all measures perform as designed.",
    },
]

# KfW / BAFA funding eligibility per phase
# Source: KfW 261, 358, 359; BAFA BEG (Bundesförderung Energieeffiziente Gebäude)
FUNDING_ELIGIBILITY: dict[int, list[dict[str, Any]]] = {
    1: [
        {"program": "BAFA EBW", "type": "grant",
         "max_eur": 10_000, "note": "80% of energy audit cost covered"},
        {"program": "KfW 359", "type": "grant",
         "max_eur": 5_000, "note": "Add-on for renovation roadmap (iSFP)"},
    ],
    2: [
        {"program": "BAFA BEG EM", "type": "grant",
         "max_eur": 60_000, "pct": 0.20,
         "note": "Einzelmaßnahme: 20% grant for roof insulation"},
        {"program": "KfW 261/358", "type": "loan_grant",
         "max_eur": 150_000, "pct": 0.15,
         "note": "As part of whole-building efficiency house package"},
    ],
    3: [
        {"program": "BAFA BEG EM", "type": "grant",
         "max_eur": 60_000, "pct": 0.20,
         "note": "Einzelmaßnahme: 20% grant for wall insulation"},
        {"program": "KfW 261/358", "type": "loan_grant",
         "max_eur": 150_000, "note": "As part of whole-building package"},
    ],
    4: [
        {"program": "BAFA BEG EM", "type": "grant",
         "max_eur": 60_000, "pct": 0.20,
         "note": "20% grant for window replacement"},
    ],
    5: [
        {"program": "BAFA BEG EM", "type": "grant",
         "max_eur": 60_000, "pct": 0.25,
         "note": "25-35% grant for heat pump (plus 5% natural refrigerant bonus)"},
        {"program": "KfW 270/271", "type": "loan",
         "max_eur": 100_000_000, "note": "Renewable energy systems loan"},
        {"program": "BAFA Biomasse", "type": "grant",
         "max_eur": 500_000, "pct": 0.35,
         "note": "Biomass boiler grant up to 35%"},
    ],
    6: [
        {"program": "BAFA BEG EM", "type": "grant",
         "max_eur": 60_000, "pct": 0.20,
         "note": "20% grant for basement insulation"},
    ],
}

# CRREM pathway alignment: max kg CO₂/m² for 1.5°C scenario by building type
# Source: CRREM v2, see crrem.py module
CRREM_PATHWAY_TARGETS: dict[str, int] = {
    "residential": 2030,
    "office": 2030,
    "retail": 2030,
    "logistics": 2030,
    "hotel": 2030,
}


async def get_renovation_roadmap(
    building_type: str = "residential_modern",
    current_class: str = "E",
    target_class: str = "A",
    country: str = "DE",
) -> dict[str, Any]:
    """
    Generate a step-by-step renovation roadmap to improve EPC class.

    Args:
        building_type: Building type identifier
        current_class: Current EPC class (A+ to H)
        target_class: Target EPC class (default 'A')
        country: Country code (default 'DE')

    Returns:
        dict with phases, costs, savings, funding, CRREM alignment, payback
    """
    cache = get_cache()
    cache_key = cache.make_key(
        "renov_roadmap", building_type, current_class, target_class, country,
    )
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Map class letter to numeric energy value (primary energy kWh/m²a)
    # Use the EPC thresholds upper bound for the current class
    sorted_classes = ["A+", "A", "B", "C", "D", "E", "F", "G", "H"]
    class_to_energy = {}
    prev_thresh = 0.0
    for c in sorted_classes:
        thresh = EPC_THRESHOLDS_DE[c]
        class_to_energy[c] = prev_thresh + (thresh - prev_thresh) / 2
        prev_thresh = thresh

    current_energy = class_to_energy.get(current_class.upper(), 180.0)
    target_energy = class_to_energy.get(target_class.upper(), 40.0)

    # How many classes we need to jump
    if current_class.upper() in sorted_classes and target_class.upper() in sorted_classes:
        idx_current = sorted_classes.index(current_class.upper())
        idx_target = sorted_classes.index(target_class.upper())
        class_gap = idx_current - idx_target
    else:
        class_gap = 3  # default moderate gap

    # Determine how many phases are realistically needed
    # Deeper renovation = more phases, higher cost
    if class_gap <= 0:
        needed_phases = 0  # Already at or above target
    elif class_gap <= 2:
        needed_phases = 3  # Light retrofit (phases 1-3)
    elif class_gap <= 4:
        needed_phases = 5  # Medium retrofit (phases 1-5)
    else:
        needed_phases = 6  # Deep retrofit (phases 1-6)

    # Build the phased roadmap
    phases_output = []
    total_cost_low = 0.0
    total_cost_high = 0.0
    cumulative_savings_low = 0.0
    cumulative_savings_high = 0.0

    for phase_info in RENOVATION_PHASES:
        phase_num = phase_info["phase"]
        if phase_num > needed_phases:
            break

        cost_low, cost_high = phase_info["cost_eur_m2"]
        sav_low, sav_high = phase_info["savings_pct"]

        # Apply building type multiplier on costs
        # Residential is baseline (1.0); commercial is typically 1.2-1.5x
        type_multiplier = 1.0
        if building_type.startswith("office"):
            type_multiplier = 1.2
        elif building_type.startswith("retail"):
            type_multiplier = 1.3
        elif building_type.startswith("hotel"):
            type_multiplier = 1.4
        elif building_type.startswith("logistics"):
            type_multiplier = 1.1

        adj_cost_low = round(cost_low * type_multiplier, 0)
        adj_cost_high = round(cost_high * type_multiplier, 0)

        # Get funding for this phase
        funding = FUNDING_ELIGIBILITY.get(phase_num, [])

        # CRREM pathway note
        crrem_note = "Not yet aligned" if phase_num < 4 else "Partially aligned"
        if phase_num >= 5 and class_gap >= 3:
            crrem_note = "Aligned with CRREM 1.5°C pathway for 2030 target"

        phase_entry = {
            "phase": phase_num,
            "name": phase_info["name"],
            "measures": phase_info["measures"],
            "cost_eur_m2": {"low": adj_cost_low, "high": adj_cost_high},
            "energy_savings_pct": {"low": sav_low, "high": sav_high},
            "description": phase_info["description"],
            "funding_available": funding,
            "crrem_alignment_note": crrem_note,
        }
        phases_output.append(phase_entry)

        total_cost_low += adj_cost_low
        total_cost_high += adj_cost_high
        cumulative_savings_low += sav_low
        cumulative_savings_high += sav_high

    # Payback period estimate (simple, years)
    # Assume €0.25/kWh energy cost, baseline ~200 kWh/m²
    energy_cost_per_kwh = 0.25  # €/kWh, typical German household rate 2024
    annual_savings_low = cumulative_savings_low / 100 * current_energy * energy_cost_per_kwh
    annual_savings_high = cumulative_savings_high / 100 * current_energy * energy_cost_per_kwh
    payback_low = (
        total_cost_low / max(annual_savings_high, 1)
        if annual_savings_high > 0 else 999
    )
    payback_high = (
        total_cost_high / max(annual_savings_low, 1)
        if annual_savings_low > 0 else 999
    )

    # KfW efficiency class eligibility based on target
    kfw_class = None
    if target_class.upper() in ("A+", "A") and class_gap >= 2:
        # Deep renovation likely achieves EH 85 or better
        if class_gap >= 4:
            kfw_class = "Effizienzhaus 40"
        elif class_gap >= 3:
            kfw_class = "Effizienzhaus 55"
        else:
            kfw_class = "Effizienzhaus 85"
    elif target_class.upper() in ("B", "C"):
        kfw_class = "Effizienzhaus 100"

    result = {
        "building_type": building_type,
        "country": country.upper(),
        "current_epc_class": current_class.upper(),
        "target_epc_class": target_class.upper(),
        "current_estimated_energy_kwh_sqm": round(current_energy, 0),
        "target_estimated_energy_kwh_sqm": round(target_energy, 0),
        "class_gap": max(0, class_gap),
        "phases": phases_output,
        "total_cost_eur_m2": {
            "low": total_cost_low,
            "high": total_cost_high,
        },
        "cumulative_savings_pct": {
            "low": cumulative_savings_low,
            "high": cumulative_savings_high,
        },
        "estimated_payback_years": {
            "optimistic": round(payback_low, 1),
            "pessimistic": round(payback_high, 1),
        },
        "kfw_efficiency_class_eligibility": kfw_class,
        "kfw_funding_summary": {
            "kfw_261": {
                "max_loan_eur": 150_000,
                "grant_eur": 15_000,
                "condition": "Only for EH 40 with QNG certification",
                "note": "KfW 261: €15K grant for klimafreundlichen Neubau",
            },
            "kfw_358": {
                "max_loan_eur": 150_000,
                "max_grant_eur": 17_500,
                "condition": "Non-residential buildings, EH 40 standard",
                "note": "KfW 358: up to €150K loan + €17.5K grant",
            },
            "kfw_359": {
                "max_grant_eur": 5_000,
                "condition": "Renovation roadmap (iSFP) supplement",
                "note": "KfW 359: grant for energy consulting",
            },
        },
        "crrem_pathway_alignment": {
            "aligned_with_1_5c": class_gap >= 4,
            "aligned_with_wb2c": class_gap >= 3,
            "target_year_for_stranding_risk": 2030,
        },
        "recommended_priority": (
            "Deep energy retrofit recommended: focus on building envelope "
            "(roof + walls) first, then HVAC systems. Max 20-25 year horizon."
        ),
        "data_source": (
            "dena Sanierungsfahrplan / BAFA BEG / KfW 261/358/359 / "
            "CRREM v2 / BPIE renovation cost database — 2024-2025"
        ),
    }
    cache.set(cache_key, result, category="real_estate", ttl_days=30)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. BUILDING CLIMATE RESILIENCE
# ═══════════════════════════════════════════════════════════════════════════════

# Climate adaptation measures per risk type with cost estimates
# Source: BBSR Klimaanpassung im Gebäudebestand, DGNB, EIB Climate Adaptation
FLOOD_MEASURES: list[dict[str, Any]] = [
    {
        "measure": "Mobile flood barriers (doors & windows)",
        "description": "Demountable watertight barriers at all ground-level openings",
        "cost_eur_m2": (100, 300),
        "effectiveness": "high",
        "cert_points": {"DGNB": 4, "LEED": 2, "BREEAM": 3},
    },
    {
        "measure": "Permanent flood walls / floodproofing",
        "description": "Reinforced concrete/steel walls around building perimeter",
        "cost_eur_m2": (250, 600),
        "effectiveness": "very_high",
        "cert_points": {"DGNB": 6, "LEED": 3, "BREEAM": 5},
    },
    {
        "measure": "Enhanced drainage & sump pump system",
        "description": "French drains, perimeter drainage, redundant sump pumps with backup power",
        "cost_eur_m2": (50, 150),
        "effectiveness": "moderate",
        "cert_points": {"DGNB": 2, "LEED": 1, "BREEAM": 2},
    },
    {
        "measure": "Building elevation (raising structure)",
        "description": "Structural jacking and raising of the building above flood level",
        "cost_eur_m2": (400, 1000),
        "effectiveness": "very_high",
        "cert_points": {"DGNB": 8, "LEED": 4, "BREEAM": 6},
    },
    {
        "measure": "Water-resistant materials (ground floor)",
        "description": "Closed-cell insulation, water-resistant gypsum, raised electrics",
        "cost_eur_m2": (60, 180),
        "effectiveness": "moderate",
        "cert_points": {"DGNB": 3, "LEED": 1, "BREEAM": 2},
    },
]

HEAT_MEASURES: list[dict[str, Any]] = [
    {
        "measure": "Green roof (extensive/intensive)",
        "description": "Vegetated roof: reduces urban heat island, provides insulation, retains rainwater",
        "cost_eur_m2": (40, 150),
        "effectiveness": "high",
        "cert_points": {"DGNB": 6, "LEED": 4, "BREEAM": 5},
    },
    {
        "measure": "Enhanced roof & wall insulation",
        "description": "Increased insulation thickness (U-value ≤ 0.14 W/m²K) for overheating protection",
        "cost_eur_m2": (80, 200),
        "effectiveness": "high",
        "cert_points": {"DGNB": 4, "LEED": 2, "BREEAM": 3},
    },
    {
        "measure": "External solar shading",
        "description": "Automated external blinds, brise-soleil, or overhangs to block direct solar gain",
        "cost_eur_m2": (100, 350),  # Per m² of window area
        "effectiveness": "very_high",
        "cert_points": {"DGNB": 5, "LEED": 3, "BREEAM": 4},
    },
    {
        "measure": "High-efficiency HVAC with heat recovery",
        "description": "Cooling-capable heat pump, mechanical ventilation with passive cooling bypass",
        "cost_eur_m2": (100, 300),
        "effectiveness": "high",
        "cert_points": {"DGNB": 4, "LEED": 3, "BREEAM": 3},
    },
    {
        "measure": "Cool surfaces / albedo management",
        "description": "High-albedo roof coating, light-coloured facade materials, cool pavements",
        "cost_eur_m2": (15, 60),
        "effectiveness": "moderate",
        "cert_points": {"DGNB": 3, "LEED": 2, "BREEAM": 2},
    },
    {
        "measure": "Passive night cooling & thermal mass",
        "description": "Exposed concrete ceilings, automated night ventilation, phase-change materials",
        "cost_eur_m2": (30, 120),
        "effectiveness": "moderate",
        "cert_points": {"DGNB": 4, "LEED": 2, "BREEAM": 3},
    },
]

STORM_MEASURES: list[dict[str, Any]] = [
    {
        "measure": "Structural reinforcement (roof & connections)",
        "description": "Hurricane straps, reinforced roof-to-wall connections, impact-resistant decking",
        "cost_eur_m2": (80, 250),
        "effectiveness": "very_high",
        "cert_points": {"DGNB": 5, "LEED": 2, "BREEAM": 4},
    },
    {
        "measure": "Impact-resistant glazing & storm shutters",
        "description": "Laminated/impact-rated glass, roll-down or accordion storm shutters",
        "cost_eur_m2": (200, 500),
        "effectiveness": "very_high",
        "cert_points": {"DGNB": 4, "LEED": 2, "BREEAM": 3},
    },
    {
        "measure": "Reinforced facade & cladding anchoring",
        "description": "Enhanced anchorage for exterior cladding, rain-screen systems",
        "cost_eur_m2": (50, 180),
        "effectiveness": "high",
        "cert_points": {"DGNB": 3, "LEED": 1, "BREEAM": 2},
    },
    {
        "measure": "Drainage & waterproofing upgrades",
        "description": "Over-sized downspouts, scupper drains, emergency overflow systems",
        "cost_eur_m2": (30, 100),
        "effectiveness": "moderate",
        "cert_points": {"DGNB": 2, "LEED": 1, "BREEAM": 2},
    },
]

# Building type multipliers for resilience costs
RESILIENCE_TYPE_MULTIPLIERS: dict[str, float] = {
    "residential": 1.0,
    "office": 1.2,
    "retail": 1.3,
    "logistics": 1.0,
    "hotel": 1.4,
}


async def get_building_climate_resilience(
    building_type: str = "residential",
    flood_risk: str = "low",
    heat_risk: str = "medium",
    storm_risk: str = "low",
) -> dict[str, Any]:
    """
    Assess building climate resilience and recommend adaptation measures.

    Args:
        building_type: Building type (residential, office, retail, logistics, hotel)
        flood_risk: Flood risk level ('low', 'medium', 'high', 'very_high')
        heat_risk: Heat risk level ('low', 'medium', 'high', 'very_high')
        storm_risk: Storm risk level ('low', 'medium', 'high', 'very_high')

    Returns:
        dict with recommended measures per risk type, cost estimates,
              green building certification points, data_source
    """
    cache = get_cache()
    cache_key = cache.make_key(
        "bldg_resilience", building_type, flood_risk, heat_risk, storm_risk,
    )
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Risk priority: how many measures to recommend per risk level
    risk_to_priority = {"low": 1, "medium": 2, "high": 3, "very_high": 4}

    type_mult = RESILIENCE_TYPE_MULTIPLIERS.get(building_type, 1.0)

    # Select measures based on risk levels
    def _select_measures(
        measure_list: list[dict], risk_level: str, multiplier: float,
    ) -> list[dict]:
        count = min(risk_to_priority.get(risk_level, 2), len(measure_list))
        selected = []
        for i in range(count):
            m = measure_list[i]
            c_low, c_high = m["cost_eur_m2"]
            selected.append({
                "measure": m["measure"],
                "description": m["description"],
                "cost_eur_m2": {
                    "low": round(c_low * multiplier, 0),
                    "high": round(c_high * multiplier, 0),
                },
                "effectiveness": m["effectiveness"],
            })
        return selected

    flood_selected = _select_measures(FLOOD_MEASURES, flood_risk, type_mult)
    heat_selected = _select_measures(HEAT_MEASURES, heat_risk, type_mult)
    storm_selected = _select_measures(STORM_MEASURES, storm_risk, type_mult)

    # Total cost estimates
    all_measures = flood_selected + heat_selected + storm_selected
    total_cost_low = sum(m["cost_eur_m2"]["low"] for m in all_measures)
    total_cost_high = sum(m["cost_eur_m2"]["high"] for m in all_measures)

    # Certification point estimates
    cert_scores = {"DGNB": 0, "LEED": 0, "BREEAM": 0}
    for m, level in [
        (FLOOD_MEASURES, flood_risk),
        (HEAT_MEASURES, heat_risk),
        (STORM_MEASURES, storm_risk),
    ]:
        count = min(risk_to_priority.get(level, 2), len(m))
        for i in range(count):
            pts = m[i].get("cert_points", {})
            for scheme, points in pts.items():
                if scheme in cert_scores:
                    cert_scores[scheme] += points

    # Overall resilience score (0-100)
    # Based on risk levels and cost coverage
    risk_scores = {
        "flood": {"low": 10, "medium": 30, "high": 60, "very_high": 90},
        "heat": {"low": 10, "medium": 25, "high": 50, "very_high": 80},
        "storm": {"low": 10, "medium": 25, "high": 50, "very_high": 80},
    }
    base_risk = (
        risk_scores["flood"].get(flood_risk, 10)
        + risk_scores["heat"].get(heat_risk, 10)
        + risk_scores["storm"].get(storm_risk, 10)
    ) / 3
    # Higher cost measures = better resilience
    coverage_factor = min(1.0, total_cost_high / 1500)
    resilience_score = round(min(100, base_risk * (0.5 + 0.5 * coverage_factor)), 0)

    resilience_label = (
        "critical" if resilience_score < 25 else
        "low" if resilience_score < 45 else
        "moderate" if resilience_score < 65 else
        "good" if resilience_score < 85 else
        "excellent"
    )

    result = {
        "building_type": building_type,
        "climate_risks_assessed": {
            "flood_risk": flood_risk,
            "heat_risk": heat_risk,
            "storm_risk": storm_risk,
        },
        "resilience_score": resilience_score,
        "resilience_label": resilience_label,
        "recommended_measures": {
            "flood_protection": flood_selected,
            "heat_adaptation": heat_selected,
            "storm_protection": storm_selected,
        },
        "total_cost_eur_m2": {
            "low": total_cost_low,
            "high": total_cost_high,
        },
        "green_building_certification_points": {
            "DGNB": {"estimated_points": cert_scores["DGNB"], "max_relevant": 100},
            "LEED": {"estimated_points": cert_scores["LEED"], "max_relevant": 50},
            "BREEAM": {"estimated_points": cert_scores["BREEAM"], "max_relevant": 50},
        },
        "recommendation_priority": (
            "Address highest risk first. For flood > heat > storm order, "
            "structural measures at foundation level. For heat > flood, "
            "envelope and shading first."
        ),
        "data_source": (
            "BBSR Klimaanpassung im Gebäudebestand (2023) / "
            "DGNB Kriterium ENV1.2 / LEED EA Pilot Credits / "
            "BREEAM WAT-01 / EIB Climate Adaptation Guidelines"
        ),
    }
    cache.set(cache_key, result, category="real_estate", ttl_days=30)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 4. KFW EFFICIENZHAUS
# ═══════════════════════════════════════════════════════════════════════════════

# KfW Efficiency House classes per GEG 2024
# Source: GEG 2024 §22, Anlagen 1-2; KfW Merkblatt 261/358/359
# EH 40 = 40 % of reference building primary energy / 45 % transmission heat loss
# EH 55 = 55 % / 60 %, EH 70 = 70 % / 75 %, EH 85 = 85 % / 90 %, EH 100 = 100 % / 100 %

KFW_EFFICIENCY_CLASSES: dict[str, dict[str, Any]] = {
    "40_plus": {
        "label": "Effizienzhaus 40 Plus",
        "primary_energy_pct": 40,
        "transmission_loss_pct": 45,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 150_000, "grant_eur": 15_000,
             "condition": "Neubau Wohngebäude, QNG Nachhaltigkeitszertifikat erforderlich"},
            {"program": "KfW 358", "max_loan_eur": 150_000, "grant_eur": 17_500,
             "condition": "Nichtwohngebäude, Neubau oder Ersterwerb"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 40 % of GEG reference building",
            "Transmission heat loss: ≤ 45 % of reference",
            "Mandatory renewable heating system (heat pump, solar thermal, or district heating)",
            "PV system required (≥ 4 kWp per residential unit for EH 40 Plus)",
            "Battery storage recommended (≥ 5 kWh per residential unit for EH 40 Plus)",
            "Mechanical ventilation with ≥ 75 % heat recovery efficiency",
        ],
        "qng_required": True,
        "description": "Highest efficiency class. Nearly-zero energy building with on-site "
                       "renewable generation. Eligible for maximum KfW grants.",
    },
    "40": {
        "label": "Effizienzhaus 40",
        "primary_energy_pct": 40,
        "transmission_loss_pct": 45,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 150_000, "grant_eur": 12_500,
             "condition": "Neubau Wohngebäude"},
            {"program": "KfW 358", "max_loan_eur": 150_000, "grant_eur": 15_000,
             "condition": "Nichtwohngebäude"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 40 % of GEG reference building",
            "Transmission heat loss: ≤ 45 % of reference",
            "Renewable heating system required",
            "Triple-glazed windows (U_g ≤ 0.8 W/m²K)",
            "Highly insulated envelope (U ≤ 0.20 W/m²K walls, ≤ 0.14 W/m²K roof)",
        ],
        "qng_required": False,
        "description": "Very high efficiency. GEG 2024 target standard for new buildings. "
                       "Significant KfW funding available.",
    },
    "55": {
        "label": "Effizienzhaus 55",
        "primary_energy_pct": 55,
        "transmission_loss_pct": 60,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 120_000, "grant_eur": 10_000,
             "condition": "Neubau Wohngebäude"},
            {"program": "KfW 358", "max_loan_eur": 120_000, "grant_eur": 12_000,
             "condition": "Nichtwohngebäude"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 55 % of GEG reference building",
            "Transmission heat loss: ≤ 60 % of reference",
            "Good insulation standard (U ≤ 0.24 W/m²K walls, ≤ 0.18 W/m²K roof)",
            "Triple-glazed or high-performance double-glazed windows",
        ],
        "qng_required": False,
        "description": "High efficiency. Common target for both new builds and deep retrofits. "
                       "Good balance of cost and energy performance.",
    },
    "70": {
        "label": "Effizienzhaus 70",
        "primary_energy_pct": 70,
        "transmission_loss_pct": 75,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 100_000, "grant_eur": 8_000,
             "condition": "Neubau Wohngebäude"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 70 % of GEG reference building",
            "Transmission heat loss: ≤ 75 % of reference",
            "Moderate insulation standard",
            "Efficient heating system typically required",
        ],
        "qng_required": False,
        "description": "Good efficiency. Achievable with moderate renovation measures. "
                       "Lower funding but still significant energy savings.",
    },
    "85": {
        "label": "Effizienzhaus 85",
        "primary_energy_pct": 85,
        "transmission_loss_pct": 90,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 80_000, "grant_eur": 5_000,
             "condition": "Neubau Wohngebäude"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 85 % of GEG reference building",
            "Transmission heat loss: ≤ 90 % of reference",
            "Standard insulation level",
            "Efficient windows and heating system",
        ],
        "qng_required": False,
        "description": "Standard efficiency. Baseline for many renovation projects. "
                       "Meets minimum regulatory requirements in many cases.",
    },
    "100": {
        "label": "Effizienzhaus 100",
        "primary_energy_pct": 100,
        "transmission_loss_pct": 100,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 60_000, "grant_eur": 3_500,
             "condition": "Neubau Wohngebäude"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 100 % of GEG reference building",
            "Transmission heat loss: ≤ 100 % of reference",
            "Meets GEG minimum standard",
        ],
        "qng_required": False,
        "description": "Minimum standard. Equivalent to GEG 2024 reference building. "
                       "Basic funding available.",
    },
    "denkmal": {
        "label": "Effizienzhaus Denkmal",
        "primary_energy_pct": 160,
        "transmission_loss_pct": 160,
        "grant_loan_eligible": True,
        "grants": [
            {"program": "KfW 261", "max_loan_eur": 100_000, "grant_eur": 7_500,
             "condition": "Wohngebäude, Baudenkmal oder besonders erhaltenswerte Bausubstanz"},
        ],
        "technical_requirements": [
            "Primary energy: ≤ 160 % of GEG reference building (relaxed for heritage)",
            "Interior insulation solutions compatible with historic fabric",
            "Compromise solutions for windows (e.g., slim-profile double glazing)",
            "Heritage-compatible HVAC systems",
        ],
        "qng_required": False,
        "description": "Special class for listed/heritage buildings. Relaxed requirements "
                       "acknowledge structural limitations while still improving efficiency.",
    },
}

# EH Denkmal = 160 % max, for heritage buildings


async def get_kfw_efficiency_house(
    efficiency_class: str = "40",
    building_type: str = "residential",
) -> dict[str, Any]:
    """
    Get KfW Effizienzhaus details for a given efficiency class.

    Args:
        efficiency_class: '40_plus', '40', '55', '70', '85', '100', 'denkmal'
        building_type: 'residential' or 'non_residential'

    Returns:
        dict with efficiency class details, funding, requirements, description
    """
    cache = get_cache()
    cache_key = cache.make_key("kfw_eh", efficiency_class, building_type)
    cached = cache.get(cache_key)
    if cached:
        return cached

    ec = efficiency_class.lower().replace(" ", "_")
    if ec not in KFW_EFFICIENCY_CLASSES:
        ec = "55"  # Default fallback

    class_data = KFW_EFFICIENCY_CLASSES[ec]

    # Filter grants for the specific building type
    applicable_grants = []
    for grant in class_data["grants"]:
        program = grant["program"]
        if building_type == "non_residential" and program in ("KfW 358",):
            applicable_grants.append(grant)
        elif building_type == "residential" and program in ("KfW 261",):
            applicable_grants.append(grant)
        else:
            applicable_grants.append(grant)  # Include all for general query

    result = {
        "efficiency_class": ec,
        "label": class_data["label"],
        "primary_energy_pct": class_data["primary_energy_pct"],
        "transmission_loss_pct": class_data["transmission_loss_pct"],
        "primary_energy_note": (
            f"EH {ec.replace('_plus', ' Plus').replace('denkmal', ' Denkmal')}: "
            f"primary energy demand = {class_data['primary_energy_pct']} % of "
            f"GEG 2024 reference building (§22 Anlage 1-2)"
        ),
        "building_type": building_type,
        "grants_and_loans": applicable_grants,
        "technical_requirements": class_data["technical_requirements"],
        "qng_certification_required": class_data["qng_required"],
        "description": class_data["description"],
        "geg_2024_reference": (
            "GEG 2024 reference building is defined in Anlage 1 (Wohngebäude) and "
            "Anlage 2 (Nichtwohngebäude). Primary energy factor for electricity: 0.4 "
            "(reduced from 1.8 in older versions reflecting renewable grid mix)."
        ),
        "data_source": (
            "GEG 2024 (BGBl. I 2023, Nr. 269) / KfW Merkblatt 261, 358, 359 / "
            "KfW Stand 2025: https://www.kfw.de/inlandsfoerderung/"
        ),
    }
    cache.set(cache_key, result, category="real_estate", ttl_days=30)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 5. EPC BENCHMARKS
# ═══════════════════════════════════════════════════════════════════════════════

# EPC distribution data by country and building type (% of buildings per class)
# Source: BPIE (Buildings Performance Institute Europe) 2023; dena Gebäudereport 2024
EPC_DISTRIBUTION: dict[str, dict[str, list[float]]] = {
    "DE": {
        "office": [1, 3, 8, 15, 25, 25, 15, 6, 2],  # A+ to H
        "residential": [1, 2, 5, 12, 20, 28, 18, 10, 4],
        "retail": [2, 3, 7, 14, 22, 26, 16, 7, 3],
        "logistics": [3, 5, 10, 18, 22, 22, 12, 6, 2],
        "hotel": [1, 2, 5, 10, 18, 25, 20, 12, 7],
    },
    "FR": {
        "office": [2, 4, 10, 18, 22, 22, 14, 8],
        "residential": [1, 3, 8, 14, 22, 25, 18, 9],
    },
    "UK": {
        "office": [3, 5, 12, 18, 22, 20, 12, 8],
        "residential": [2, 4, 10, 16, 24, 22, 14, 8],
    },
    "IT": {
        "office": [1, 3, 8, 15, 22, 25, 16, 10],
        "residential": [1, 2, 6, 12, 20, 28, 20, 11],
    },
}

# CRREM stranding risk by EPC class — simplified mapping
# Source: CRREM v2 stranding risk methodology; BPIE
CRREM_STRANDING_BY_EPC: dict[str, dict[str, Any]] = {
    "A+": {"stranding_risk": "very_low", "years_to_stranding": 2045, "capex_risk_eur_m2": 0},
    "A": {"stranding_risk": "low", "years_to_stranding": 2038, "capex_risk_eur_m2": 50},
    "B": {"stranding_risk": "low_moderate", "years_to_stranding": 2033, "capex_risk_eur_m2": 100},
    "C": {"stranding_risk": "moderate", "years_to_stranding": 2028, "capex_risk_eur_m2": 180},
    "D": {"stranding_risk": "moderate_high", "years_to_stranding": 2026, "capex_risk_eur_m2": 280},
    "E": {"stranding_risk": "high", "years_to_stranding": 2025, "capex_risk_eur_m2": 400},
    "F": {"stranding_risk": "very_high", "years_to_stranding": 2024, "capex_risk_eur_m2": 520},
    "G": {"stranding_risk": "critical", "years_to_stranding": 2023, "capex_risk_eur_m2": 650},
    "H": {"stranding_risk": "critical", "years_to_stranding": 2022, "capex_risk_eur_m2": 800},
}

# GHG emission intensity benchmarks (kg CO₂/m²/year)
# Source: UBA emissions data, CRREM v2 pathways
BENCHMARK_EMISSIONS: dict[str, dict[str, float]] = {
    "DE": {
        "office_current_avg": 32.5,
        "office_target_2030": 17.0,
        "residential_current_avg": 28.0,
        "residential_target_2030": 12.0,
        "retail_current_avg": 40.0,
        "logistics_current_avg": 25.0,
        "hotel_current_avg": 50.0,
    },
    "FR": {
        "office_current_avg": 28.0,
        "office_target_2030": 15.0,
        "residential_current_avg": 24.0,
        "residential_target_2030": 10.5,
    },
    "UK": {
        "office_current_avg": 27.0,
        "office_target_2030": 14.0,
        "residential_current_avg": 23.0,
        "residential_target_2030": 10.0,
    },
}

# EU Taxonomy DNSH (Do No Significant Harm) thresholds for buildings
# Source: EU Taxonomy Delegated Act (EU) 2021/2139 — Climate Change Mitigation
# DNSH for buildings: primary energy demand must not exceed thresholds
TAXONOMY_DNSH_THRESHOLDS: dict[str, float] = {
    "residential_primary_energy_max": 100,  # kWh/m²a — max for taxonomy alignment
    "office_primary_energy_max": 120,
    "retail_primary_energy_max": 130,
    "logistics_primary_energy_max": 90,
    "hotel_primary_energy_max": 120,
    "nearly_zero_energy_building_threshold": 50,
}


async def get_epc_benchmarks(
    country: str = "DE",
    building_type: str = "office",
) -> dict[str, Any]:
    """
    Get EPC industry benchmarks, distribution, CRREM stranding risk, and EU Taxonomy DNSH.

    Args:
        country: ISO country code (DE, FR, UK, IT)
        building_type: Building type (office, residential, retail, logistics, hotel)

    Returns:
        dict with benchmarks, distribution, stranding risk, DNSH alignment
    """
    cache = get_cache()
    cache_key = cache.make_key("epc_benchmarks", country, building_type)
    cached = cache.get(cache_key)
    if cached:
        return cached

    country_upper = country.upper()
    btype = building_type.lower()

    # Distribution data (default DE office if not found)
    dist_data = EPC_DISTRIBUTION.get(country_upper, EPC_DISTRIBUTION["DE"])
    distribution = dist_data.get(btype, dist_data["office"])

    # Map distribution to class labels
    class_labels = ["A+", "A", "B", "C", "D", "E", "F", "G", "H"]
    if len(distribution) == 8:
        class_labels = ["A", "B", "C", "D", "E", "F", "G", "H"]

    distribution_map = {}
    for i, cls in enumerate(class_labels):
        pct = distribution[i] if i < len(distribution) else 0
        distribution_map[cls] = pct
        distribution_map[f"{cls}_pct"] = pct

    # Weighted average EPC energy
    # Map each class to its midpoint energy value
    class_midpoints = {
        "A+": 20, "A": 40, "B": 62, "C": 87, "D": 115,
        "E": 145, "F": 180, "G": 225, "H": 280,
    }
    weighted_avg_energy = sum(
        class_midpoints.get(cls, 150) * (dist / 100)
        for cls, dist in zip(class_labels, distribution)
    )
    weighted_avg_energy = round(weighted_avg_energy, 0)

    # Emissions benchmarks
    emissions = BENCHMARK_EMISSIONS.get(country_upper, BENCHMARK_EMISSIONS["DE"])
    current_avg_key = f"{btype}_current_avg"
    target_2030_key = f"{btype}_target_2030"
    current_emissions = emissions.get(current_avg_key, 30.0)
    target_emissions_2030 = emissions.get(target_2030_key, 15.0)

    # CRREM stranding risk for the average building in this class
    # Determine the typical EPC class from the weighted average
    typical_epc = _classify_epc(weighted_avg_energy, EPC_THRESHOLDS_DE)
    stranding_info = CRREM_STRANDING_BY_EPC.get(typical_epc, CRREM_STRANDING_BY_EPC["D"])

    # EU Taxonomy DNSH alignment check
    dnsh_threshold = TAXONOMY_DNSH_THRESHOLDS.get(
        f"{btype}_primary_energy_max",
        TAXONOMY_DNSH_THRESHOLDS["office_primary_energy_max"],
    )
    dnsh_aligned = weighted_avg_energy <= dnsh_threshold
    nzeb_threshold = TAXONOMY_DNSH_THRESHOLDS["nearly_zero_energy_building_threshold"]
    nzeb_aligned = weighted_avg_energy <= nzeb_threshold

    result = {
        "country": country_upper,
        "building_type": btype,
        "industry_benchmarks_kwh_sqm_a": {
            "current_weighted_average": weighted_avg_energy,
            "typical_epc_class": typical_epc,
            "current_emissions_kgco2_m2": current_emissions,
            "target_emissions_2030_kgco2_m2": target_emissions_2030,
            "emissions_reduction_needed_pct": round(
                (1 - target_emissions_2030 / max(current_emissions, 1)) * 100, 1
            ),
            "unit": "kWh/m²a primary energy",
        },
        "epc_distribution_pct": distribution_map,
        "epc_class_labels": class_labels,
        "crrem_stranding_risk_assessment": {
            "typical_epc": typical_epc,
            "stranding_risk": stranding_info["stranding_risk"],
            "estimated_years_to_stranding": stranding_info["years_to_stranding"],
            "estimated_capex_risk_eur_m2": stranding_info["capex_risk_eur_m2"],
            "stranding_note": (
                f"Buildings at EPC class {typical_epc} face "
                f"{stranding_info['stranding_risk']} stranding risk by "
                f"~{stranding_info['years_to_stranding']} under CRREM 1.5°C pathway."
            ),
        },
        "eu_taxonomy_dnsh_alignment": {
            "do_no_significant_harm": dnsh_aligned,
            "dnsh_threshold_kwh_sqm_a": dnsh_threshold,
            "current_estimate_kwh_sqm_a": weighted_avg_energy,
            "gap_to_dnsh_kwh_sqm_a": round(max(0, weighted_avg_energy - dnsh_threshold), 0),
            "nearly_zero_energy_building": nzeb_aligned,
            "nzeb_threshold_kwh_sqm_a": nzeb_threshold,
            "taxonomy_regulation": "Climate Delegated Act (EU) 2021/2139, Annex I — CCM 6.2 "
                                   "(Renovation of existing buildings) and CCM 7.7 (Acquisition of buildings)",
            "note": (
                "EU Taxonomy DNSH requires that buildings do not exceed specified "
                "primary energy demand thresholds. For new buildings: NZEB standard. "
                "For renovations: ≥ 30 % reduction in primary energy vs pre-renovation."
            ),
        },
        "data_source": (
            "BPIE Buildings Performance Index 2023 / dena Gebäudereport 2024 / "
            "CRREM v2 / EU Taxonomy (EU) 2021/2139 / BMWSB benchmarking data"
        ),
    }
    cache.set(cache_key, result, category="real_estate", ttl_days=30)
    return result
