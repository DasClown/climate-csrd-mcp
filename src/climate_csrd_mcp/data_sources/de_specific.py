"""Germany-Specific Climate & ESG data source.

Provides:
- BISKO municipal GHG accounting (Bilanzierungs-Systematik Kommunal)
- KSG (Klimaschutzgesetz) compliance check
- LkSG (Lieferkettensorgfaltspflichtengesetz) supply chain due diligence

Sources: UBA 2024, KSG 2021 (amended 2023), LkSG 2021, BAFA LkSG 2024,
Destatis 2024, BMWK 2024, ifo Institut 2024, DEHSt 2024.
"""

import asyncio
from typing import Any
from ..cache import get_cache
from ..utils import risk_label, risk_color, get_esrs_ref, today_iso

# ═══════════════════════════════════════════════════════════════════════
# BISKO — Municipal GHG Accounting
# Bilanzierungs-Systematik Kommunal (IFEU 2019, UBA 2023)
# ═══════════════════════════════════════════════════════════════════════

# Emission factors by energy type (tCO2e/MWh, BISKO standard)
# Source: UBA Emissionsbilanz erneuerbarer Energieträger 2024; GEMIS 5.0

BISKO_EMISSION_FACTORS: dict[str, float] = {
    "natural_gas": 0.201,       # Erdgas H
    "natural_gas_l": 0.182,    # Erdgas L
    "heating_oil": 0.266,      # Heizöl EL
    "hard_coal": 0.335,        # Steinkohle
    "lignite": 0.370,          # Braunkohle
    "diesel": 0.266,           # Diesel (traffic)
    "gasoline": 0.252,         # Benzin (traffic)
    "kerosene": 0.260,         # Kerosin (aviation)
    "petrol_coke": 0.310,      # Petrolkoks
    "refinery_gas": 0.248,     # Raffineriegas
    "district_heating_mix": 0.150,  # Fernwärme-Mix DE
    "electricity_mix_de": 0.420,    # DE Strommix 2023 (vorläufig)
    "electricity_mix_de_2024": 0.380,  # DE Strommix 2024 (Schätzung)
    "biomass_solid": 0.022,    # Feste Biomasse (biogen)
    "biomass_gaseous": 0.093,  # Biogas (BHKW)
    "biomass_liquid": 0.025,   # Biogene Flüssigbrennstoffe
    "solar_thermal": 0.010,    # Solarthermie
    "heat_pump": 0.101,        # Wärmepumpe (Stromverbrauch)
    "hydrogen_green": 0.028,   # Grüner Wasserstoff (Elektrolyse)
    "hydrogen_grey": 0.310,    # Grauer Wasserstoff (Dampfreformierung)
    "waste_incineration": 0.150,  # Abfallverbrennung (energetisch)
}

# Sectoral breakdown default shares for BISKO
# Source: UBA THG-Bilanz 2023, Sektorenkopplung

BISKO_SECTOR_DEFAULTS: dict[str, dict[str, float]] = {
    "traffic": {
        "description": "Mobilität & Verkehr (Straße, Schiene, Wasser, Luft)",
        "sub_sectors": {"road": 0.82, "rail": 0.05, "water": 0.07, "aviation": 0.06},
        "typical_fuel": "diesel",
        "energy_share_of_total": 0.25,
    },
    "buildings": {
        "description": "Gebäude (Wohnen, Gewerbe, Handel, Dienstleistungen)",
        "sub_sectors": {"residential": 0.55, "commercial": 0.45},
        "typical_fuel": "natural_gas",
        "energy_share_of_total": 0.30,
    },
    "industry": {
        "description": "Industrie & verarbeitendes Gewerbe",
        "sub_sectors": {"manufacturing": 0.60, "chemical": 0.20, "metal": 0.15, "other": 0.05},
        "typical_fuel": "natural_gas",
        "energy_share_of_total": 0.30,
    },
    "waste": {
        "description": "Abfallwirtschaft & sonstige Quellen",
        "sub_sectors": {"incineration": 0.40, "landfill": 0.30, "wastewater": 0.20, "recycling": 0.10},
        "typical_fuel": "waste_incineration",
        "energy_share_of_total": 0.05,
    },
}

# ─── Territorial vs Consumption-based ratios (BISKO distinction)
TERRITORIAL_VS_CONSUMPTION: dict[str, float] = {
    "territorial_share": 0.65,  # 65% of municipal emissions are territorial
    "consumption_share": 0.35,  # 35% attributed via consumption footprint
    "note": "BISKO primarily uses territorial (endenergie-basiert) accounting. "
            "Consumption-based allocation follows BISKO 2.0 methodology.",
}


# ═══════════════════════════════════════════════════════════════════════
# KSG — Klimaschutzgesetz (Bundes-Klimaschutzgesetz, KSG 2021/2023)
# ═══════════════════════════════════════════════════════════════════════

# KSG sector annual emission budgets in MtCO2e
# Source: KSG §4 Anlage 2 (Fassung 2023), UBA THG-Berichterstattung 2024
# Budgets are cumulative annual allowances for each sector

KSG_SECTOR_BUDGETS: dict[str, list[dict[str, Any]]] = {
    "energy": [
        {"year": 2020, "budget_mt": 280, "actual_mt": 245, "status": "compliant"},
        {"year": 2021, "budget_mt": 257, "actual_mt": 240, "status": "compliant"},
        {"year": 2022, "budget_mt": 247, "actual_mt": 235, "status": "compliant"},
        {"year": 2023, "budget_mt": 237, "actual_mt": 220, "status": "compliant"},
        {"year": 2024, "budget_mt": 227, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 217, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 175, "actual_mt": None, "status": "target"},
    ],
    "industry": [
        {"year": 2020, "budget_mt": 186, "actual_mt": 178, "status": "compliant"},
        {"year": 2021, "budget_mt": 182, "actual_mt": 175, "status": "compliant"},
        {"year": 2022, "budget_mt": 177, "actual_mt": 170, "status": "compliant"},
        {"year": 2023, "budget_mt": 172, "actual_mt": 162, "status": "compliant"},
        {"year": 2024, "budget_mt": 167, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 157, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 118, "actual_mt": None, "status": "target"},
    ],
    "buildings": [
        {"year": 2020, "budget_mt": 118, "actual_mt": 123, "status": "breached"},
        {"year": 2021, "budget_mt": 113, "actual_mt": 115, "status": "breached"},
        {"year": 2022, "budget_mt": 108, "actual_mt": 110, "status": "breached"},
        {"year": 2023, "budget_mt": 102, "actual_mt": 105, "status": "breached"},
        {"year": 2024, "budget_mt": 97, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 92, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 67, "actual_mt": None, "status": "target"},
    ],
    "transport": [
        {"year": 2020, "budget_mt": 150, "actual_mt": 146, "status": "compliant"},
        {"year": 2021, "budget_mt": 148, "actual_mt": 147, "status": "compliant"},
        {"year": 2022, "budget_mt": 145, "actual_mt": 148, "status": "breached"},
        {"year": 2023, "budget_mt": 140, "actual_mt": 143, "status": "breached"},
        {"year": 2024, "budget_mt": 133, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 126, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 85, "actual_mt": None, "status": "target"},
    ],
    "agriculture": [
        {"year": 2020, "budget_mt": 70, "actual_mt": 68, "status": "compliant"},
        {"year": 2021, "budget_mt": 68, "actual_mt": 66, "status": "compliant"},
        {"year": 2022, "budget_mt": 66, "actual_mt": 65, "status": "compliant"},
        {"year": 2023, "budget_mt": 64, "actual_mt": 62, "status": "compliant"},
        {"year": 2024, "budget_mt": 62, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 60, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 53, "actual_mt": None, "status": "target"},
    ],
    "waste": [
        {"year": 2020, "budget_mt": 9, "actual_mt": 8, "status": "compliant"},
        {"year": 2021, "budget_mt": 9, "actual_mt": 8, "status": "compliant"},
        {"year": 2022, "budget_mt": 8, "actual_mt": 8, "status": "compliant"},
        {"year": 2023, "budget_mt": 8, "actual_mt": 7, "status": "compliant"},
        {"year": 2024, "budget_mt": 8, "actual_mt": None, "status": "pending"},
        {"year": 2025, "budget_mt": 7, "actual_mt": None, "status": "projected"},
        {"year": 2030, "budget_mt": 5, "actual_mt": None, "status": "target"},
    ],
}

# KSG national targets
# Source: KSG §3 (Bundes-Klimaschutzgesetz i.d.F. v. 18.08.2023)

KSG_TARGETS: list[dict[str, Any]] = [
    {"year": 2020, "target_pct_reduction_vs_1990": 40.0, "actual_pct": 41.3, "status": "achieved"},
    {"year": 2030, "target_pct_reduction_vs_1990": 65.0, "actual_pct": None, "status": "in_progress"},
    {"year": 2040, "target_pct_reduction_vs_1990": 88.0, "actual_pct": None, "status": "target"},
    {"year": 2045, "target_pct_reduction_vs_1990": 100.0, "actual_pct": None, "status": "net_zero_target"},
]

# KSG cross-sectoral compliance mechanism triggers
# Source: KSG §4-5, Expertenrat für Klimafragen 2024

KSG_COMPLIANCE_TRIGGERS: dict[str, Any] = {
    "mechanism": "Cross-sectoral compliance mechanism (KSG §4 Abs. 4-5)",
    "trigger": "If any sector exceeds annual budget for two consecutive years, "
               "the BMWK must submit an immediate action program within 3 months",
    "immediate_action_program": {
        "trigger_condition": "Two consecutive annual budget breaches in any sector",
        "response_time": "3 months",
        "responsible_body": "Bundesministerium für Wirtschaft und Klimaschutz (BMWK)",
        "measures": ["Additional emission reduction measures for the breaching sector",
                     "Cross-sectoral compensatory measures",
                     "Review and strengthening of existing measures",
                     "Parliamentary approval required"],
        "expert_review": "Expertenrat für Klimafragen reviews program within 2 months",
    },
    "historic_triggers": [
        {"year": 2022, "sectors": ["buildings", "transport"],
         "triggered": True, "action_program_submitted": True,
         "outcome": "Immediate action program submitted Dec 2022, supplemented 2023"},
        {"year": 2023, "sectors": ["buildings", "transport"],
         "triggered": True, "action_program_submitted": True,
         "outcome": "Strengthened measures adopted in Klimaschutzprogramm 2023"},
    ],
}


# ═══════════════════════════════════════════════════════════════════════
# LkSG — Lieferkettensorgfaltspflichtengesetz (Supply Chain Due Diligence)
# Source: LkSG v. 16.07.2021 (BGBl. I S. 2959), BAFA 2024
# ═══════════════════════════════════════════════════════════════════════

LKSG_THRESHOLDS: list[dict[str, Any]] = [
    {"effective_from": "2023-01-01", "min_employees": 3000, "label": "Phase 1: >3,000 employees"},
    {"effective_from": "2024-01-01", "min_employees": 1000, "label": "Phase 2: >1,000 employees"},
]

LKSG_SECTOR_RISK: dict[str, dict[str, Any]] = {
    "textiles": {"hr_risk": "very_high", "env_risk": "high",
                 "typical_issues": ["child labour", "forced labour", "unsafe conditions",
                                    "wage violations", "chemical pollution"]},
    "agriculture": {"hr_risk": "very_high", "env_risk": "very_high",
                    "typical_issues": ["child labour", "debt bondage", "land rights",
                                       "deforestation", "pesticide exposure"]},
    "construction": {"hr_risk": "high", "env_risk": "medium",
                     "typical_issues": ["migrant forced labour", "OHS violations",
                                        "wage theft", "waste management"]},
    "manufacturing": {"hr_risk": "medium", "env_risk": "medium",
                      "typical_issues": ["OHS compliance", "wage compliance",
                                         "supply chain transparency"]},
    "technology": {"hr_risk": "medium", "env_risk": "medium",
                   "typical_issues": ["conflict minerals", "e-waste", "data privacy"]},
    "automotive": {"hr_risk": "medium", "env_risk": "high",
                   "typical_issues": ["battery supply chain", "cobalt mining",
                                      "manufacturing OHS"]},
    "chemicals": {"hr_risk": "medium", "env_risk": "high",
                  "typical_issues": ["hazardous substance exposure", "waste disposal",
                                     "community health impacts"]},
}

LKSG_DEFAULT_RISK = {"hr_risk": "medium", "env_risk": "medium",
                     "typical_issues": ["supply chain transparency",
                                        "OHS compliance", "wage compliance"]}

LKSG_FINES: dict[str, Any] = {
    "max_fine_eur": 8000000,
    "max_fine_revenue_pct": 2.0,
    "max_fine_label": "Up to €8M or up to 2% of annual group revenue (whichever is higher)",
    "exclusions_from_public_tenders": True,
    "exclusion_period_months": 36,
    "enforcement_authority": "BAFA — Bundesamt für Wirtschaft und Ausfuhrkontrolle",
    "aggravating_factors": ["Willful violation", "Systemic failure", "No remedial action",
                            "Obstruction of BAFA investigation", "Repeat violation"],
    "mitigating_factors": ["Voluntary implementation of DD obligations",
                           "Effective remedial measures", "Cooperation with BAFA",
                           "First-time violation"],
}

LKSG_DD_OBLIGATIONS: list[dict[str, Any]] = [
    {"obligation": 1, "title": "Risk management system",
     "description": "Establish adequate and effective risk management to identify, prevent, "
                    "and mitigate human rights and environmental risks in supply chain",
     "mo": 3},
    {"obligation": 2, "title": "Risk analysis",
     "description": "Annual risk analysis of own operations, direct suppliers, and "
                    "indirect suppliers with substantiated knowledge",
     "mo": 6},
    {"obligation": 3, "title": "Prevention measures",
     "description": "Implement appropriate prevention measures for identified risks: "
                    "supplier code of conduct, contractual cascading, training, audits",
     "mo": 12},
    {"obligation": 4, "title": "Remedial measures",
     "description": "Take remedial action when legal interest or human rights violation "
                    "has occurred; provide or cooperate in remediation",
     "mo": 12},
    {"obligation": 5, "title": "Grievance mechanism",
     "description": "Establish internal complaint procedure enabling affected parties "
                    "to report human rights and environmental risks",
     "mo": 6},
    {"obligation": 6, "title": "Documentation and reporting",
     "description": "Document compliance continuously; submit annual report to BAFA; "
                    "publish on company website for at least 7 years",
     "mo": 12},
]


def _sector_budget(sector: str, year: int) -> dict | None:
    budgets = KSG_SECTOR_BUDGETS.get(sector)
    if not budgets:
        return None
    for b in budgets:
        if b["year"] == year:
            return b
    return None


def _lksg_applicable(employees: int) -> bool:
    return employees >= 1000


def _lksg_risk(sector: str) -> dict:
    return LKSG_SECTOR_RISK.get(sector.lower(), LKSG_DEFAULT_RISK)


# ─── Public API ────────────────────────────────────────────────────────


async def get_bisko_assessment(
    sector: str = "buildings",
    energy_consumption_mwh: float = 10000.0,
    fuel_type: str = "natural_gas",
) -> dict[str, Any]:
    """BISKO municipal GHG accounting assessment.
    
    Calculates GHG emissions using BISKO emission factors,
    provides territorial vs consumption-based breakdown,
    and sectoral allocation.
    """
    cache = get_cache()
    ck = cache.make_key("bisko", sector, str(energy_consumption_mwh), fuel_type)
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    ef = BISKO_EMISSION_FACTORS.get(fuel_type, 0.25)
    total_emissions = round(energy_consumption_mwh * ef, 2)

    # Sector breakdown
    sector_info = BISKO_SECTOR_DEFAULTS.get(s, {
        "description": f"{s.capitalize()} sector",
        "sub_sectors": {"primary": 1.0},
        "typical_fuel": "natural_gas",
        "energy_share_of_total": 0.10,
    })

    sub_sector_emissions = {}
    for sub_name, sub_share in sector_info["sub_sectors"].items():
        sub_ef = BISKO_EMISSION_FACTORS.get(sector_info.get("typical_fuel", "natural_gas"), 0.25)
        sub_emissions = round(energy_consumption_mwh * sub_share * ef, 2)
        sub_sector_emissions[sub_name] = {
            "share_pct": round(sub_share * 100, 1),
            "emissions_tco2e": sub_emissions,
        }

    territorial = round(total_emissions * TERRITORIAL_VS_CONSUMPTION["territorial_share"], 2)
    consumption = round(total_emissions * TERRITORIAL_VS_CONSUMPTION["consumption_share"], 2)

    results = {
        "methodology": "BISKO (Bilanzierungs-Systematik Kommunal), IFEU 2019 / UBA 2023",
        "input": {
            "sector": s,
            "energy_consumption_mwh": energy_consumption_mwh,
            "fuel_type": fuel_type,
            "emission_factor_tco2e_per_mwh": ef,
            "emission_factor_source": "UBA GEMIS 5.0 / UBA Emissionsbilanz 2024",
        },
        "total_emissions_tco2e": total_emissions,
        "sector_breakdown": {
            "name": s,
            "description": sector_info["description"],
            "sub_sectors": sub_sector_emissions,
            "share_of_total_municipal": sector_info["energy_share_of_total"],
        },
        "accounting_methods": {
            "territorial_based": {
                "tco2e": territorial,
                "share_pct": TERRITORIAL_VS_CONSUMPTION["territorial_share"] * 100,
                "description": "Emissions occurring within municipal boundaries "
                                "(endenergie-basiert nach BISKO-Standard)",
            },
            "consumption_based": {
                "tco2e": consumption,
                "share_pct": TERRITORIAL_VS_CONSUMPTION["consumption_share"] * 100,
                "description": "Emissions attributed via consumption footprint "
                                "(BISKO 2.0 Erweiterung für Graue Emissionen)",
            },
        },
        "comparison_to_benchmark": {
            "emissions_per_capita_tco2e": round(total_emissions / 1000, 2) if total_emissions > 0 else 0,
            "de_average_per_capita_2023": 7.3,
            "de_target_2030_per_capita": 4.5,
        },
        "recommendations": [
            f"Current emission factor {ef} tCO2e/MWh for {fuel_type}",
            "Consider switching to lower-carbon fuel alternatives",
            "Expand renewable energy share in energy mix",
            "Conduct BISKO-compliant full municipal inventory for completeness",
        ],
        "sources": "BISKO (IFEU 2019); UBA Emissionsbilanz 2024; GEMIS 5.0; "
                   "Umweltbundesamt THG-Bilanz 2023",
        "disclaimer": f"Generated {today_iso()}. BISKO assessment indicative — "
                      "consult UBA methodology guidance for full inventory.",
    }
    cache.set(ck, results, "climate", 14)
    return results


async def get_ksg_compliance(
    target_year: int = 2030,
    sector: str = "energy",
    emission_reduction_pct: float = 0.0,
) -> dict[str, Any]:
    """KSG (Klimaschutzgesetz) compliance check.
    
    Assesses compliance with annual sector emission budgets,
    cross-sectoral compliance mechanism triggers, and
    immediate action program requirements.
    """
    cache = get_cache()
    ck = cache.make_key("ksg", str(target_year), sector, str(emission_reduction_pct))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    budget = _sector_budget(s, target_year)
    if budget is None:
        budget = {"year": target_year, "budget_mt": 0, "actual_mt": None, "status": "no_data"}

    actual_mt = None
    if budget["actual_mt"] is not None:
        actual_mt = budget["actual_mt"]
    elif emission_reduction_pct > 0:
        # Back-calculate from baseline approximation
        baseline = budget["budget_mt"] * 2 if budget["budget_mt"] > 0 else 250
        actual_mt = round(baseline * (1 - emission_reduction_pct / 100), 1)

    # Calculate compliance
    budget_remaining = None
    compliance_pct = None
    status = budget["status"]

    if actual_mt is not None and budget["budget_mt"] > 0:
        compliance_pct = round((1 - actual_mt / budget["budget_mt"]) * 100, 1)
        budget_remaining = round(budget["budget_mt"] - actual_mt, 1)
        if actual_mt <= budget["budget_mt"]:
            status = "compliant"
        else:
            status = "breached"

    # Check if this triggers immediate action program
    breach_history = []
    for past_year in [target_year - 1, target_year - 2]:
        pb = _sector_budget(s, past_year)
        if pb and pb["status"] == "breached":
            breach_history.append(past_year)

    # National target check
    national_target = None
    for t in KSG_TARGETS:
        if t["year"] == target_year:
            national_target = t
            break

    sector_budgets_all = KSG_SECTOR_BUDGETS.get(s, [])
    target_compliance = None
    if national_target and emission_reduction_pct > 0:
        required = national_target["target_pct_reduction_vs_1990"]
        target_compliance = "on_track" if emission_reduction_pct >= required else "off_track"
    elif national_target:
        target_compliance = "insufficient_data"

    results = {
        "legislation": "Bundes-Klimaschutzgesetz (KSG) 2021, amended 2023",
        "assessment": {
            "sector": s, "target_year": target_year,
            "sector_budget_mt_co2e": budget["budget_mt"],
            "actual_or_projected_mt": actual_mt,
            "budget_remaining_mt": budget_remaining,
            "compliance_pct": compliance_pct,
            "status": status,
            "status_color": "🟢" if status == "compliant" else "🔴" if status == "breached" else "🟡",
        },
        "national_target": {
            "year": target_year,
            "required_reduction_pct": national_target["target_pct_reduction_vs_1990"] if national_target else None,
            "current_projected_reduction_pct": emission_reduction_pct if emission_reduction_pct > 0 else None,
            "target_compliance": target_compliance,
            "overall_status": national_target["status"] if national_target else "unknown",
            ("Baseline: 1990 = ~1250 MtCO2e" if national_target else "N/A"): "",
        },
        "cross_sectoral_compliance": {
            "mechanism": KSG_COMPLIANCE_TRIGGERS["mechanism"],
            "triggered": status == "breached" and len(breach_history) >= 2,
            "consecutive_breach_years": breach_history + ([target_year] if status == "breached" else []),
            "immediate_action_program_required": status == "breached" and len(breach_history) >= 2,
            "program_details": KSG_COMPLIANCE_TRIGGERS["immediate_action_program"] if (
                status == "breached" and len(breach_history) >= 2) else None,
        },
        "sector_timeline": [
            {"year": b["year"], "budget_mt": b["budget_mt"],
             "actual_mt": b["actual_mt"], "status": b["status"]}
            for b in sector_budgets_all
            if b["year"] <= target_year
        ],
        "all_sector_budgets_for_year": {
            sec: _sector_budget(sec, target_year)
            for sec in KSG_SECTOR_BUDGETS.keys()
        },
        "recommendations": [
            *(["⚠️ BREACH: Sector exceeds annual budget — immediate action program required"]
              if status == "breached" else []),
            *([f"CRITICAL: {len(breach_history)+1} consecutive breaches — "
               f"mandatory immediate action program per KSG §4 Abs.4"]
              if status == "breached" and len(breach_history) >= 2 else []),
            *([f"Sector on track — maintain reduction trajectory of {compliance_pct}% over budget"]
              if status == "compliant" else []),
            "Align corporate climate targets with KSG sector budgets",
            "Integrate KSG compliance into CSRD / ESRS E1-1 transition plan",
            f"Target year {target_year}: KSG mandates "
            f"{KSG_TARGETS[1]['target_pct_reduction_vs_1990']}% reduction by 2030, "
            f"net zero by 2045",
        ],
        "sources": "KSG 2021 (BGBl. I S. 3905), amended 2023 (BGBl. I Nr. 204); "
                   "UBA THG-Berichterstattung 2024; Expertenrat für Klimafragen 2024",
        "disclaimer": f"Generated {today_iso()}. Indicative — consult BAFA and UBA official data.",
    }
    cache.set(ck, results, "climate", 14)
    return results


async def get_lksg_supply_chain_duty(
    sector: str,
    regions: list[str],
    revenue_eur_m: float = 100.0,
    employees: int = 500,
) -> dict[str, Any]:
    """LkSG (Lieferkettensorgfaltspflichtengesetz) assessment.
    
    Evaluates applicability, risk analysis, prevention and remedial
    measures, documentation obligations, and BAFA enforcement.
    Covers both human rights and environmental due diligence.
    """
    cache = get_cache()
    ck = cache.make_key("lksg", sector, *sorted(regions or ["?"]),
                        str(revenue_eur_m), str(employees))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    applicable = _lksg_applicable(employees)
    risk = _lksg_risk(s)

    # Determine applicable phase
    phase = None
    for t in reversed(LKSG_THRESHOLDS):
        if employees >= t["min_employees"]:
            phase = t
            break

    # Risk analysis by region
    from .csddd import REGION_HR as csddd_region_hr

    region_risks = []
    for r in regions:
        rh = csddd_region_hr.get(r, {"o": 3, "c": 3, "f": 3, "fr": 3, "d": 3})
        hr_score = max(rh.get("o", 3), rh.get("c", 3), rh.get("f", 3))
        region_risks.append({
            "region": r, "hr_risk_score": hr_score,
            "hr_label": risk_label(hr_score), "hr_color": risk_color(hr_score),
            "key_concerns": [
                "child labour" if rh.get("c", 3) >= 4 else None,
                "forced labour" if rh.get("f", 3) >= 4 else None,
                "discrimination" if rh.get("d", 3) >= 4 else None,
            ],
        })

    # Max fine calculation
    max_fine = min(8000000, round(revenue_eur_m * 1000000 * 0.02))
    turnover_based_fine = round(revenue_eur_m * 1000000 * 0.02)
    effective_max = min(8000000, turnover_based_fine)

    results = {
        "legislation": "Lieferkettensorgfaltspflichtengesetz (LkSG), 16.07.2021",
        "company": {
            "sector": s, "employees": employees,
            "revenue_eur_m": revenue_eur_m,
            "regions": regions,
            "applicable": applicable,
        },
        "applicability": {
            "in_scope": applicable,
            "phase": phase["label"] if phase else "Not applicable",
            "threshold_note": "LkSG applies to companies with >1,000 employees in Germany "
                              "(from 2024; >3,000 from 2023)",
            "note": "Companies below threshold may still be indirectly affected as "
                    "suppliers to in-scope companies",
        },
        "risk_analysis": {
            "sector_risk": {
                "human_rights": risk["hr_risk"],
                "environmental": risk["env_risk"],
                "typical_issues": risk["typical_issues"],
            },
            "regional_risk": region_risks,
        },
        "due_diligence_obligations": [
            {
                "id": f"O{o['obligation']:02d}",
                "obligation": o["title"],
                "description": o["description"],
                "timeline_months": o["mo"],
                "status": "required" if applicable else "recommended",
            }
            for o in LKSG_DD_OBLIGATIONS
        ],
        "documentation_and_reporting": {
            "requirement": "Continuous documentation of DD compliance; annual report to BAFA",
            "content": ["Description of risk management system",
                        "Results of risk analysis",
                        "Prevention and remedial measures implemented",
                        "Effectiveness assessment",
                        "Grievance mechanism description"],
            "publication": "Report must be published on company website for min. 7 years",
            "deadline": "Report due annually within 4 months of fiscal year end",
            "language": "German",
        },
        "enforcement_and_penalties": {
            "authority": LKSG_FINES["enforcement_authority"],
            "powers": ["Request information and documents", "Conduct on-site inspections",
                       "Order remedial measures", "Impose fines"],
            "penalties": {
                "max_fine_eur": max_fine,
                "max_fine_revenue_based_eur": turnover_based_fine,
                "effective_max_eur": effective_max,
                "max_fine_label": LKSG_FINES["max_fine_label"],
                "exclusion_from_public_tenders": LKSG_FINES["exclusions_from_public_tenders"],
                "exclusion_period_months": LKSG_FINES["exclusion_period_months"],
            },
            "aggravating_factors": LKSG_FINES["aggravating_factors"],
            "mitigating_factors": LKSG_FINES["mitigating_factors"],
        },
        "recommendations": [
            *(["IMMEDIATE: LkSG applies — establish DD management system within 3 months"]
              if applicable else []),
            *([f"Prepare compliance: current {employees} employees approaching "
               f"1,000 threshold — implement DD framework proactively"]
              if 500 <= employees < 1000 else []),
            "Conduct annual risk analysis of direct and indirect suppliers",
            "Establish grievance mechanism accessible to all supply chain workers",
            "Implement supplier code of conduct with contractual cascading",
            "Align LkSG documentation with CSRD/ESRS S1-S3 and CSDDD reporting",
            "Prepare BAFA report content: risk analysis, measures, effectiveness",
        ],
        "sources": "LkSG (BGBl. I 2021, S. 2959); BAFA FAQ LkSG 2024; "
                   "BMAS LkSG Praxishilfe 2024; ILO 2024; HRW 2025",
        "disclaimer": f"Generated {today_iso()}. Indicative — consult BAFA guidance and "
                      "legal counsel for definitive compliance obligations.",
    }
    cache.set(ck, results, "csrd", 14)
    return results
