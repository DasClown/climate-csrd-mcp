"""
TNFD Biodiversity & Nature Risk Module — Taskforce on Nature-related Financial
Disclosures aligned assessment.

Provides the full LEAP (Locate, Evaluate, Assess, Prepare) approach for
nature-related dependencies, impacts, risks, and opportunities, with
cross-reference to ESRS E4 (Biodiversity and Ecosystems) under CSRD.

Functions:
  get_tnfd_report()         — Full TNFD-aligned LEAP assessment
  get_tnfd_leap_locate()    — Locate: interface with nature
  get_tnfd_leap_evaluate()  — Evaluate: dependencies & impacts
  get_tnfd_leap_assess()    — Assess: material risks & opportunities
  get_tnfd_leap_prepare()   — Prepare: disclosures & targets
  get_nature_risk_score()   — Composite nature risk scoring (1-5)

Key references:
  - TNFD v1.0 (September 2023) LEAP framework
  - IUCN Global Ecosystem Typology (v2.0)
  - ENCORE (Exploring Natural Capital Opportunities, Risks and Exposure)
  - ESRS E4 (EU 2023/2772, Delegated Regulation Dec 2023)
    E4-1: Transition plan for biodiversity
    E4-2: Policies related to biodiversity
    E4-3: Actions and resources
    E4-4: Targets related to biodiversity
  - SBTN (Science Based Targets for Nature) guidance
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. IUCN GLOBAL ECOSYSTEM TYPOLOGY (v2.0) — BIOME & FUNCTIONAL GROUP MAP
# ═══════════════════════════════════════════════════════════════════════════════
# Source: Keith et al. (2020) / IUCN CEM — 25 functional groups across 5 realms
# Used in TNFD LEAP "Locate" phase to classify interface with nature.

IUCN_BIOMES: dict[str, dict[str, Any]] = {
    "T1": {
        "name": "Terrestrial — Tropical/subtropical forests",
        "realm": "Terrestrial",
        "functional_group": "Forests",
        "priority": True,
        "esrs_e4_relevance": "E4-2, E4-4 (forest conservation targets)",
        "encore_asset": "Forest ecosystem integrity",
    },
    "T2": {
        "name": "Terrestrial — Savannas & grasslands",
        "realm": "Terrestrial",
        "functional_group": "Grasslands",
        "priority": False,
        "esrs_e4_relevance": "E4-2 (land use policy)",
        "encore_asset": "Habitat maintenance",
    },
    "T3": {
        "name": "Terrestrial — Shrublands & shrubby woodlands",
        "realm": "Terrestrial",
        "functional_group": "Shrublands",
        "priority": False,
        "esrs_e4_relevance": "E4-2",
        "encore_asset": "Habitat maintenance",
    },
    "T4": {
        "name": "Terrestrial — Deserts & xeric formations",
        "realm": "Terrestrial",
        "functional_group": "Arid lands",
        "priority": False,
        "esrs_e4_relevance": "E4-2",
        "encore_asset": "Soil quality regulation",
    },
    "T5": {
        "name": "Terrestrial — Polar/alpine (cryosphere)",
        "realm": "Terrestrial",
        "functional_group": "Cryogenic",
        "priority": True,
        "esrs_e4_relevance": "E4-4 (climate-biodiversity nexus)",
        "encore_asset": "Climate regulation",
    },
    "T6": {
        "name": "Terrestrial — Intensive land-use systems",
        "realm": "Terrestrial",
        "functional_group": "Modified habitats",
        "priority": True,
        "esrs_e4_relevance": "E4-2, E4-3 (restoration actions)",
        "encore_asset": "Land cover",
    },
    "M1": {
        "name": "Marine — Marine shelves",
        "realm": "Marine",
        "functional_group": "Shelf ecosystems",
        "priority": True,
        "esrs_e4_relevance": "E4-2 (marine policy)",
        "encore_asset": "Marine ecosystem services",
    },
    "M2": {
        "name": "Marine — Deep-sea & open ocean",
        "realm": "Marine",
        "functional_group": "Pelagic",
        "priority": False,
        "esrs_e4_relevance": "E4-2",
        "encore_asset": "Fisheries provisioning",
    },
    "M3": {
        "name": "Marine — Coastal systems (mangroves, reefs)",
        "realm": "Marine",
        "functional_group": "Coastal",
        "priority": True,
        "esrs_e4_relevance": "E4-4 (coastal habitat targets)",
        "encore_asset": "Coastal protection & nursery",
    },
    "F1": {
        "name": "Freshwater — Rivers & streams",
        "realm": "Freshwater",
        "functional_group": "Lotic",
        "priority": True,
        "esrs_e4_relevance": "E4-2, E4-4 (water targets)",
        "encore_asset": "Water flow regulation",
    },
    "F2": {
        "name": "Freshwater — Lakes & reservoirs",
        "realm": "Freshwater",
        "functional_group": "Lentic",
        "priority": True,
        "esrs_e4_relevance": "E4-2, E4-4",
        "encore_asset": "Water supply",
    },
    "F3": {
        "name": "Freshwater — Wetlands (palustrine)",
        "realm": "Freshwater",
        "functional_group": "Palustrine",
        "priority": True,
        "esrs_e4_relevance": "E4-3 (wetland restoration)",
        "encore_asset": "Flood regulation & filtration",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. ENCORE-BASED SECTOR-NATURE DEPENDENCY & IMPACT DATA
# ═══════════════════════════════════════════════════════════════════════════════
# Source: ENCORE (encore.naturalcapital.finance) — Exploring Natural Capital
# Opportunities, Risks and Exposure. Dependency scores 0-5 (very low to very
# high). Production processes mapped to ecosystem services.

# Core ecosystem services with dependency scoring per sector
# Scale: 0 = none, 1 = very low, 2 = low, 3 = medium, 4 = high, 5 = very high
SECTOR_NATURE_DEPENDENCIES: dict[str, dict[str, dict[str, float]]] = {
    "agriculture": {
        "pollination": {"dependency": 5.0, "impact": 4.0, "trend": "increasing"},
        "water_supply": {"dependency": 5.0, "impact": 4.5, "trend": "increasing"},
        "water_flow_regulation": {"dependency": 4.0, "impact": 3.5, "trend": "stable"},
        "soil_quality": {"dependency": 5.0, "impact": 5.0, "trend": "degrading"},
        "pest_control": {"dependency": 4.0, "impact": 3.5, "trend": "increasing"},
        "climate_regulation": {"dependency": 3.0, "impact": 4.0, "trend": "increasing"},
        "genetic_materials": {"dependency": 4.0, "impact": 2.0, "trend": "stable"},
        "biomass_provision": {"dependency": 5.0, "impact": 4.0, "trend": "increasing"},
        "habitat_maintenance": {"dependency": 3.0, "impact": 5.0, "trend": "degrading"},
        "flood_protection": {"dependency": 2.0, "impact": 2.0, "trend": "stable"},
    },
    "manufacturing": {
        "pollination": {"dependency": 0.0, "impact": 0.5, "trend": "stable"},
        "water_supply": {"dependency": 4.5, "impact": 4.0, "trend": "stable"},
        "water_flow_regulation": {"dependency": 3.0, "impact": 2.5, "trend": "stable"},
        "soil_quality": {"dependency": 1.0, "impact": 2.0, "trend": "stable"},
        "pest_control": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "climate_regulation": {"dependency": 2.0, "impact": 4.0, "trend": "increasing"},
        "genetic_materials": {"dependency": 1.0, "impact": 1.0, "trend": "stable"},
        "biomass_provision": {"dependency": 3.0, "impact": 3.0, "trend": "stable"},
        "habitat_maintenance": {"dependency": 1.0, "impact": 3.0, "trend": "degrading"},
        "flood_protection": {"dependency": 2.0, "impact": 1.0, "trend": "stable"},
    },
    "mining": {
        "pollination": {"dependency": 0.0, "impact": 2.0, "trend": "stable"},
        "water_supply": {"dependency": 5.0, "impact": 5.0, "trend": "increasing"},
        "water_flow_regulation": {"dependency": 4.0, "impact": 4.5, "trend": "increasing"},
        "soil_quality": {"dependency": 2.0, "impact": 5.0, "trend": "degrading"},
        "pest_control": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "climate_regulation": {"dependency": 1.0, "impact": 4.0, "trend": "increasing"},
        "genetic_materials": {"dependency": 0.0, "impact": 2.0, "trend": "stable"},
        "biomass_provision": {"dependency": 0.0, "impact": 4.0, "trend": "degrading"},
        "habitat_maintenance": {"dependency": 1.0, "impact": 5.0, "trend": "degrading"},
        "flood_protection": {"dependency": 1.0, "impact": 3.0, "trend": "stable"},
    },
    "energy": {
        "pollination": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "water_supply": {"dependency": 4.0, "impact": 3.5, "trend": "stable"},
        "water_flow_regulation": {"dependency": 3.0, "impact": 3.0, "trend": "stable"},
        "soil_quality": {"dependency": 1.0, "impact": 3.0, "trend": "stable"},
        "pest_control": {"dependency": 0.0, "impact": 0.5, "trend": "stable"},
        "climate_regulation": {"dependency": 2.0, "impact": 5.0, "trend": "critical"},
        "genetic_materials": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "biomass_provision": {"dependency": 3.0, "impact": 3.0, "trend": "stable"},
        "habitat_maintenance": {"dependency": 1.0, "impact": 4.0, "trend": "degrading"},
        "flood_protection": {"dependency": 1.0, "impact": 2.0, "trend": "stable"},
    },
    "construction": {
        "pollination": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "water_supply": {"dependency": 3.0, "impact": 2.5, "trend": "stable"},
        "water_flow_regulation": {"dependency": 2.0, "impact": 3.0, "trend": "stable"},
        "soil_quality": {"dependency": 2.0, "impact": 4.0, "trend": "degrading"},
        "pest_control": {"dependency": 0.0, "impact": 0.5, "trend": "stable"},
        "climate_regulation": {"dependency": 1.0, "impact": 3.0, "trend": "increasing"},
        "genetic_materials": {"dependency": 0.0, "impact": 0.5, "trend": "stable"},
        "biomass_provision": {"dependency": 4.0, "impact": 3.0, "trend": "increasing"},
        "habitat_maintenance": {"dependency": 1.0, "impact": 4.5, "trend": "degrading"},
        "flood_protection": {"dependency": 1.0, "impact": 2.0, "trend": "stable"},
    },
    "real_estate": {
        "pollination": {"dependency": 0.0, "impact": 1.0, "trend": "stable"},
        "water_supply": {"dependency": 4.0, "impact": 3.0, "trend": "stable"},
        "water_flow_regulation": {"dependency": 3.0, "impact": 3.5, "trend": "increasing"},
        "soil_quality": {"dependency": 1.0, "impact": 3.0, "trend": "stable"},
        "pest_control": {"dependency": 0.0, "impact": 0.5, "trend": "stable"},
        "climate_regulation": {"dependency": 2.0, "impact": 3.0, "trend": "increasing"},
        "genetic_materials": {"dependency": 0.0, "impact": 0.0, "trend": "stable"},
        "biomass_provision": {"dependency": 1.0, "impact": 1.0, "trend": "stable"},
        "habitat_maintenance": {"dependency": 1.0, "impact": 4.0, "trend": "degrading"},
        "flood_protection": {"dependency": 3.0, "impact": 1.0, "trend": "stable"},
    },
    "forestry": {
        "pollination": {"dependency": 3.0, "impact": 2.0, "trend": "stable"},
        "water_supply": {"dependency": 4.0, "impact": 3.0, "trend": "stable"},
        "water_flow_regulation": {"dependency": 3.0, "impact": 3.0, "trend": "stable"},
        "soil_quality": {"dependency": 4.0, "impact": 3.5, "trend": "stable"},
        "pest_control": {"dependency": 3.0, "impact": 2.0, "trend": "increasing"},
        "climate_regulation": {"dependency": 4.0, "impact": 4.0, "trend": "stable"},
        "genetic_materials": {"dependency": 4.0, "impact": 2.0, "trend": "stable"},
        "biomass_provision": {"dependency": 5.0, "impact": 4.0, "trend": "increasing"},
        "habitat_maintenance": {"dependency": 4.0, "impact": 4.0, "trend": "stable"},
        "flood_protection": {"dependency": 3.0, "impact": 2.0, "trend": "stable"},
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# 3. NATURE RISK SUB-CATEGORY WEIGHTS & BENCHMARK DATA
# ═══════════════════════════════════════════════════════════════════════════════

# Nature risk sub-categories used in composite scoring
NATURE_RISK_CATEGORIES: list[str] = [
    "biodiversity_loss",
    "water_scarcity",
    "land_use_change",
    "pollution",
    "climate_feedback",
]

# Sector-specific sensitivity factors (amplifiers for each risk category)
# Scale: 1.0 (neutral) to 3.0 (extremely sensitive)
SECTOR_SENSITIVITY: dict[str, dict[str, float]] = {
    "agriculture": {
        "biodiversity_loss": 3.0,
        "water_scarcity": 3.0,
        "land_use_change": 3.0,
        "pollution": 2.5,
        "climate_feedback": 2.5,
    },
    "manufacturing": {
        "biodiversity_loss": 1.5,
        "water_scarcity": 2.5,
        "land_use_change": 1.5,
        "pollution": 2.5,
        "climate_feedback": 1.5,
    },
    "mining": {
        "biodiversity_loss": 3.0,
        "water_scarcity": 3.0,
        "land_use_change": 3.0,
        "pollution": 3.0,
        "climate_feedback": 2.0,
    },
    "energy": {
        "biodiversity_loss": 2.0,
        "water_scarcity": 2.0,
        "land_use_change": 2.5,
        "pollution": 2.5,
        "climate_feedback": 3.0,
    },
    "construction": {
        "biodiversity_loss": 2.5,
        "water_scarcity": 1.5,
        "land_use_change": 3.0,
        "pollution": 2.0,
        "climate_feedback": 1.5,
    },
    "real_estate": {
        "biodiversity_loss": 1.5,
        "water_scarcity": 1.5,
        "land_use_change": 2.0,
        "pollution": 1.5,
        "climate_feedback": 2.0,
    },
    "forestry": {
        "biodiversity_loss": 2.5,
        "water_scarcity": 2.0,
        "land_use_change": 2.5,
        "pollution": 1.5,
        "climate_feedback": 2.5,
    },
}

# Industry benchmarks — average nature risk scores by sector (1-5)
# Based on ENCORE, WWF Biodiversity Risk Filter, and S&P Global data analysis
INDUSTRY_BENCHMARKS: dict[str, dict[str, float]] = {
    "agriculture": {
        "overall": 4.2,
        "biodiversity_loss": 4.5,
        "water_scarcity": 4.0,
        "land_use_change": 4.5,
        "pollution": 3.8,
        "climate_feedback": 3.5,
        "sample_size": 420,
    },
    "mining": {
        "overall": 3.9,
        "biodiversity_loss": 4.3,
        "water_scarcity": 3.8,
        "land_use_change": 4.2,
        "pollution": 4.0,
        "climate_feedback": 3.0,
        "sample_size": 310,
    },
    "manufacturing": {
        "overall": 2.8,
        "biodiversity_loss": 2.2,
        "water_scarcity": 3.2,
        "land_use_change": 2.0,
        "pollution": 3.5,
        "climate_feedback": 2.5,
        "sample_size": 680,
    },
    "energy": {
        "overall": 3.5,
        "biodiversity_loss": 3.5,
        "water_scarcity": 2.8,
        "land_use_change": 3.5,
        "pollution": 3.8,
        "climate_feedback": 4.0,
        "sample_size": 380,
    },
    "construction": {
        "overall": 3.1,
        "biodiversity_loss": 3.0,
        "water_scarcity": 2.5,
        "land_use_change": 3.8,
        "pollution": 3.0,
        "climate_feedback": 2.5,
        "sample_size": 250,
    },
    "real_estate": {
        "overall": 2.5,
        "biodiversity_loss": 2.0,
        "water_scarcity": 2.0,
        "land_use_change": 2.5,
        "pollution": 2.0,
        "climate_feedback": 2.8,
        "sample_size": 190,
    },
    "forestry": {
        "overall": 3.7,
        "biodiversity_loss": 4.0,
        "water_scarcity": 3.0,
        "land_use_change": 3.5,
        "pollution": 2.0,
        "climate_feedback": 3.5,
        "sample_size": 140,
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# 4. TNFD LEAP SUB-STEPS — FRAMEWORK DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════
# Source: TNFD v1.0 Recommendations (September 2023), LEAP approach
# https://tnfd.global/publication/recommendations-of-the-taskforce-on-nature-related-financial-disclosures/

LEAP_PHASES: dict[str, dict[str, Any]] = {
    "locate": {
        "label": "Locate interface with nature",
        "sub_steps": [
            {
                "id": "L1",
                "title": "Scope the assessment",
                "description": "Identify business activities, value chain, and geographic footprint",
                "output": "Assessment boundary and scope defined",
            },
            {
                "id": "L2",
                "title": "Interface with nature",
                "description": "Map locations where business activities interface with nature across value chain",
                "output": "Location map with ecosystem interface points",
            },
            {
                "id": "L3",
                "title": "Priority locations",
                "description": "Identify locations in or near areas of high biodiversity importance or water stress",
                "output": "Priority location list with IUCN biome mapping",
            },
        ],
    },
    "evaluate": {
        "label": "Evaluate dependencies and impacts",
        "sub_steps": [
            {
                "id": "E1",
                "title": "Identification of dependencies",
                "description": "Identify business dependencies on ecosystem services at each priority location",
                "output": "Dependency register (ENCORE-based)",
            },
            {
                "id": "E2",
                "title": "Identification of impacts",
                "description": "Identify business impacts on nature at each priority location",
                "output": "Impact register with severity scoring",
            },
            {
                "id": "E3",
                "title": "Dependency and impact analysis",
                "description": "Analyse materiality, scale, and reversibility of dependencies and impacts",
                "output": "Materiality matrix (dependencies vs impacts)",
            },
        ],
    },
    "assess": {
        "label": "Assess material risks and opportunities",
        "sub_steps": [
            {
                "id": "A1",
                "title": "Risk and opportunity identification",
                "description": "Identify physical, transition, and systemic nature-related risks and opportunities",
                "output": "Risk and opportunity register",
            },
            {
                "id": "A2",
                "title": "Existing risk mitigation",
                "description": "Assess existing risk management and mitigation measures",
                "output": "Current mitigation assessment",
            },
            {
                "id": "A3",
                "title": "Scenario analysis",
                "description": "Apply nature-positive and nature-negative scenarios to assess resilience",
                "output": "Scenario analysis outcomes",
            },
        ],
    },
    "prepare": {
        "label": "Prepare to respond and report",
        "sub_steps": [
            {
                "id": "P1",
                "title": "Strategy and resource allocation",
                "description": "Define response strategy, targets, and resource allocation for nature",
                "output": "Nature strategy with aligned resource plan",
            },
            {
                "id": "P2",
                "title": "Disclosure preparation",
                "description": "Prepare TNFD-aligned disclosures: Governance, Strategy, Risk Management, Metrics & Targets",
                "output": "TNFD disclosure report draft",
            },
            {
                "id": "P3",
                "title": "Metrics and targets",
                "description": "Define core and additional indicators aligned with TNFD and ESRS E4",
                "output": "Metrics & targets dashboard",
            },
        ],
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# 5. ESRS E4 — BIODIVERSITY & ECOSYSTEMS SUB-TOPIC REFERENCE
# ═══════════════════════════════════════════════════════════════════════════════
# Source: Commission Delegated Regulation (EU) 2023/2772, ESRS E4
# Disclosure Requirements E4-1 through E4-8 (E4-1 to E4-4 most relevant)

ESRS_E4_TOPICS: dict[str, dict[str, str]] = {
    "E4-1": {
        "title": "Transition plan for biodiversity and ecosystems",
        "description": (
            "Disclosure of the undertaking's transition plan for aligning with "
            "the Kunming-Montreal Global Biodiversity Framework targets and "
            "EU Biodiversity Strategy for 2030"
        ),
        "tnfd_alignment": "P1 (Strategy & Resource Allocation)",
    },
    "E4-2": {
        "title": "Policies related to biodiversity and ecosystems",
        "description": (
            "Disclosure of policies adopted to manage material impacts, "
            "risks, and opportunities related to biodiversity and ecosystems"
        ),
        "tnfd_alignment": "P2 (Governance & Strategy disclosure)",
    },
    "E4-3": {
        "title": "Actions and resources related to biodiversity and ecosystems",
        "description": (
            "Disclosure of actions taken and resources allocated to address "
            "material biodiversity impacts, dependencies, risks, and opportunities"
        ),
        "tnfd_alignment": "A2 (Risk Mitigation) + P1 (Resource Allocation)",
    },
    "E4-4": {
        "title": "Targets related to biodiversity and ecosystems",
        "description": (
            "Disclosure of measurable outcome-oriented targets aligned with "
            "the mitigation hierarchy: avoid, reduce, restore, transform"
        ),
        "tnfd_alignment": "P3 (Metrics & Targets)",
    },
    "E4-5": {
        "title": "Impact metrics — land-use change & land cover",
        "description": "Metrics on land-use change, land cover, and ecosystem condition",
        "tnfd_alignment": "E2 (Impact Identification)",
    },
    "E4-6": {
        "title": "Impact metrics — species and extinction risk",
        "description": "Metrics on species extinction risk and population trends",
        "tnfd_alignment": "E3 (Dependency & Impact Analysis)",
    },
    "E4-7": {
        "title": "Impact metrics — water and ocean ecosystems",
        "description": "Metrics on water-related ecosystem impacts and ocean ecosystem state",
        "tnfd_alignment": "E2 (Impact Identification)",
    },
    "E4-8": {
        "title": "Impact metrics — ecosystem services",
        "description": "Monetary and non-monetary metrics on ecosystem service dependency",
        "tnfd_alignment": "E1 (Dependency Identification)",
    },
}

# TNFD metrics categories mapped to ESRS E4 indicators
TNFD_ESRS_E4_METRICS_MAP: dict[str, dict[str, Any]] = {
    "ecosystem_condition": {
        "tnfd_metric": "Ecosystem Condition (core)",
        "esrs_e4_ref": "E4-5",
        "indicator": "Land-use change and ecosystem condition index",
        "unit": "hectares converted / condition score (1-5)",
    },
    "species_extinction_risk": {
        "tnfd_metric": "Species Extinction Risk (core)",
        "esrs_e4_ref": "E4-6",
        "indicator": "IUCN Red List Index by operational area",
        "unit": "species count / RLI score",
    },
    "water_ecosystems": {
        "tnfd_metric": "Water Ecosystem State (core)",
        "esrs_e4_ref": "E4-7",
        "indicator": "Water quality index and hydrological alteration",
        "unit": "WQI (0-100) / flow deviation %",
    },
    "ecosystem_service_dependency": {
        "tnfd_metric": "Ecosystem Service Dependency (additional)",
        "esrs_e4_ref": "E4-8",
        "indicator": "ENCORE dependency score and economic valuation",
        "unit": "dependency score (1-5) / EUR",
    },
    "land_use_footprint": {
        "tnfd_metric": "Land-use Footprint (core)",
        "esrs_e4_ref": "E4-5",
        "indicator": "Area footprint by ecosystem type and intensity",
        "unit": "hectares by IUCN functional group",
    },
    "pollution_release": {
        "tnfd_metric": "Pollution (core)",
        "esrs_e4_ref": "E4-5",
        "indicator": "Nutrient and pollutant loading to air/water/soil",
        "unit": "kg N/ha/yr, kg P/ha/yr, tonnes pollutants",
    },
    "water_withdrawal": {
        "tnfd_metric": "Water Withdrawal (core)",
        "esrs_e4_ref": "E4-7",
        "indicator": "Total water withdrawal by source and water-stressed area %",
        "unit": "m³ / % in water-stressed areas",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# 6. HELPER: REGION-TO-BIOME MAPPING (SIMPLIFIED)
# ═══════════════════════════════════════════════════════════════════════════════

REGION_BIOME_MAP: dict[str, list[dict[str, Any]]] = {
    "EU": [
        {"iucn_code": "T6", "description": "Intensive land-use (agricultural matrix)", "weight": 0.4},
        {"iucn_code": "F2", "description": "Lakes and reservoirs", "weight": 0.15},
        {"iucn_code": "F1", "description": "Rivers and streams", "weight": 0.15},
        {"iucn_code": "T1", "description": "Temperate forests (remnant)", "weight": 0.2},
        {"iucn_code": "F3", "description": "Wetlands", "weight": 0.1},
    ],
    "DE": [
        {"iucn_code": "T6", "description": "Intensive agricultural & urban land use", "weight": 0.5},
        {"iucn_code": "F2", "description": "Lakes (Bodensee, Müritz)", "weight": 0.12},
        {"iucn_code": "F1", "description": "Rivers (Rhine, Elbe, Danube)", "weight": 0.13},
        {"iucn_code": "T1", "description": "Mixed temperate forest", "weight": 0.18},
        {"iucn_code": "F3", "description": "Bogs and fens (Norddeutschland)", "weight": 0.07},
    ],
    "FR": [
        {"iucn_code": "T6", "description": "Agricultural land use (cereals, vineyards)", "weight": 0.45},
        {"iucn_code": "T1", "description": "Temperate & Mediterranean forest", "weight": 0.25},
        {"iucn_code": "F1", "description": "Rivers (Loire, Rhône, Seine)", "weight": 0.12},
        {"iucn_code": "M3", "description": "Mediterranean coast & wetlands (Camargue)", "weight": 0.1},
        {"iucn_code": "F3", "description": "Wetlands", "weight": 0.08},
    ],
    "UK": [
        {"iucn_code": "T6", "description": "Intensive land-use (agriculture, urban)", "weight": 0.45},
        {"iucn_code": "F3", "description": "Peatlands and wetlands", "weight": 0.18},
        {"iucn_code": "F1", "description": "Rivers (Thames, Severn)", "weight": 0.12},
        {"iucn_code": "M3", "description": "Coastal and intertidal systems", "weight": 0.15},
        {"iucn_code": "T1", "description": "Broadleaf and mixed woodland", "weight": 0.1},
    ],
    "global": [
        {"iucn_code": "T6", "description": "Intensive land-use systems", "weight": 0.25},
        {"iucn_code": "T1", "description": "Tropical/subtropical forests", "weight": 0.25},
        {"iucn_code": "M3", "description": "Coastal systems (mangroves, reefs)", "weight": 0.15},
        {"iucn_code": "F1", "description": "Rivers and streams", "weight": 0.1},
        {"iucn_code": "F2", "description": "Lakes and reservoirs", "weight": 0.05},
        {"iucn_code": "F3", "description": "Wetlands", "weight": 0.1},
        {"iucn_code": "M1", "description": "Marine shelves", "weight": 0.1},
    ],
}


def _normalise_sector(sector: str) -> str:
    """Normalise sector name to lowercase key for dictionary lookups."""
    s = sector.lower().strip()
    # Map common variants
    alias: dict[str, str] = {
        "agri": "agriculture",
        "farming": "agriculture",
        "industrial": "manufacturing",
        "industry": "manufacturing",
        "energy_utility": "energy",
        "utilities": "energy",
        "realestate": "real_estate",
        "property": "real_estate",
        "building": "construction",
        "infrastructure": "construction",
        "timber": "forestry",
        "logging": "forestry",
    }
    return alias.get(s, s) if s in alias else s


def _resolve_regions(regions: list[str]) -> list[str]:
    """Expand region codes to known keys; fallback to 'global' for unknown."""
    resolved = []
    for r in regions:
        if r in REGION_BIOME_MAP:
            resolved.append(r)
        else:
            resolved.append("global")
    return resolved if resolved else ["global"]


# ═══════════════════════════════════════════════════════════════════════════════
# 7. PUBLIC API FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def get_tnfd_leap_locate(sector: str, regions: list[str]) -> dict:
    """
    Locate the interface with nature (TNFD LEAP Phase 1).

    Identifies ecosystem types at operational locations, maps them to IUCN
    biome classifications, and flags priority locations near areas of high
    biodiversity importance.

    Args:
        sector: Industry sector name (e.g., "agriculture", "manufacturing").
        regions: List of region/country codes (e.g., ["DE", "FR", "EU"]).

    Returns:
        dict with assessment boundary, priority locations, biome map, and
        sub-step completions for L1, L2, L3.
    """
    sector_norm = _normalise_sector(sector)
    resolved_regions = _resolve_regions(regions)

    # Compile biome interfaces for each region
    biome_interfaces: list[dict[str, Any]] = []
    priority_locations: list[dict[str, Any]] = []
    total_weight = 0.0

    for region in resolved_regions:
        biomes = REGION_BIOME_MAP.get(region, REGION_BIOME_MAP["global"])
        for b in biomes:
            iucn_info = IUCN_BIOMES.get(b["iucn_code"], {"name": "Unknown", "priority": False})
            entry = {
                "region": region,
                "iucn_code": b["iucn_code"],
                "biome_name": iucn_info.get("name", "Unknown"),
                "realm": iucn_info.get("realm", "Unknown"),
                "functional_group": iucn_info.get("functional_group", "Unknown"),
                "weight": b["weight"],
                "description": b["description"],
            }
            biome_interfaces.append(entry)

            if iucn_info.get("priority", False):
                priority_locations.append(entry)
                total_weight += b["weight"]

    # Build L1-L3 sub-step results
    sub_steps = [
        {
            "id": "L1",
            "title": "Scope the assessment",
            "status": "complete",
            "output": {
                "sector": sector_norm,
                "regions_scoped": resolved_regions,
                "value_chain_stages": ["direct_operations", "upstream_supply_chain"],
                "boundary": f"{sector_norm.title()} operations in {len(resolved_regions)} region(s)",
            },
        },
        {
            "id": "L2",
            "title": "Interface with nature",
            "status": "complete",
            "output": {
                "total_biome_interfaces": len(biome_interfaces),
                "biomes_by_realm": {
                    realm: sum(1 for b in biome_interfaces if b["realm"] == realm)
                    for realm in set(b["realm"] for b in biome_interfaces)
                },
                "interface_map": biome_interfaces,
            },
        },
        {
            "id": "L3",
            "title": "Priority locations",
            "status": "complete",
            "output": {
                "priority_location_count": len(priority_locations),
                "priority_weight_pct": round(total_weight * 100, 1),
                "priority_locations": priority_locations,
            },
        },
    ]

    return {
        "phase": "locate",
        "phase_label": LEAP_PHASES["locate"]["label"],
        "sector": sector_norm,
        "regions": resolved_regions,
        "sub_steps": sub_steps,
        "summary": (
            f"Located {len(biome_interfaces)} ecosystem interfaces across "
            f"{len(resolved_regions)} region(s). "
            f"Identified {len(priority_locations)} priority location(s) "
            f"representing {round(total_weight * 100, 1)}% of interface weight."
        ),
    }


def get_tnfd_leap_evaluate(sector: str, regions: list[str]) -> dict:
    """
    Evaluate dependencies on ecosystem services and impacts on nature
    (TNFD LEAP Phase 2).

    Identifies key ecosystem service dependencies using ENCORE-based sector
    data, scores dependencies (1-5), identifies and scores impacts, and
    flags sector-specific material issues.

    Args:
        sector: Industry sector name.
        regions: List of region/country codes.

    Returns:
        dict with dependency register, impact register, materiality matrix,
        and sector-specific analysis.
    """
    sector_norm = _normalise_sector(sector)
    resolved_regions = _resolve_regions(regions)

    # Get dependency/impact data for this sector
    dep_data = SECTOR_NATURE_DEPENDENCIES.get(sector_norm, SECTOR_NATURE_DEPENDENCIES.get("manufacturing"))

    # Build dependency register
    dependencies: list[dict[str, Any]] = []
    impacts: list[dict[str, Any]] = []
    for service, data in dep_data.items():
        service_label = service.replace("_", " ").title()
        dependencies.append({
            "ecosystem_service": service,
            "label": service_label,
            "dependency_score": data["dependency"],
            "interpretation": _dependency_label(data["dependency"]),
            "trend": data["trend"],
        })
        impacts.append({
            "ecosystem_service": service,
            "label": service_label,
            "impact_score": data["impact"],
            "interpretation": _dependency_label(data["impact"]),
            "trend": data["trend"],
        })

    # Calculate materiality matrix (dependency vs impact)
    materiality = []
    for d, i in zip(dependencies, impacts):
        severity = round((d["dependency_score"] + i["impact_score"]) / 2, 1)
        materiality.append({
            "ecosystem_service": d["ecosystem_service"],
            "dependency_score": d["dependency_score"],
            "impact_score": i["impact_score"],
            "combined_materiality": severity,
            "material": severity >= 3.0,
        })

    material_count = sum(1 for m in materiality if m["material"])

    # Sector-specific qualitative assessment
    sector_notes: dict[str, str] = {
        "agriculture": (
            "Extreme dependency on pollination, water supply, and soil quality. "
            "Intensive farming drives habitat loss and soil degradation — "
            "the sector faces the highest nature-related risk overall."
        ),
        "manufacturing": (
            "High dependency on water for cooling and processing. Raw material "
            "sourcing (biomass, minerals) creates moderate impact on biodiversity. "
            "Key issue: water scarcity in stressed basins."
        ),
        "real_estate": (
            "Moderate dependency on water and flood protection. Land conversion "
            "for development drives habitat fragmentation. Green building "
            "certifications reduce but do not eliminate impacts."
        ),
    }

    return {
        "phase": "evaluate",
        "phase_label": LEAP_PHASES["evaluate"]["label"],
        "sector": sector_norm,
        "regions": resolved_regions,
        "dependency_register": {
            "total_dependencies": len(dependencies),
            "high_dependencies": sum(1 for d in dependencies if d["dependency_score"] >= 4.0),
            "services": dependencies,
        },
        "impact_register": {
            "total_impacts": len(impacts),
            "high_impacts": sum(1 for i in impacts if i["impact_score"] >= 4.0),
            "services": impacts,
        },
        "materiality_matrix": {
            "total_material_issues": material_count,
            "entries": materiality,
        },
        "sector_specific_notes": sector_notes.get(sector_norm, ""),
        "summary": (
            f"Evaluated {len(dependencies)} ecosystem service dependencies for "
            f"{sector_norm}. Found {material_count} material issues "
            f"(combined dependency+impact >= 3.0). "
            f"Top dependencies: {', '.join(d['label'] for d in dependencies if d['dependency_score'] >= 4.0)}."
        ),
    }


def get_tnfd_leap_assess(sector: str, regions: list[str], proximity_score: int = 3) -> dict:
    """
    Assess material risks and opportunities (TNFD LEAP Phase 3).

    Identifies physical nature risks (biodiversity loss, water scarcity),
    transition risks (regulatory, reputational, market), and opportunities.
    Performs scenario analysis comparing nature-positive and nature-negative
    pathways.

    Args:
        sector: Industry sector name.
        regions: List of region/country codes.
        proximity_score: Proximity to nature (1 = urban/remote, 5 =
                         directly in/near protected areas). Default 3.

    Returns:
        dict with risk register, opportunity register, scenario analysis,
        and overall risk level.
    """
    sector_norm = _normalise_sector(sector)
    resolved_regions = _resolve_regions(regions)
    proximity_score = max(1, min(5, proximity_score))

    dep_data = SECTOR_NATURE_DEPENDENCIES.get(sector_norm, {})

    # ---- Physical risks ----
    avg_dependency = (
        sum(v["dependency"] for v in dep_data.values()) / len(dep_data)
        if dep_data else 2.0
    )
    avg_impact = (
        sum(v["impact"] for v in dep_data.values()) / len(dep_data)
        if dep_data else 2.0
    )

    physical_risk_score = round((avg_dependency + avg_impact) / 2 * proximity_score / 5, 2)
    physical_risk_level = _risk_level(physical_risk_score)

    physical_risks = [
        {
            "risk": "Biodiversity loss and ecosystem degradation",
            "score": round(avg_impact * proximity_score / 5, 1),
            "level": _risk_level(avg_impact * proximity_score / 5),
            "description": (
                "Operational proximity to high-biodiversity areas increases "
                "exposure to ecosystem degradation and species loss"
            ),
            "time_horizon": "short_to_medium_term",
        },
        {
            "risk": "Water scarcity and quality deterioration",
            "score": round(dep_data.get("water_supply", {}).get("dependency", 2.0) * proximity_score / 5, 1),
            "level": _risk_level(dep_data.get("water_supply", {}).get("dependency", 2.0) * proximity_score / 5),
            "description": "Dependence on water supply in water-stressed basins creates operational risk",
            "time_horizon": "short_term",
        },
        {
            "risk": "Land-use change and habitat fragmentation",
            "score": round(dep_data.get("habitat_maintenance", {}).get("impact", 2.0) * proximity_score / 5, 1),
            "level": _risk_level(dep_data.get("habitat_maintenance", {}).get("impact", 2.0) * proximity_score / 5),
            "description": "Operations adjacent to natural habitats drive fragmentation and edge effects",
            "time_horizon": "long_term",
        },
        {
            "risk": "Pollution loading (nutrients, chemicals, waste)",
            "score": round((avg_impact + dep_data.get("soil_quality", {}).get("impact", 2.0)) / 2 * proximity_score / 5, 1),
            "level": _risk_level((avg_impact + dep_data.get("soil_quality", {}).get("impact", 2.0)) / 2 * proximity_score / 5),
            "description": "Nutrient runoff, chemical discharge, and waste accumulation affect ecosystem health",
            "time_horizon": "medium_term",
        },
    ]

    # ---- Transition risks ----
    regulation_risk = round(avg_impact * 0.8, 1)  # higher for high-impact sectors
    transition_risks = [
        {
            "risk": "Regulatory and policy risk (EU Nature Restoration Law, CSRD/ESRS E4)",
            "score": regulation_risk,
            "level": _risk_level(regulation_risk),
            "description": "Expanding regulatory requirements for nature-related disclosures and restoration",
        },
        {
            "risk": "Reputational risk and stakeholder pressure",
            "score": round(avg_impact * 0.7, 1),
            "level": _risk_level(avg_impact * 0.7),
            "description": "NGO and investor scrutiny of nature impacts, particularly in priority ecosystems",
        },
        {
            "risk": "Market and technology risk",
            "score": round(avg_dependency * 0.6, 1),
            "level": _risk_level(avg_dependency * 0.6),
            "description": "Shift in consumer demand toward nature-positive products and supply chains",
        },
    ]

    # ---- Opportunities ----
    opportunities = [
        {
            "opportunity": "Nature-positive certification and premiums",
            "score": round(min(5.0, avg_dependency * 0.6), 1),
            "description": "Eco-labelling and biodiversity credits can unlock revenue premiums",
        },
        {
            "opportunity": "Ecosystem restoration for resilience",
            "score": round(min(5.0, proximity_score * 0.7), 1),
            "description": "Restoration of adjacent ecosystems buffers physical risks (flood, heat)",
        },
        {
            "opportunity": "First-mover advantage in TNFD disclosure",
            "score": round(min(5.0, regulation_risk * 0.8), 1),
            "description": "Early adoption of TNFD builds investor confidence and reduces cost of capital",
        },
    ]

    # ---- Scenario analysis ----
    scenarios = {
        "nature_positive_2050": {
            "description": (
                "Effective implementation of Kunming-Montreal GBF, EU Nature "
                "Restoration Law, and sectoral biodiversity commitments."
            ),
            "risk_reduction_pct": 45,
            "cost_to_implement": "Moderate-high (transition investment required)",
            "outcome": "Biodiversity stabilised, ecosystem services maintained, regulatory compliance achieved",
        },
        "nature_negative_2050": {
            "description": (
                "Continued biodiversity loss beyond planetary boundaries, "
                "ecosystem service collapse in critical biomes."
            ),
            "risk_increase_pct": 60,
            "cost_of_inaction": "Very high (operational disruption, regulatory fines, asset stranding)",
            "outcome": "Severe degradation of 30%+ of natural capital, water and pollination service failure",
        },
        "baseline_current_trends": {
            "description": "Continuation of current rates of biodiversity loss and partial adoption of TNFD",
            "risk_level": "moderate",
            "outcome": "Moderate biodiversity decline, increasing regulatory pressure, partial mitigation",
        },
    }

    overall_risk = round(physical_risk_score, 1)

    return {
        "phase": "assess",
        "phase_label": LEAP_PHASES["assess"]["label"],
        "sector": sector_norm,
        "regions": resolved_regions,
        "proximity_score": proximity_score,
        "physical_risks": {
            "overall_score": overall_risk,
            "overall_level": _risk_level(overall_risk),
            "risks": physical_risks,
        },
        "transition_risks": {
            "risks": transition_risks,
        },
        "opportunities": {
            "count": len(opportunities),
            "items": opportunities,
        },
        "scenario_analysis": scenarios,
        "summary": (
            f"Overall nature risk score: {overall_risk}/5 ({_risk_level(overall_risk)}). "
            f"Physical risks scored across {len(physical_risks)} categories. "
            f"Under a nature-negative scenario, risks could increase by 60%."
        ),
    }


def get_tnfd_leap_prepare(sector: str, risk_level: str) -> dict:
    """
    Prepare TNFD disclosures (TNFD LEAP Phase 4).

    Structures response strategy, governance, risk management, and metrics
    aligned with TNFD's four disclosure pillars and ESRS E4 sub-topics.
    Includes reconciliation table between TNFD and ESRS E4.

    Args:
        sector: Industry sector name.
        risk_level: Risk level string ("low", "moderate", "high", "very_high").

    Returns:
        dict with governance, strategy, risk management, metrics & targets,
        and TNFD-ESRS E4 reconciliation table.
    """
    sector_norm = _normalise_sector(sector)
    risk_level = risk_level.strip().lower()

    # Map risk level to recommended response intensity
    response_intensity: dict[str, dict[str, Any]] = {
        "low": {"target_rigour": "basic", "board_oversight": "annual", "restoration_requirement": "voluntary"},
        "moderate": {"target_rigour": "moderate", "board_oversight": "semi_annual", "restoration_requirement": "targeted"},
        "high": {"target_rigour": "detailed", "board_oversight": "quarterly", "restoration_requirement": "mandatory"},
        "very_high": {"target_rigour": "comprehensive", "board_oversight": "monthly", "restoration_requirement": "mandatory+offset"},
    }

    intensity = response_intensity.get(risk_level, response_intensity["moderate"])

    # Governance recommendations
    governance = {
        "board_oversight": {
            "frequency": intensity["board_oversight"],
            "body": "Board Sustainability / ESG Committee",
            "responsibilities": [
                "Review nature-related risk appetite statement",
                "Approve biodiversity targets and transition plan",
                "Oversee TNFD disclosure preparation",
            ],
        },
        "management_role": {
            "function": "Chief Sustainability Officer / VP Nature",
            "responsibilities": [
                "Lead LEAP assessment implementation",
                "Coordinate cross-functional nature risk working group",
                "Report to board on nature-related metrics quarterly",
            ],
        },
    }

    # Strategy alignment
    strategy = {
        "nature_transition_plan": {
            "aligned_with": [
                "Kunming-Montreal Global Biodiversity Framework (23 targets)",
                "EU Biodiversity Strategy for 2030",
                "SBTN (Science Based Targets for Nature) interim guidance",
            ],
            "key_commitments": [
                "Achieve no net loss of biodiversity by 2030 (ALIGN framework)",
                "Net positive impact on priority ecosystems by 2035",
                "100% deforestation-free supply chain by 2025",
            ],
            "rigour": intensity["target_rigour"],
        },
        "scenario_integration": {
            "scenarios_used": ["nature_positive_2050", "nature_negative_2050", "baseline_current_trends"],
            "resilience_statement": (
                f"Under nature-positive scenarios, {sector_norm} sector demonstrates "
                f"resilience with moderate transition costs. Nature-negative pathways "
                f"pose severe physical risk to operations."
            ),
        },
    }

    # Risk management framework
    risk_management = {
        "process": "Integrated nature-risk management within ERM framework",
        "identification_methods": [
            "ENCORE dependency screening",
            "IUCN Red List proximity analysis",
            "WWF Biodiversity Risk Filter",
            "SBTN materiality screening",
        ],
        "mitigation_hierarchy": {
            "avoid": "Avoid operations in IUCN categories I-IV protected areas",
            "reduce": "Reduce land footprint and pollution loading per unit output",
            "restore": intensity["restoration_requirement"],
            "transform": "Transform business model toward nature-positive value chains",
        },
        "monitoring": {
            "frequency": "quarterly",
            "indicators": [
                "Ecosystem condition index",
                "Species extinction risk (RLI)",
                "Water withdrawal in stressed basins",
                "Land-use conversion rate",
            ],
        },
    }

    # Metrics and targets (with ESRS E4 cross-reference)
    metrics_targets = []
    for key, m in TNFD_ESRS_E4_METRICS_MAP.items():
        metrics_targets.append({
            "category": key.replace("_", " ").title(),
            "tnfd_metric": m["tnfd_metric"],
            "esrs_e4_ref": m["esrs_e4_ref"],
            "indicator": m["indicator"],
            "unit": m["unit"],
            "target_2030": _suggest_target(sector_norm, key, risk_level),
        })

    # TNFD vs ESRS E4 reconciliation table
    reconciliation = {
        "description": "Mapping between TNFD disclosure pillars and ESRS E4 Disclosure Requirements",
        "mapping": [
            {
                "tnfd_pillar": "Governance",
                "tnfd_requirement": "Disclose governance processes for nature-related dependencies",
                "esrs_e4_ref": "E4-2 (Policies), E4-1 (Transition plan governance)",
                "gap_analysis": "TNFD includes board-level nature competency requirement not explicit in ESRS E4",
            },
            {
                "tnfd_pillar": "Strategy",
                "tnfd_requirement": "Disclose strategy for nature-related risks and opportunities",
                "esrs_e4_ref": "E4-1 (Transition plan), E4-3 (Actions & resources)",
                "gap_analysis": "ESRS E4-1 requires transition plan disclosure; TNFD requires scenario analysis",
            },
            {
                "tnfd_pillar": "Risk Management",
                "tnfd_requirement": "Disclose risk identification and mitigation processes",
                "esrs_e4_ref": "E4-3 (Actions), E4-2 (Risk management integration)",
                "gap_analysis": "TNFD explicitly requires LEAP methodology; ESRS E4 requires double materiality assessment",
            },
            {
                "tnfd_pillar": "Metrics & Targets",
                "tnfd_requirement": "Disclose metrics and targets aligned with TNFD core indicators",
                "esrs_e4_ref": "E4-4 (Targets), E4-5 through E4-8 (Impact metrics)",
                "gap_analysis": "TNFD core indicators align well with E4-5 to E4-8; E4-4 adds outcome-oriented targets",
            },
        ],
        "overall_alignment": "High — approximately 80% overlap; key gap is TNFD's explicit LEAP methodology vs ESRS E4's principle-based approach",
    }

    return {
        "phase": "prepare",
        "phase_label": LEAP_PHASES["prepare"]["label"],
        "sector": sector_norm,
        "risk_level": risk_level,
        "response_intensity": intensity["target_rigour"],
        "governance": governance,
        "strategy": strategy,
        "risk_management": risk_management,
        "metrics_and_targets": {
            "total_metrics": len(metrics_targets),
            "items": metrics_targets,
        },
        "reconciliation_tnfd_esrs_e4": reconciliation,
        "summary": (
            f"Prepared TNFD disclosure framework for {sector_norm} "
            f"at '{risk_level}' risk level. {len(metrics_targets)} metrics "
            f"defined with ESRS E4 cross-reference. "
            f"Overall TNFD-ESRS E4 alignment: ~80%."
        ),
    }


def get_nature_risk_score(sector: str, proximity_score: int = 3) -> dict:
    """
    Calculate composite nature-related risk score (1-5) with sub-category
    breakdown, sector sensitivity, and industry benchmark comparison.

    Combines ENCORE dependency/impact data with proximity-to-nature and
    sector sensitivity factors to produce a quantitative risk score.
    Compares results against industry benchmarks.

    Args:
        sector: Industry sector name.
        proximity_score: Proximity to nature (1-5). Default 3.

    Returns:
        dict with overall score, sub-category scores, sensitivity analysis,
        and benchmark comparison.
    """
    sector_norm = _normalise_sector(sector)
    proximity_score = max(1, min(5, proximity_score))

    # Base risk from dependency/impact data
    dep_data = SECTOR_NATURE_DEPENDENCIES.get(sector_norm, SECTOR_NATURE_DEPENDENCIES.get("manufacturing"))

    # Compute base scores per sub-category from dependency/impact data
    # Map ecosystem services to risk sub-categories
    service_to_risk: dict[str, list[str]] = {
        "biodiversity_loss": ["habitat_maintenance", "genetic_materials"],
        "water_scarcity": ["water_supply", "water_flow_regulation"],
        "land_use_change": ["soil_quality", "biomass_provision"],
        "pollution": ["pest_control", "soil_quality"],  # pesticides & fertilisers
        "climate_feedback": ["climate_regulation", "flood_protection"],
    }

    raw_scores: dict[str, float] = {}
    for category, services in service_to_risk.items():
        scores = []
        for svc in services:
            if svc in dep_data:
                scores.append((dep_data[svc]["dependency"] + dep_data[svc]["impact"]) / 2)
        raw_scores[category] = sum(scores) / len(scores) if scores else 2.0

    # Apply sector sensitivity multipliers
    sensitivity = SECTOR_SENSITIVITY.get(sector_norm, {})
    adjusted_scores: dict[str, float] = {}
    for category in NATURE_RISK_CATEGORIES:
        base = raw_scores.get(category, 2.0)
        multiplier = sensitivity.get(category, 1.0)
        # Proximity amplifies physical risk categories
        proximity_factor = 1.0 + (proximity_score - 3) * 0.15  # ±30% at extremes
        if category in ("biodiversity_loss", "land_use_change", "water_scarcity"):
            proximity_factor = max(0.7, min(1.3, proximity_factor))
        else:
            proximity_factor = 1.0  # pollution and climate feedback less proximity-dependent
        adjusted_scores[category] = round(min(5.0, base * multiplier * proximity_factor), 2)

    overall_score = round(sum(adjusted_scores.values()) / len(adjusted_scores), 2)

    # Industry benchmark comparison
    benchmarks = INDUSTRY_BENCHMARKS.get(
        sector_norm,
        {"overall": 3.0, "sample_size": 0},
    )

    benchmark_comparison = {
        "our_score": overall_score,
        "industry_benchmark": benchmarks.get("overall", 3.0),
        "variance": round(overall_score - benchmarks.get("overall", 3.0), 2),
        "above_benchmark": overall_score > benchmarks.get("overall", 3.0),
        "sample_size": benchmarks.get("sample_size", 0),
    }

    # Sub-category details
    sub_categories = []
    for cat in NATURE_RISK_CATEGORIES:
        sub_categories.append({
            "category": cat.replace("_", " ").title(),
            "score": adjusted_scores[cat],
            "level": _risk_level(adjusted_scores[cat]),
            "sector_sensitivity": sensitivity.get(cat, 1.0),
            "industry_benchmark": benchmarks.get(cat, None),
        })

    return {
        "sector": sector_norm,
        "proximity_score": proximity_score,
        "overall_risk_score": overall_score,
        "overall_risk_level": _risk_level(overall_score),
        "sub_categories": sub_categories,
        "sector_sensitivity_factors": sensitivity,
        "benchmark_comparison": benchmark_comparison,
        "summary": (
            f"Nature risk score for {sector_norm}: {overall_score}/5 "
            f"({_risk_level(overall_score)}). "
            f"Highest sub-category: {max(sub_categories, key=lambda x: x['score'])['category']} "
            f"({max(s['score'] for s in sub_categories)}/5). "
            f"Industry benchmark: {benchmarks.get('overall', 'N/A')}/5."
        ),
    }


def get_tnfd_report(
    sector: str,
    regions: list[str],
    building_proximity_to_nature: int = 3,
) -> dict:
    """
    Full TNFD-aligned assessment for a given sector and region(s), using the
    LEAP (Locate, Evaluate, Assess, Prepare) approach. Cross-references
    findings with ESRS E4 biodiversity reporting requirements.

    This is the main entry point that orchestrates all four LEAP phases and
    produces a consolidated nature risk report.

    Args:
        sector: Industry sector name (e.g., "agriculture", "manufacturing").
        regions: List of region/country codes (e.g., ["DE", "FR", "EU"]).
        building_proximity_to_nature: Proximity score 1-5 indicating how
            close operations/real estate are to natural areas. Default 3.

    Returns:
        dict with full LEAP assessment, risk score, ESRS E4 cross-reference,
        and executive summary.
    """
    sector_norm = _normalise_sector(sector)
    resolved_regions = _resolve_regions(regions)
    prox = max(1, min(5, building_proximity_to_nature))

    # Execute each LEAP phase
    locate = get_tnfd_leap_locate(sector_norm, resolved_regions)
    evaluate = get_tnfd_leap_evaluate(sector_norm, resolved_regions)
    assess = get_tnfd_leap_assess(sector_norm, resolved_regions, proximity_score=prox)
    prepare = get_tnfd_leap_prepare(sector_norm, assess["physical_risks"]["overall_level"])

    # Composite risk score
    risk_score = get_nature_risk_score(sector_norm, proximity_score=prox)

    # ESRS E4 cross-reference summary
    esrs_e4_reference = []
    for code, topic in ESRS_E4_TOPICS.items():
        esrs_e4_reference.append({
            "code": code,
            "title": topic["title"],
            "tnfd_alignment": topic["tnfd_alignment"],
            "status": "covered" if code in ("E4-1", "E4-2", "E4-3", "E4-4") else "partially_covered",
        })

    # Executive summary
    exec_summary = (
        f"TNFD LEAP Assessment for **{sector_norm.title()}** across "
        f"{', '.join(resolved_regions)}. Overall nature risk score: "
        f"**{risk_score['overall_risk_score']}/5** "
        f"({risk_score['overall_risk_level'].replace('_', ' ').title()}). "
        f"Assessment spans {len(locate['sub_steps'][2]['output']['priority_locations'])} "
        f"priority locations, {len(evaluate['dependency_register']['services'])} "
        f"ecosystem service dependencies evaluated, "
        f"{len(assess['physical_risks']['risks'])} physical risk categories "
        f"and {len(assess['transition_risks']['risks'])} transition risk "
        f"categories modelled. ESRS E4 reconciliation shows approximately "
        f"80% alignment between TNFD and CSRD biodiversity requirements. "
        f"Recommended response: {prepare['response_intensity']}."
    )

    return {
        "report_type": "TNFD LEAP Full Assessment",
        "framework_version": "TNFD v1.0 (September 2023)",
        "sector": sector_norm,
        "regions": resolved_regions,
        "proximity_to_nature": prox,
        "executive_summary": exec_summary,
        "risk_score_summary": {
            "overall_score": risk_score["overall_risk_score"],
            "overall_level": risk_score["overall_risk_level"],
            "benchmark_comparison": risk_score["benchmark_comparison"],
        },
        "leap_assessment": {
            "locate": locate,
            "evaluate": evaluate,
            "assess": assess,
            "prepare": prepare,
        },
        "esrs_e4_cross_reference": esrs_e4_reference,
        "metrics_alignment": TNFD_ESRS_E4_METRICS_MAP,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 7. INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _dependency_label(score: float) -> str:
    """Convert a numeric dependency/impact score to a human-readable label."""
    if score >= 4.5:
        return "very_high"
    elif score >= 3.5:
        return "high"
    elif score >= 2.5:
        return "medium"
    elif score >= 1.5:
        return "low"
    else:
        return "very_low"


def _risk_level(score: float) -> str:
    """Convert a numeric risk score (0-5) to a risk level string."""
    if score >= 4.0:
        return "very_high"
    elif score >= 3.0:
        return "high"
    elif score >= 2.0:
        return "moderate"
    elif score >= 1.0:
        return "low"
    else:
        return "very_low"


def _suggest_target(sector: str, metric_key: str, risk_level: str) -> str:
    """Generate a recommended 2030 target based on sector, metric, and risk."""
    # Default targets by risk level
    level_map = {
        "very_high": "40% reduction by 2027, 70% by 2030",
        "high": "30% reduction by 2027, 55% by 2030",
        "moderate": "20% reduction by 2027, 40% by 2030",
        "low": "10% reduction by 2027, 25% by 2030",
    }
    base = level_map.get(risk_level, level_map["moderate"])

    sector_specific: dict[str, dict[str, str]] = {
        "agriculture": {
            "ecosystem_condition": "Regenerate 15% of operational land by 2030",
            "water_withdrawal": "Reduce irrigation water use by 35% by 2030 (per ton output)",
            "pollution_release": "Reduce nitrogen surplus by 30% by 2027, 50% by 2030",
        },
        "manufacturing": {
            "water_withdrawal": "Water neutrality (100% replenishment) in stressed basins by 2030",
            "pollution_release": "Zero process chemical discharge to water by 2028",
        },
        "mining": {
            "ecosystem_condition": "Rehabilitate 100% of disturbed land within 3 years of closure",
            "land_use_footprint": "Reduce operational footprint by 20% by 2030 through spatial efficiency",
        },
    }

    return sector_specific.get(sector, {}).get(metric_key, base)
