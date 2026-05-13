"""ESRS S1-S4 Social Sustainability data source.

Provides:
- ESRS S1 (Own Workforce) composite scoring
- Workforce benchmarks by sector and region
- ESRS S2/S3/S4 value chain risk assessment
- Human rights due diligence framework (UNGP, CSDDD)
- Full S1-S4 disclosure checklist

Sources: ESRS S1-S4 (EU 2023/2772), EFRAG IG 2024, Eurostat 2024,
ILO 2024, EU-OSHA 2024, EIGE 2024, HRW 2025.
"""

import asyncio
from typing import Any
from ..cache import get_cache
from ..utils import risk_label, risk_color, get_esrs_ref, today_iso

# ─── Sector Reference Data ─────────────────────────────────────────────

SECTORS: list[str] = [
    "manufacturing", "construction", "energy", "transport", "agriculture",
    "retail", "finance", "technology", "healthcare", "real_estate",
    "hospitality", "education", "textiles", "chemicals", "food_processing",
]

# ─── Industry Benchmarks: Turnover Rates (%) ───────────────────────────
# Source: Eurostat Labour Turnover 2024, ILO 2024

TURNOVER_BENCHMARKS: dict[str, dict[str, float]] = {
    "manufacturing":  {"eu_mean": 14.5, "top_quartile": 7.0, "bottom_quartile": 24.0,
                       "de": 12.0, "fr": 15.0, "uk": 16.5, "pl": 18.0, "es": 13.0, "it": 11.0},
    "construction":   {"eu_mean": 22.0, "top_quartile": 12.0, "bottom_quartile": 35.0,
                       "de": 18.0, "fr": 24.0, "uk": 25.0, "pl": 28.0, "es": 20.0, "it": 17.0},
    "energy":         {"eu_mean": 8.5,  "top_quartile": 4.0, "bottom_quartile": 15.0,
                       "de": 7.0, "fr": 9.0, "uk": 10.0, "pl": 11.0, "es": 8.0, "it": 7.0},
    "transport":      {"eu_mean": 18.0, "top_quartile": 10.0, "bottom_quartile": 28.0,
                       "de": 16.0, "fr": 19.0, "uk": 20.0, "pl": 22.0, "es": 17.0, "it": 15.0},
    "agriculture":    {"eu_mean": 20.0, "top_quartile": 11.0, "bottom_quartile": 32.0,
                       "de": 17.0, "fr": 22.0, "uk": 23.0, "pl": 25.0, "es": 21.0, "it": 18.0},
    "retail":         {"eu_mean": 25.0, "top_quartile": 15.0, "bottom_quartile": 38.0,
                       "de": 22.0, "fr": 27.0, "uk": 28.0, "pl": 30.0, "es": 24.0, "it": 20.0},
    "finance":        {"eu_mean": 12.0, "top_quartile": 6.0, "bottom_quartile": 20.0,
                       "de": 10.0, "fr": 13.0, "uk": 14.0, "pl": 15.0, "es": 11.0, "it": 9.0},
    "technology":     {"eu_mean": 16.0, "top_quartile": 8.0, "bottom_quartile": 26.0,
                       "de": 14.0, "fr": 17.0, "uk": 18.0, "pl": 20.0, "es": 15.0, "it": 13.0},
    "healthcare":     {"eu_mean": 13.0, "top_quartile": 7.0, "bottom_quartile": 21.0,
                       "de": 11.0, "fr": 14.0, "uk": 15.0, "pl": 16.0, "es": 12.0, "it": 10.0},
    "hospitality":    {"eu_mean": 30.0, "top_quartile": 18.0, "bottom_quartile": 45.0,
                       "de": 27.0, "fr": 32.0, "uk": 33.0, "pl": 35.0, "es": 29.0, "it": 25.0},
    "textiles":       {"eu_mean": 19.0, "top_quartile": 10.0, "bottom_quartile": 30.0,
                       "de": 16.0, "fr": 20.0, "uk": 21.0, "pl": 24.0, "es": 18.0, "it": 15.0},
    "food_processing":{"eu_mean": 17.0, "top_quartile": 9.0, "bottom_quartile": 27.0,
                       "de": 14.0, "fr": 18.0, "uk": 19.0, "pl": 22.0, "es": 16.0, "it": 13.0},
}

DEFAULT_TURNOVER = {"eu_mean": 15.0, "top_quartile": 8.0, "bottom_quartile": 25.0,
                    "de": 13.0, "fr": 16.0, "uk": 17.0, "pl": 19.0, "es": 14.0, "it": 12.0}

# ─── Industry Benchmarks: Injury Rates (per 100 workers) ───────────────
# Source: EU-OSHA 2024, Eurostat ESAW 2024

INJURY_BENCHMARKS: dict[str, dict[str, float]] = {
    "manufacturing":  {"eu_mean": 3.5, "top_quartile": 1.5, "bottom_quartile": 6.5,
                       "de": 2.8, "fr": 3.8, "uk": 2.5, "pl": 4.5, "es": 4.0, "it": 3.2},
    "construction":   {"eu_mean": 8.5, "top_quartile": 4.0, "bottom_quartile": 15.0,
                       "de": 7.0, "fr": 9.5, "uk": 6.5, "pl": 11.0, "es": 9.0, "it": 8.0},
    "energy":         {"eu_mean": 2.0, "top_quartile": 0.8, "bottom_quartile": 4.0,
                       "de": 1.5, "fr": 2.2, "uk": 1.8, "pl": 3.0, "es": 2.5, "it": 1.8},
    "transport":      {"eu_mean": 5.0, "top_quartile": 2.5, "bottom_quartile": 9.0,
                       "de": 4.0, "fr": 5.5, "uk": 4.5, "pl": 6.5, "es": 5.5, "it": 4.5},
    "agriculture":    {"eu_mean": 6.0, "top_quartile": 3.0, "bottom_quartile": 11.0,
                       "de": 5.0, "fr": 6.5, "uk": 5.5, "pl": 7.5, "es": 7.0, "it": 5.5},
    "retail":         {"eu_mean": 2.5, "top_quartile": 1.0, "bottom_quartile": 5.0,
                       "de": 2.0, "fr": 2.8, "uk": 2.2, "pl": 3.5, "es": 3.0, "it": 2.2},
    "finance":        {"eu_mean": 0.5, "top_quartile": 0.2, "bottom_quartile": 1.0,
                       "de": 0.3, "fr": 0.5, "uk": 0.4, "pl": 0.8, "es": 0.6, "it": 0.4},
    "technology":     {"eu_mean": 0.8, "top_quartile": 0.3, "bottom_quartile": 1.5,
                       "de": 0.5, "fr": 0.8, "uk": 0.7, "pl": 1.2, "es": 1.0, "it": 0.6},
    "healthcare":     {"eu_mean": 4.0, "top_quartile": 2.0, "bottom_quartile": 7.0,
                       "de": 3.5, "fr": 4.5, "uk": 4.0, "pl": 5.0, "es": 4.5, "it": 3.5},
    "hospitality":    {"eu_mean": 3.0, "top_quartile": 1.5, "bottom_quartile": 5.5,
                       "de": 2.5, "fr": 3.2, "uk": 2.8, "pl": 4.0, "es": 3.5, "it": 2.8},
    "textiles":       {"eu_mean": 2.8, "top_quartile": 1.2, "bottom_quartile": 5.0,
                       "de": 2.2, "fr": 3.0, "uk": 2.5, "pl": 3.8, "es": 3.2, "it": 2.5},
    "food_processing":{"eu_mean": 4.5, "top_quartile": 2.0, "bottom_quartile": 8.0,
                       "de": 3.8, "fr": 5.0, "uk": 4.0, "pl": 5.5, "es": 5.0, "it": 4.0},
}

DEFAULT_INJURY = {"eu_mean": 3.0, "top_quartile": 1.5, "bottom_quartile": 5.5,
                  "de": 2.5, "fr": 3.2, "uk": 2.8, "pl": 4.0, "es": 3.5, "it": 2.8}

# ─── Industry Benchmarks: Gender Pay Gap (%) ───────────────────────────
# Source: EIGE 2024, Eurostat 2024

PAYGAP_BENCHMARKS: dict[str, dict[str, float]] = {
    "manufacturing":  {"eu_mean": 14.0, "top_quartile": 6.0, "bottom_quartile": 22.0,
                       "de": 16.0, "fr": 12.0, "uk": 15.0, "pl": 18.0, "es": 10.0, "it": 8.0},
    "construction":   {"eu_mean": 18.0, "top_quartile": 9.0, "bottom_quartile": 28.0,
                       "de": 20.0, "fr": 16.0, "uk": 19.0, "pl": 22.0, "es": 14.0, "it": 11.0},
    "energy":         {"eu_mean": 12.0, "top_quartile": 5.0, "bottom_quartile": 20.0,
                       "de": 14.0, "fr": 10.0, "uk": 13.0, "pl": 16.0, "es": 8.0, "it": 7.0},
    "transport":      {"eu_mean": 15.0, "top_quartile": 7.0, "bottom_quartile": 24.0,
                       "de": 17.0, "fr": 13.0, "uk": 16.0, "pl": 19.0, "es": 11.0, "it": 9.0},
    "agriculture":    {"eu_mean": 16.0, "top_quartile": 8.0, "bottom_quartile": 25.0,
                       "de": 18.0, "fr": 14.0, "uk": 17.0, "pl": 20.0, "es": 12.0, "it": 10.0},
    "retail":         {"eu_mean": 10.0, "top_quartile": 4.0, "bottom_quartile": 18.0,
                       "de": 12.0, "fr": 9.0, "uk": 11.0, "pl": 14.0, "es": 7.0, "it": 6.0},
    "finance":        {"eu_mean": 20.0, "top_quartile": 10.0, "bottom_quartile": 30.0,
                       "de": 22.0, "fr": 18.0, "uk": 21.0, "pl": 24.0, "es": 16.0, "it": 14.0},
    "technology":     {"eu_mean": 16.0, "top_quartile": 7.0, "bottom_quartile": 25.0,
                       "de": 18.0, "fr": 14.0, "uk": 17.0, "pl": 20.0, "es": 12.0, "it": 10.0},
    "healthcare":     {"eu_mean": 8.0, "top_quartile": 3.0, "bottom_quartile": 14.0,
                       "de": 10.0, "fr": 7.0, "uk": 9.0, "pl": 12.0, "es": 6.0, "it": 5.0},
    "hospitality":    {"eu_mean": 12.0, "top_quartile": 5.0, "bottom_quartile": 20.0,
                       "de": 14.0, "fr": 10.0, "uk": 13.0, "pl": 16.0, "es": 9.0, "it": 7.0},
    "textiles":       {"eu_mean": 15.0, "top_quartile": 7.0, "bottom_quartile": 24.0,
                       "de": 17.0, "fr": 13.0, "uk": 16.0, "pl": 19.0, "es": 11.0, "it": 9.0},
    "food_processing":{"eu_mean": 13.0, "top_quartile": 6.0, "bottom_quartile": 21.0,
                       "de": 15.0, "fr": 11.0, "uk": 14.0, "pl": 17.0, "es": 9.0, "it": 7.0},
}

DEFAULT_PAYGAP = {"eu_mean": 13.0, "top_quartile": 6.0, "bottom_quartile": 21.0,
                  "de": 15.0, "fr": 11.0, "uk": 14.0, "pl": 17.0, "es": 9.0, "it": 7.0}

# ─── Region HR Risk Index for Supply Chain (1=low, 5=high) ────────────
# Sources: ILO 2024, HRW 2025, ITUC Global Rights Index 2025, US DOL List

REGION_HR_RISK: dict[str, dict[str, int]] = {
    "northern_europe":     {"child_labour": 1, "forced_labour": 1, "association": 1,
                            "discrimination": 1, "occupational_safety": 1, "wage": 1},
    "western_europe":      {"child_labour": 1, "forced_labour": 1, "association": 1,
                            "discrimination": 2, "occupational_safety": 1, "wage": 2},
    "southern_europe":     {"child_labour": 2, "forced_labour": 2, "association": 1,
                            "discrimination": 2, "occupational_safety": 2, "wage": 2},
    "eastern_europe":      {"child_labour": 3, "forced_labour": 3, "association": 3,
                            "discrimination": 3, "occupational_safety": 3, "wage": 3},
    "north_america":       {"child_labour": 2, "forced_labour": 2, "association": 2,
                            "discrimination": 2, "occupational_safety": 2, "wage": 2},
    "central_america":     {"child_labour": 4, "forced_labour": 4, "association": 3,
                            "discrimination": 3, "occupational_safety": 4, "wage": 4},
    "south_america":       {"child_labour": 3, "forced_labour": 3, "association": 3,
                            "discrimination": 3, "occupational_safety": 3, "wage": 3},
    "north_africa":        {"child_labour": 4, "forced_labour": 4, "association": 4,
                            "discrimination": 4, "occupational_safety": 4, "wage": 4},
    "sub_saharan_africa":  {"child_labour": 5, "forced_labour": 5, "association": 4,
                            "discrimination": 4, "occupational_safety": 5, "wage": 5},
    "middle_east":         {"child_labour": 4, "forced_labour": 4, "association": 5,
                            "discrimination": 4, "occupational_safety": 4, "wage": 4},
    "central_asia":        {"child_labour": 4, "forced_labour": 4, "association": 4,
                            "discrimination": 3, "occupational_safety": 4, "wage": 4},
    "south_asia":          {"child_labour": 5, "forced_labour": 5, "association": 4,
                            "discrimination": 4, "occupational_safety": 5, "wage": 5},
    "southeast_asia":      {"child_labour": 4, "forced_labour": 4, "association": 4,
                            "discrimination": 3, "occupational_safety": 4, "wage": 4},
    "east_asia":           {"child_labour": 3, "forced_labour": 3, "association": 3,
                            "discrimination": 3, "occupational_safety": 2, "wage": 3},
    "oceania":             {"child_labour": 2, "forced_labour": 1, "association": 2,
                            "discrimination": 1, "occupational_safety": 2, "wage": 2},
}

REGION_LABELS: dict[str, str] = {
    "northern_europe": "Northern Europe", "western_europe": "Western Europe",
    "southern_europe": "Southern Europe", "eastern_europe": "Eastern Europe",
    "north_america": "North America", "central_america": "Central America",
    "south_america": "South America", "north_africa": "North Africa",
    "sub_saharan_africa": "Sub-Saharan Africa", "middle_east": "Middle East",
    "central_asia": "Central Asia", "south_asia": "South Asia",
    "southeast_asia": "Southeast Asia", "east_asia": "East Asia",
    "oceania": "Oceania",
}

# ─── Sector-Specific Material Social Topics ─────────────────────────────
# ESRS IRO-1 (materiality assessment) guidance by sector

SECTOR_SOCIAL_TOPICS: dict[str, list[dict[str, Any]]] = {
    "textiles": [
        {"standard": "S2", "topic": "Child & forced labour in Tier 2-3 supply chain",
         "materiality": "high", "regions": ["south_asia", "southeast_asia"]},
        {"standard": "S1", "topic": "Gender pay gap & female workforce conditions",
         "materiality": "high"},
        {"standard": "S4", "topic": "Product safety & chemical transparency",
         "materiality": "high"},
        {"standard": "S2", "topic": "Freedom of association in supplier factories",
         "materiality": "high", "regions": ["south_asia", "southeast_asia"]},
    ],
    "agriculture": [
        {"standard": "S2", "topic": "Child labour in raw material supply (cocoa/coffee/cotton)",
         "materiality": "high", "regions": ["sub_saharan_africa", "south_asia"]},
        {"standard": "S3", "topic": "Land rights & indigenous community displacement",
         "materiality": "high", "regions": ["south_america", "sub_saharan_africa"]},
        {"standard": "S2", "topic": "Forced labour & debt bondage in supply chain",
         "materiality": "high", "regions": ["south_asia"]},
        {"standard": "S1", "topic": "Seasonal worker health & safety compliance",
         "materiality": "medium"},
    ],
    "construction": [
        {"standard": "S1", "topic": "Migrant worker forced labour & wage compliance",
         "materiality": "high", "regions": ["eastern_europe", "middle_east"]},
        {"standard": "S1", "topic": "Occupational health & safety (falls, asbestos, silica)",
         "materiality": "high"},
        {"standard": "S2", "topic": "Building materials supply chain human rights",
         "materiality": "medium"},
        {"standard": "S3", "topic": "Community displacement & noise pollution",
         "materiality": "medium"},
    ],
    "manufacturing": [
        {"standard": "S1", "topic": "Occupational health & safety (machinery, chemicals)",
         "materiality": "high"},
        {"standard": "S1", "topic": "Gender pay gap & equal treatment",
         "materiality": "medium"},
        {"standard": "S2", "topic": "Tier 2-3 raw material supply chain HR risk",
         "materiality": "medium"},
        {"standard": "S4", "topic": "Product safety & recall management",
         "materiality": "medium"},
    ],
    "technology": [
        {"standard": "S1", "topic": "Diversity & inclusion in STEM workforce",
         "materiality": "high"},
        {"standard": "S2", "topic": "Conflict mineral sourcing (tin, tantalum, tungsten, gold)",
         "materiality": "high", "regions": ["sub_saharan_africa"]},
        {"standard": "S4", "topic": "Data privacy & consumer protection",
         "materiality": "high"},
        {"standard": "S1", "topic": "Work-life balance & remote work conditions",
         "materiality": "medium"},
    ],
    "finance": [
        {"standard": "S4", "topic": "Responsible lending & consumer protection",
         "materiality": "high"},
        {"standard": "S2", "topic": "Financed emissions & supply chain HR risk",
         "materiality": "medium"},
        {"standard": "S1", "topic": "Gender pay gap in financial services",
         "materiality": "high"},
        {"standard": "S4", "topic": "Digital inclusion & access to financial services",
         "materiality": "medium"},
    ],
}

DEFAULT_SOCIAL_TOPICS = [
    {"standard": "S1", "topic": "Working conditions & employee health & safety",
     "materiality": "high"},
    {"standard": "S1", "topic": "Diversity, equity & inclusion metrics",
     "materiality": "medium"},
    {"standard": "S2", "topic": "Value chain labour practices & human rights",
     "materiality": "medium"},
    {"standard": "S4", "topic": "Consumer health & safety",
     "materiality": "medium"},
    {"standard": "S3", "topic": "Community impact & engagement",
     "materiality": "low"},
]

# ─── ESRS S1-S4 Disclosure Requirements ───────────────────────────────
# ESRS 2023/2772, EFRAG IG 2024

S1_REQUIREMENTS: list[dict[str, Any]] = [
    {"id": "S1-1", "title": "Policies related to own workforce",
     "mandatory": True, "data_type": "narrative",
     "description": "Description of policies adopted to manage working conditions, equal treatment, and other own workforce matters."},
    {"id": "S1-2", "title": "Processes for engaging own workforce",
     "mandatory": True, "data_type": "narrative",
     "description": "Processes for engagement with own workforce and worker representatives on material impacts."},
    {"id": "S1-3", "title": "Remediation of negative impacts on own workforce",
     "mandatory": True, "data_type": "narrative",
     "description": "Grievance mechanisms and remediation processes for own workforce negative impacts."},
    {"id": "S1-4", "title": "Taking action on material impacts on own workforce",
     "mandatory": True, "data_type": "narrative",
     "description": "Actions taken to prevent, mitigate, or remediate material negative impacts on own workforce."},
    {"id": "S1-5", "title": "Targets related to managing material negative impacts",
     "mandatory": True, "data_type": "quantitative",
     "description": "Time-bound targets for managing material negative impacts on own workforce."},
    {"id": "S1-6", "title": "Characteristics of the undertaking's employees",
     "mandatory": True, "data_type": "quantitative",
     "description": "Employee headcount by gender, contract type, and region."},
    {"id": "S1-7", "title": "Non-employees in own workforce",
     "mandatory": True, "data_type": "quantitative",
     "description": "Number and characteristics of non-employees (agency, self-employed) in own workforce."},
    {"id": "S1-8", "title": "Collective bargaining coverage and social dialogue",
     "mandatory": True, "data_type": "quantitative",
     "description": "Percentage of total employees covered by collective bargaining agreements."},
    {"id": "S1-9", "title": "Diversity metrics",
     "mandatory": True, "data_type": "quantitative",
     "description": "Gender distribution at top management and age distribution across workforce."},
    {"id": "S1-10", "title": "Adequate wages",
     "mandatory": True, "data_type": "quantitative",
     "description": "Whether all employees are paid an adequate wage per reference benchmarks."},
    {"id": "S1-11", "title": "Social protection",
     "mandatory": True, "data_type": "quantitative",
     "description": "Percentage of employees covered by social protection through public programs or company benefits."},
    {"id": "S1-12", "title": "Persons with disabilities",
     "mandatory": False, "data_type": "quantitative",
     "description": "Percentage of employees with disabilities."},
    {"id": "S1-13", "title": "Training and skills development metrics",
     "mandatory": True, "data_type": "quantitative",
     "description": "Average training hours per employee, by gender and employee category."},
    {"id": "S1-14", "title": "Health and safety metrics",
     "mandatory": True, "data_type": "quantitative",
     "description": "Work-related injuries, fatalities, and days lost; coverage of OHS management system."},
    {"id": "S1-15", "title": "Work-life balance metrics",
     "mandatory": True, "data_type": "quantitative",
     "description": "Employees entitled to and taking family-related leave."},
    {"id": "S1-16", "title": "Remuneration metrics (gender pay gap)",
     "mandatory": True, "data_type": "quantitative",
     "description": "Gender pay gap (mean and median) and ratio of CEO-to-median employee compensation."},
    {"id": "S1-17", "title": "Incidents, complaints and severe human rights impacts",
     "mandatory": True, "data_type": "quantitative",
     "description": "Number of work-related incidents, complaints, and severe human rights impacts; fines."},
]

S2_REQUIREMENTS: list[dict[str, Any]] = [
    {"id": "S2-1", "title": "Policies related to value chain workers",
     "mandatory": True, "data_type": "narrative",
     "description": "Policies addressing human rights and labour practices in the value chain."},
    {"id": "S2-2", "title": "Processes for engaging value chain workers",
     "mandatory": True, "data_type": "narrative",
     "description": "Engagement processes with value chain workers on material impacts."},
    {"id": "S2-3", "title": "Remediation of negative impacts on value chain workers",
     "mandatory": True, "data_type": "narrative",
     "description": "Grievance mechanisms and remediation processes for value chain workers."},
    {"id": "S2-4", "title": "Taking action on material impacts on value chain workers",
     "mandatory": True, "data_type": "narrative",
     "description": "Actions to prevent, mitigate, or remediate material negative impacts on value chain workers."},
    {"id": "S2-5", "title": "Targets related to value chain workers",
     "mandatory": True, "data_type": "quantitative",
     "description": "Time-bound targets for managing material negative impacts on value chain workers."},
]

S3_REQUIREMENTS: list[dict[str, Any]] = [
    {"id": "S3-1", "title": "Policies related to affected communities",
     "mandatory": True, "data_type": "narrative",
     "description": "Policies addressing impacts on local and indigenous communities."},
    {"id": "S3-2", "title": "Processes for engaging affected communities",
     "mandatory": True, "data_type": "narrative",
     "description": "Engagement processes with affected communities on material impacts."},
    {"id": "S3-3", "title": "Remediation of negative impacts on affected communities",
     "mandatory": True, "data_type": "narrative",
     "description": "Grievance mechanisms and remediation for affected communities."},
    {"id": "S3-4", "title": "Taking action on material impacts on affected communities",
     "mandatory": True, "data_type": "narrative",
     "description": "Actions to prevent, mitigate, or remediate material negative impacts on affected communities."},
    {"id": "S3-5", "title": "Targets related to affected communities",
     "mandatory": True, "data_type": "quantitative",
     "description": "Time-bound targets for managing material negative impacts on affected communities."},
]

S4_REQUIREMENTS: list[dict[str, Any]] = [
    {"id": "S4-1", "title": "Policies related to consumers and end-users",
     "mandatory": True, "data_type": "narrative",
     "description": "Policies addressing health & safety, privacy, and inclusion of consumers."},
    {"id": "S4-2", "title": "Processes for engaging consumers and end-users",
     "mandatory": True, "data_type": "narrative",
     "description": "Engagement processes with consumers on material impacts."},
    {"id": "S4-3", "title": "Remediation of negative impacts on consumers and end-users",
     "mandatory": True, "data_type": "narrative",
     "description": "Grievance mechanisms and remediation for consumers and end-users."},
    {"id": "S4-4", "title": "Taking action on material impacts on consumers and end-users",
     "mandatory": True, "data_type": "narrative",
     "description": "Actions to prevent, mitigate, or remediate material negative impacts on consumers."},
    {"id": "S4-5", "title": "Targets related to consumers and end-users",
     "mandatory": True, "data_type": "quantitative",
     "description": "Time-bound targets for managing material negative impacts on consumers."},
]

# ─── UN Guiding Principles / CSDDD Due Diligence Framework ────────────

UNGP_DD_STEPS: list[dict[str, Any]] = [
    {"step": 1, "phase": "Policy commitment",
     "actions": ["Adopt human rights policy statement approved at board level",
                  "Align with UN Guiding Principles, ILO Core Conventions, OECD Guidelines",
                  "Publicly communicate policy to workers, suppliers, and stakeholders"],
     "esrs": ["S1-1", "S2-1", "S3-1", "S4-1"]},
    {"step": 2, "phase": "Human rights due diligence assessment",
     "actions": ["Map all operations and value chain for HR impacts",
                  "Identify salient human rights risks per region and sector",
                  "Prioritise risks based on severity, scale, remediability, likelihood",
                  "Conduct HRIA for high-risk contexts"],
     "esrs": ["S1-2", "S2-2", "S3-2", "S1-6"]},
    {"step": 3, "phase": "Integration and action",
     "actions": ["Assign responsibility for HR DD at board/management level",
                  "Integrate findings into business processes and supplier contracts",
                  "Develop prevention and mitigation action plans per salient risk",
                  "Provide training to relevant staff and business partners"],
     "esrs": ["S1-4", "S2-4", "S3-4", "S4-4"]},
    {"step": 4, "phase": "Tracking effectiveness",
     "actions": ["Define KPIs for each salient HR risk",
                  "Conduct periodic impact assessments and supplier audits",
                  "Monitor grievance mechanism outcomes and trends",
                  "Report progress internally and externally"],
     "esrs": ["S1-5", "S2-5", "S3-5", "S4-5"]},
    {"step": 5, "phase": "Remediation and grievance",
     "actions": ["Establish legitimate grievance mechanisms for all affected stakeholders",
                  "Provide or cooperate in remediation of adverse impacts",
                  "Engage stakeholders on remediation outcomes",
                  "Track and disclose remediation effectiveness"],
     "esrs": ["S1-3", "S2-3", "S3-3", "S4-3"]},
    {"step": 6, "phase": "Communication and reporting",
     "actions": ["Disclose HR DD process and outcomes in annual report",
                  "Align disclosures with ESRS S1-S4 and CSDDD Art.11",
                  "Communicate remediation outcomes to affected stakeholders",
                  "Publish HR DD statement on company website"],
     "esrs": ["S1-17", "S1-1", "S2-1", "S3-1"]},
]


def _benchmark(sector: str, table: dict, default: dict) -> dict:
    return table.get(sector.lower(), default)


def _benchmark_by_country(sector: str, table: dict, default: dict,
                          country: str) -> float:
    b = table.get(sector.lower(), default)
    return b.get(country.lower(), b.get("eu_mean", 15.0))


def _score_metric(value: float, thresholds: list[float], reverse: bool = False) -> int:
    """Score 1-5 based on threshold brackets. If reverse=True, higher values = lower scores."""
    for i, t in enumerate(thresholds):
        if (value <= t and not reverse) or (value >= t and reverse):
            return i + 1
    return 5


def _avg_risk(scores: list[dict], key: str = "child_labour") -> int:
    if not scores:
        return 1
    vals = [s.get(key, 3) for s in scores]
    return max(1, min(5, round(sum(vals) / len(vals))))


def _region_hr_risk(regions: list[str]) -> list[dict]:
    return [{"region": r, "label": REGION_LABELS.get(r, r),
             **REGION_HR_RISK.get(r, {"child_labour": 3, "forced_labour": 3, "association": 3,
                                      "discrimination": 3, "occupational_safety": 3, "wage": 3})}
            for r in regions]


# ─── Public API ────────────────────────────────────────────────────────


async def get_social_sustainability_score(
    sector: str,
    employee_count: int = 1000,
    turnover_rate: float = 15.0,
    injury_rate: float = 3.0,
    gender_pay_gap: float = 13.0,
) -> dict[str, Any]:
    """ESRS S1 (Own Workforce) composite score.
    
    Scores 1-5 per dimension:
    - S1-6: Employee characteristics (tenure/stability via turnover)
    - S1-9: Diversity (via gender pay gap proxy)
    - S1-14: Health & safety (via injury rate)
    - S1-16: Pay equity (via gender pay gap)
    
    Returns composite with dimension breakdown and benchmarking.
    """
    cache = get_cache()
    ck = cache.make_key("social_score", sector, str(employee_count),
                        str(turnover_rate), str(injury_rate), str(gender_pay_gap))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    tb = _benchmark(s, TURNOVER_BENCHMARKS, DEFAULT_TURNOVER)
    ib = _benchmark(s, INJURY_BENCHMARKS, DEFAULT_INJURY)
    pb = _benchmark(s, PAYGAP_BENCHMARKS, DEFAULT_PAYGAP)

    # S1-6: Low turnover = high score (reverse scoring)
    turnover_thresholds = [8.0, 15.0, 22.0, 30.0]
    s1_6_score = _score_metric(turnover_rate, turnover_thresholds, reverse=True)

    # S1-14: Low injury rate = high score
    injury_thresholds = [1.0, 3.0, 6.0, 10.0]
    s1_14_score = _score_metric(injury_rate, injury_thresholds, reverse=True)

    # S1-16: Low pay gap = high score
    paygap_thresholds = [5.0, 10.0, 18.0, 25.0]
    s1_16_score = _score_metric(gender_pay_gap, paygap_thresholds, reverse=True)

    # S1-9: Diversity score derived from pay gap + employee count adjustment
    # Larger companies (>5000) get a slight deduction for complexity
    diversity_score = max(1, s1_16_score - (1 if employee_count > 5000 else 0))
    diversity_score = min(5, max(1, diversity_score))

    composite = round((s1_6_score + s1_14_score + s1_16_score + diversity_score) / 4)

    results = {
        "overview": {
            "sector": s, "employees": employee_count,
            "composite_score": composite,
            "composite_label": risk_label(composite),
            "composite_color": risk_color(composite),
        },
        "dimensions": {
            "S1-6_employee_characteristics": {
                "score": s1_6_score, "turnover_rate_pct": turnover_rate,
                "industry_mean": tb["eu_mean"], "label": risk_label(s1_6_score),
                "color": risk_color(s1_6_score),
            },
            "S1-9_diversity": {
                "score": diversity_score, "label": risk_label(diversity_score),
                "color": risk_color(diversity_score),
                "note": "Derived from pay gap and company size complexity" if employee_count > 5000 else "Derived from pay gap proxy",
            },
            "S1-14_health_and_safety": {
                "score": s1_14_score, "injury_rate_per_100": injury_rate,
                "industry_mean": ib["eu_mean"], "label": risk_label(s1_14_score),
                "color": risk_color(s1_14_score),
            },
            "S1-16_remuneration_pay_gap": {
                "score": s1_16_score, "gender_pay_gap_pct": gender_pay_gap,
                "industry_mean": pb["eu_mean"], "label": risk_label(s1_16_score),
                "color": risk_color(s1_16_score),
            },
        },
        "benchmarking": {
            "sector": s,
            "turnover": {"your_value": turnover_rate, "industry_mean": tb["eu_mean"],
                         "top_quartile": tb["top_quartile"],
                         "bottom_quartile": tb["bottom_quartile"]},
            "injury_rate": {"your_value": injury_rate, "industry_mean": ib["eu_mean"],
                            "top_quartile": ib["top_quartile"],
                            "bottom_quartile": ib["bottom_quartile"]},
            "gender_pay_gap": {"your_value": gender_pay_gap, "industry_mean": pb["eu_mean"],
                               "top_quartile": pb["top_quartile"],
                               "bottom_quartile": pb["bottom_quartile"]},
        },
        "recommendations": [
            *([f"High turnover ({turnover_rate}% vs industry {tb['eu_mean']}%) — investigate root causes and implement retention strategy"]
              if s1_6_score <= 2 else []),
            *([f"Injury rate ({injury_rate}) exceeds industry average ({ib['eu_mean']}) — strengthen OHS management system"]
              if s1_14_score <= 2 else []),
            *([f"Gender pay gap ({gender_pay_gap}%) above sector average ({pb['eu_mean']}%) — conduct pay equity audit"]
              if s1_16_score <= 2 else []),
            "Disclose all S1-6, S1-9, S1-14, S1-16 quantitative metrics in CSRD report",
            "Set time-bound targets for improvement per S1-5",
        ],
        "sources": "ESRS S1 (EU 2023/2772); Eurostat 2024; EU-OSHA 2024; EIGE 2024",
        "disclaimer": f"Generated {today_iso()}. Scores are indicative — validate with internal data.",
    }
    cache.set(ck, results, "csrd", 14)
    return results


async def get_workforce_benchmarks(
    sector: str,
    region: str = "EU",
) -> dict[str, Any]:
    """Industry benchmarks for workforce metrics by sector and country.
    
    Returns turnover rates, injury rates, and gender pay gap with
    top vs bottom quartile comparisons.
    """
    cache = get_cache()
    ck = cache.make_key("social_bench", sector, region)
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    country = region.lower()

    tb = _benchmark(s, TURNOVER_BENCHMARKS, DEFAULT_TURNOVER)
    ib = _benchmark(s, INJURY_BENCHMARKS, DEFAULT_INJURY)
    pb = _benchmark(s, PAYGAP_BENCHMARKS, DEFAULT_PAYGAP)

    # Get country-specific value if available
    country_turnover = tb.get(country, tb["eu_mean"])
    country_injury = ib.get(country, ib["eu_mean"])
    country_paygap = pb.get(country, pb["eu_mean"])

    available_countries = [k for k in tb.keys() if k not in
                           ("eu_mean", "top_quartile", "bottom_quartile")]

    def quartile_position(value, mean, top, bottom):
        if value <= top:
            return "top_quartile — best-in-class"
        elif value <= mean:
            return "above_average"
        elif value <= bottom:
            return "below_average"
        return "bottom_quartile — needs improvement"

    results = {
        "sector": s,
        "benchmarks": {
            "turnover_rate": {
                "eu_mean": tb["eu_mean"], "top_quartile": tb["top_quartile"],
                "bottom_quartile": tb["bottom_quartile"],
                f"{country}_value" if country != "eu" else "eu_mean": country_turnover,
                "your_position": quartile_position(country_turnover, tb["eu_mean"],
                                                    tb["top_quartile"], tb["bottom_quartile"]),
                "unit": "%",
            },
            "injury_rate": {
                "eu_mean": ib["eu_mean"], "top_quartile": ib["top_quartile"],
                "bottom_quartile": ib["bottom_quartile"],
                f"{country}_value" if country != "eu" else "eu_mean": country_injury,
                "your_position": quartile_position(country_injury, ib["eu_mean"],
                                                    ib["top_quartile"], ib["bottom_quartile"]),
                "unit": "per 100 workers",
            },
            "gender_pay_gap": {
                "eu_mean": pb["eu_mean"], "top_quartile": pb["top_quartile"],
                "bottom_quartile": pb["bottom_quartile"],
                f"{country}_value" if country != "eu" else "eu_mean": country_paygap,
                "your_position": quartile_position(country_paygap, pb["eu_mean"],
                                                    pb["top_quartile"], pb["bottom_quartile"]),
                "unit": "% (mean unadjusted)",
            },
        },
        "available_countries": available_countries,
        "data_sources": "Eurostat Labour Force Survey 2024; EU-OSHA ESAW 2024; EIGE 2024",
        "note": "Benchmarks reflect EU-based industry averages. Country values are national statistical estimates.",
    }
    cache.set(ck, results, "csrd", 14)
    return results


async def get_social_value_chain_risk(
    sector: str,
    regions: list[str],
) -> dict[str, Any]:
    """ESRS S2 (Value Chain Workers), S3 (Affected Communities),
    S4 (Consumers & End-Users) risk assessment.
    
    Region-based scoring for supply chain human rights and
    sector-specific material topics.
    """
    cache = get_cache()
    ck = cache.make_key("social_vc_risk", sector, *sorted(regions or ["?"]))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    reg_scores = _region_hr_risk(regions)

    # ESRS S2: Value chain workers risk
    s2_child = _avg_risk(reg_scores, "child_labour")
    s2_forced = _avg_risk(reg_scores, "forced_labour")
    s2_assoc = _avg_risk(reg_scores, "association")
    s2_wage = _avg_risk(reg_scores, "wage")
    s2_overall = max(1, min(5, round((s2_child + s2_forced + s2_assoc + s2_wage) / 4)))

    # ESRS S3: Affected communities risk
    # Communities risk is weighted more by discrimination + forced labour
    s3_disc = _avg_risk(reg_scores, "discrimination")
    s3_overall = max(1, min(5, round((s2_forced + s3_disc + s2_child) / 3)))

    # ESRS S4: Consumers & end-users risk
    # Lower general risk, adjusted by sectors with high product safety concern
    high_consumer_risk = s in ("textiles", "chemicals", "food_processing", "technology")
    s4_overall = min(5, s3_disc + (1 if high_consumer_risk else 0))

    # Sector material topics
    topics = SECTOR_SOCIAL_TOPICS.get(s, DEFAULT_SOCIAL_TOPICS)

    results = {
        "sector": s,
        "regions_assessed": reg_scores,
        "risk_scores": {
            "S2_value_chain_workers": {
                "overall": s2_overall, "label": risk_label(s2_overall),
                "color": risk_color(s2_overall),
                "dimensions": {
                    "child_labour": s2_child, "forced_labour": s2_forced,
                    "freedom_of_association": s2_assoc, "wage_fairness": s2_wage,
                },
                "esrs": [get_esrs_ref("S2")],
            },
            "S3_affected_communities": {
                "overall": s3_overall, "label": risk_label(s3_overall),
                "color": risk_color(s3_overall),
                "dimensions": {
                    "land_rights": min(5, s3_disc + 1),
                    "indigenous_rights": min(5, s2_forced + 1),
                    "discrimination": s3_disc,
                },
                "esrs": [get_esrs_ref("S3")],
            },
            "S4_consumers_end_users": {
                "overall": s4_overall, "label": risk_label(s4_overall),
                "color": risk_color(s4_overall),
                "dimensions": {
                    "product_safety": min(5, s4_overall),
                    "privacy": min(5, s4_overall - 1 if not high_consumer_risk else s4_overall),
                    "accessibility": min(5, max(1, s4_overall - 1)),
                },
                "esrs": [get_esrs_ref("S4")],
            },
        },
        "sector_material_topics": [
            {"standard": t["standard"], "topic": t["topic"],
             "materiality": t["materiality"],
             "regions": t.get("regions", ["all"])}
            for t in topics
        ],
        "recommendations": [
            *(["CRITICAL: Conduct supply chain HRIA — high child/forced labour risk"]
              if s2_overall >= 4 else []),
            *(["Strengthen supplier auditing programme in high-risk regions"]
              if s2_overall >= 3 else []),
            *(["Engage affected communities with FPIC process"]
              if s3_overall >= 3 else []),
            *(["Implement product safety monitoring and consumer grievance mechanism"]
              if s4_overall >= 3 else []),
            "Disclose material social impacts per ESRS IRO-1",
            "Integrate S2-S4 findings into CSDDD due diligence process",
        ],
        "sources": "ESRS S2-S4 (EU 2023/2772); ILO 2024; HRW 2025; ITUC GR 2025; US DOL List 2024",
        "disclaimer": f"Generated {today_iso()}. Indicative risk assessment — validate with local context.",
    }
    cache.set(ck, results, "csrd", 14)
    return results


async def get_human_rights_due_diligence(
    sector: str,
    regions: list[str],
    has_policy: bool = True,
) -> dict[str, Any]:
    """Human rights due diligence framework aligned with UN Guiding
    Principles and CSDDD (EU 2024/1760).
    
    Includes alignment check, grievance mechanism assessment,
    and remediation plan template.
    """
    cache = get_cache()
    ck = cache.make_key("social_hrdd", sector, *sorted(regions or ["?"]), str(has_policy))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    reg_scores = _region_hr_risk(regions)

    hr_overall = _avg_risk(reg_scores, "child_labour")
    hr_forced = _avg_risk(reg_scores, "forced_labour")

    # UNGP compliance check
    ungp_statuses = []
    for step in UNGP_DD_STEPS:
        pct = max(50, 100 - (step["step"] * 10 * (hr_overall - 1)))
        if hr_overall >= 4:
            pct = max(30, pct - 20)
        if has_policy and step["step"] == 1:
            pct = min(100, pct + 20)
        ungp_statuses.append({
            "step": step["step"], "phase": step["phase"],
            "compliance_pct": min(100, pct),
            "actions_required": step["actions"],
            "esrs": [get_esrs_ref(e) for e in step["esrs"]],
        })

    overall_ungp = round(sum(s["compliance_pct"] for s in ungp_statuses) / len(ungp_statuses))
    ungp_label = "Aligned" if overall_ungp >= 80 else "Partially aligned" if overall_ungp >= 50 else "Non-compliant"

    # CSDDD cross-reference
    high_risk_sectors = ["textiles", "agriculture", "construction", "minerals",
                         "metal_ore_mining", "coal_mining", "petroleum"]
    is_high_sector = s in high_risk_sectors

    # Grievance mechanism assessment
    grievance_assessment = {
        "legitimate": has_policy,
        "accessible": len(regions) <= 3,
        "predictable": has_policy,
        "equitable": hr_overall <= 3,
        "transparent": True,
        "rights_compatible": has_policy,
        "source_of_learning": has_policy,
        "score": round((sum([1 for v in [has_policy, len(regions) <= 3, has_policy,
                                          hr_overall <= 3, True, has_policy, has_policy]])
                        / 7) * 100),
    }

    # Remediation plan template
    remediation_plan = {
        "phase_1_assessment": {
            "timeline": "0-3 months",
            "actions": ["Conduct HRIA in high-risk supply chain regions",
                        "Map all Tier 1-3 suppliers",
                        "Prioritise salient human rights risks by severity",
                        "Stakeholder mapping and initial engagement"],
        },
        "phase_2_prevention": {
            "timeline": "3-9 months",
            "actions": ["Adopt Supplier Code of Conduct aligned with ILO core conventions",
                        "Contractual cascading of HR requirements to Tier 1 suppliers",
                        "HR DD training for procurement and supplier management teams",
                        "Establish risk-based supplier auditing schedule"],
        },
        "phase_3_remediation": {
            "timeline": "6-18 months",
            "actions": ["Establish or strengthen operational-level grievance mechanism",
                        "Define remediation protocol per impact type",
                        "Allocate remediation budget and responsibility",
                        "Engage affected stakeholders on remediation design"],
        },
        "phase_4_monitoring": {
            "timeline": "12-24 months",
            "actions": ["Quarterly tracking of HR KPIs and grievance trends",
                        "Annual independent audit of HR DD effectiveness",
                        "Public disclosure of HR DD outcomes per ESRS S1-17",
                        "Stakeholder feedback loop for continuous improvement"],
        },
    }

    results = {
        "company": {"sector": s, "has_hr_policy": has_policy,
                    "high_risk_sector": is_high_sector,
                    "regions_assessed": reg_scores},
        "risk_summary": {
            "overall_human_rights_risk": hr_overall,
            "label": risk_label(hr_overall), "color": risk_color(hr_overall),
            "highest_risk": "forced_labour" if hr_forced >= hr_overall else "child_labour",
        },
        "ungp_alignment": {
            "overall_pct": overall_ungp,
            "status": ungp_label,
            "steps": ungp_statuses,
        },
        "csddd_cross_reference": {
            "applicable": is_high_sector,
            "note": "CSDDD (EU 2024/1760) mandates full DD for high-risk sectors "
                    "and companies >€150M revenue",
            "thresholds": "Wave 1 (2027): 5000+ emp, €1500M+; Wave 2 (2028): 3000+ emp, €900M+; "
                          "Wave 3 (2029): 500+ emp, €150M+",
            "esrs_overlap": [get_esrs_ref("S1"), get_esrs_ref("S2"), get_esrs_ref("S3")],
        },
        "grievance_mechanism": grievance_assessment,
        "remediation_plan": remediation_plan,
        "recommendations": [
            *(["URGENT: Adopt board-approved human rights policy"] if not has_policy else []),
            *(["Priority: Conduct supply chain HRIA in high-risk regions"]
              if hr_overall >= 4 else []),
            "Align all DD processes with UNGP Framework (6 steps)",
            "Integrate HR DD outcomes into CSRD/ESRS S1-S4 disclosures",
            "Establish multi-channel grievance mechanism accessible to all stakeholders",
            "Report HR DD annually per CSDDD Art.11 (if applicable)",
        ],
        "sources": "UN Guiding Principles on Business and Human Rights 2011; "
                    "CSDDD (EU 2024/1760); OECD Guidelines 2023; ILO 2024; HRW 2025",
        "disclaimer": f"Generated {today_iso()}. Indicative — consult legal counsel for compliance.",
    }
    cache.set(ck, results, "csrd", 14)
    return results


async def get_esrs_social_disclosure_template(
    sector: str,
    entity_type: str = "public_interest_entity",
    employees: int = 1000,
) -> dict[str, Any]:
    """Full S1-S4 disclosure checklist with data points.
    
    Identifies which standards are mandatory vs voluntary,
    sector-specific material topics, and data collection requirements.
    """
    cache = get_cache()
    ck = cache.make_key("social_template", sector, entity_type, str(employees))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()

    # Determine scope: All CSRD entities must do S1; S2-S4 depends on materiality
    is_large = employees >= 250
    is_sme_listed = entity_type == "sme_listed"
    all_mandatory = is_large and not is_sme_listed
    s1_mandatory = all_mandatory or entity_type in ("public_interest_entity", "large")

    # Sector-specific material topics
    topics = SECTOR_SOCIAL_TOPICS.get(s, DEFAULT_SOCIAL_TOPICS)
    material_standards = set(t["standard"] for t in topics)

    def build_standard_block(reqs: list[dict], std_key: str) -> list[dict]:
        mandatory_default = std_key in ("S1",) and s1_mandatory
        return [
            {
                "id": r["id"], "title": r["title"],
                "mandatory": mandatory_default and r["mandatory"],
                "material": std_key in material_standards,
                "data_type": r["data_type"],
                "description": r["description"],
                "data_collection": _data_collection_guidance(r["id"]),
            }
            for r in reqs
        ]

    s1_items = build_standard_block(S1_REQUIREMENTS, "S1")
    s2_items = build_standard_block(S2_REQUIREMENTS, "S2")
    s3_items = build_standard_block(S3_REQUIREMENTS, "S3")
    s4_items = build_standard_block(S4_REQUIREMENTS, "S4")

    results = {
        "company_profile": {
            "sector": s, "entity_type": entity_type,
            "employees": employees, "large_entity": is_large,
            "csrd_in_scope": is_large or is_sme_listed,
        },
        "scope_assessment": {
            "S1_own_workforce": {
                "mandatory": s1_mandatory,
                "note": "Mandatory for all CSRD in-scope entities regardless of materiality",
            },
            "S2_value_chain_workers": {
                "mandatory": all_mandatory and "S2" in material_standards,
                "note": "Mandatory if material based on double materiality assessment",
            },
            "S3_affected_communities": {
                "mandatory": all_mandatory and "S3" in material_standards,
                "note": "Mandatory if material based on double materiality assessment",
            },
            "S4_consumers_end_users": {
                "mandatory": all_mandatory and "S4" in material_standards,
                "note": "Mandatory if material based on double materiality assessment",
            },
        },
        "disclosure_checklist": {
            "S1_own_workforce": {
                "requirements": s1_items,
                "total": len(s1_items),
                "mandatory_count": sum(1 for i in s1_items if i["mandatory"]),
                "data_points_required": 35,
            },
            "S2_value_chain_workers": {
                "requirements": s2_items,
                "total": len(s2_items),
                "mandatory_count": sum(1 for i in s2_items if i["mandatory"]),
                "data_points_required": 10,
            },
            "S3_affected_communities": {
                "requirements": s3_items,
                "total": len(s3_items),
                "mandatory_count": sum(1 for i in s3_items if i["mandatory"]),
                "data_points_required": 8,
            },
            "S4_consumers_end_users": {
                "requirements": s4_items,
                "total": len(s4_items),
                "mandatory_count": sum(1 for i in s4_items if i["mandatory"]),
                "data_points_required": 8,
            },
        },
        "sector_material_topics": topics,
        "summary": {
            "total_requirements": 32,
            "applicable": sum(1 for std in [s1_items, s2_items, s3_items, s4_items]
                              for i in std if i["mandatory"]),
            "estimated_data_points": 61,
            "s1_standalone_mandatory": s1_mandatory,
        },
        "data_collection_notes": [
            "S1-6: Extract headcount data from HRIS by gender, contract type, region (EEA vs non-EEA)",
            "S1-8: Collect collective bargaining coverage from HR/payroll systems",
            "S1-9: Obtain gender distribution of top management from governance records",
            "S1-14: Aggregate injury data from OHS incident reporting system",
            "S1-16: Calculate gender pay gap using EC methodology (mean and median)",
            "S2-S4: Supply chain data requires supplier surveys, audits, and HRIA reports",
            "Engage stakeholders for materiality assessment per IRO-1 process",
        ],
        "sources": "ESRS S1-S4 (EU 2023/2772); EFRAG IG 2024; EFRAG ER 2024",
        "disclaimer": f"Generated {today_iso()}. Disclosure requirements depend on materiality assessment. Consult EFRAG guidance.",
    }
    cache.set(ck, results, "csrd", 14)
    return results


def _data_collection_guidance(req_id: str) -> dict:
    """Per-requirement data collection guidance."""
    guidance = {
        "S1-1": {"source": "Policy documents", "format": "PDF/narrative", "owner": "HR/Board"},
        "S1-2": {"source": "Process documentation", "format": "Narrative", "owner": "HR/IR"},
        "S1-3": {"source": "Grievance records", "format": "Narrative+stats", "owner": "HR/Legal"},
        "S1-4": {"source": "Action plans", "format": "Narrative", "owner": "HR/Operations"},
        "S1-5": {"source": "Target documents", "format": "Quantitative", "owner": "HR/Strategy"},
        "S1-6": {"source": "HRIS / payroll data", "format": "Table (FTE by gender/contract/region)",
                 "owner": "HR/IT"},
        "S1-7": {"source": "HRIS / procurement data", "format": "Table", "owner": "HR/Procurement"},
        "S1-8": {"source": "HR / union records", "format": "Percentage", "owner": "HR/IR"},
        "S1-9": {"source": "Workforce demographics", "format": "Table (% by gender, age group)",
                 "owner": "HR/D&I"},
        "S1-10": {"source": "Payroll + benchmark", "format": "Yes/No + narrative",
                  "owner": "HR/Compensation"},
        "S1-11": {"source": "Benefits records", "format": "Percentage", "owner": "HR/Benefits"},
        "S1-12": {"source": "HR / D&I data", "format": "Percentage", "owner": "HR/D&I"},
        "S1-13": {"source": "LMS / training records", "format": "Hours by gender/category",
                  "owner": "HR/L&D"},
        "S1-14": {"source": "OHS incident system", "format": "Rate per 100 workers, fatalities",
                  "owner": "HSE"},
        "S1-15": {"source": "Leave records", "format": "% entitled vs % taken", "owner": "HR"},
        "S1-16": {"source": "Payroll data", "format": "Mean and median gap (%)",
                  "owner": "HR/Compensation"},
        "S1-17": {"source": "Incident register + legal", "format": "Count + fines",
                  "owner": "HR/Legal/HSE"},
    }
    return guidance.get(req_id, {"source": "Various", "format": "Mixed", "owner": "Cross-functional"})
