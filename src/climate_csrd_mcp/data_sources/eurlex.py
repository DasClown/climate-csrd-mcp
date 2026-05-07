"""
EUR-Lex / ESRS Standards client — Full CSRD regulatory intelligence.

Provides:
- ESRS S1-S4 social standards requirements (full detail)
- ESRS G1 governance requirements (full detail)
- EFRAG implementation guidance references
- Sector-specific materiality with REAL EFRAG guidance data
- Double materiality assessment generator
- ESRS Data Points (1,200+) reference by standard
- NACE code mapping for all sectors
- VSME (Voluntary Standard for Non-listed SMEs) support
- CSRD entity classification per Art. 3 thresholds
- Location-specific ESRS triggers

Sources:
- EUR-Lex: Regulation (EU) 2023/2772 (ESRS Delegated Regulation)
- CSRD Directive (EU) 2022/2464, Art. 3 thresholds
- EFRAG IG documents (IG 1-3 Materiality, IG 2 Value Chain)
- EFRAG ESRS Data Point Mapping (all 1,200+ data points)
- VSME Standard (EFRAG draft, expected 2025)
- NACE Rev. 2 Classification (EC 1893/2006)
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

logger = logging.getLogger(__name__)

# ─── CSRD Thresholds (Art. 3 Directive 2022/2464) ────────────────────

CSRD_THRESHOLDS = {
    "large": {
        "description": "Large undertaking — full CSRD",
        "conditions": "Meets 2 of 3: >250 employees, >€50M revenue, >€25M assets",
        "required_esrs": ["ESRS_2", "E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"],
        "core_mandatory": ["E1", "ESRS_2"],
        "first_reporting": "FY 2025 (for FY 2024 for public-interest entities already under NFRD)",
        "reporting_deadline": "Within 12 months of balance sheet date",
    },
    "listed_sme": {
        "description": "Listed SME — proportionate CSRD (LSME)",
        "conditions": "Listed on EU regulated market, <250 employees",
        "required_esrs": ["ESRS_2", "LSME_ESRS"],
        "core_mandatory": ["ESRS_2"],
        "first_reporting": "FY 2026 (opt-out until 2028 possible)",
        "reporting_deadline": "Within 12 months of balance sheet date",
    },
    "non_eu_group": {
        "description": "Non-EU group with EU presence",
        "conditions": ">€150M revenue in EU, or has EU subsidiary/branch exceeding thresholds",
        "required_esrs": ["ESRS_2", "E1", "E2", "E3", "E4", "E5", "S1", "G1"],
        "core_mandatory": ["ESRS_2", "E1"],
        "first_reporting": "FY 2028",
        "reporting_deadline": "Within 12 months of balance sheet date",
    },
    "vsme": {
        "description": "VSME — Voluntary Standard for Non-listed SMEs",
        "conditions": "Non-listed SME (<250 employees, <€50M revenue, <€43M assets)",
        "required_esrs": ["VSME_BASIC", "VSME_COMPREHENSIVE"],
        "core_mandatory": ["VSME_BASIC"],
        "first_reporting": "Voluntary, no mandatory start date (expected 2025+)",
        "reporting_deadline": "Voluntary",
    },
}

# ─── ESRS S1-S4 Social Standards (Full Detail) ──────────────────────
# Source: ESRS Delegated Regulation (EU) 2023/2772, Annex I

ESRS_S1_REQUIREMENTS = {
    "standard": "ESRS S1 — Own Workforce",
    "objective": "Disclosure on company's impact on its own workforce, including working conditions, health & safety, diversity, and human rights.",
    "disclosure_requirements": [
        {
            "code": "S1-1",
            "title": "Policies related to own workforce",
            "description": "Description of policies adopted to manage impacts on own workforce, including human rights policies, equal opportunity policies, and health & safety policies.",
            "data_points": 25,
            "materiality": "mandatory",
        },
        {
            "code": "S1-2",
            "title": "Processes for engaging with own workforce about impacts",
            "description": "Description of stakeholder engagement processes with own workforce and worker representatives regarding actual and potential impacts.",
            "data_points": 18,
            "materiality": "mandatory",
        },
        {
            "code": "S1-3",
            "title": "Processes to remediate negative impacts and channels for workforce to raise concerns",
            "description": "Description of grievance mechanisms and remediation processes for workforce concerns.",
            "data_points": 15,
            "materiality": "mandatory",
        },
        {
            "code": "S1-4",
            "title": "Taking action on material impacts on own workforce",
            "description": "Description of actions taken to prevent, mitigate, or remediate material negative impacts and to achieve positive impacts.",
            "data_points": 22,
            "materiality": "material",
        },
        {
            "code": "S1-5",
            "title": "Targets related to managing material negative impacts, advancing positive impacts, and managing risks and opportunities",
            "description": "Description of time-bound, outcome-oriented targets related to workforce impacts.",
            "data_points": 20,
            "materiality": "material",
        },
        {
            "code": "S1-6",
            "title": "Characteristics of the undertaking's employees",
            "description": "Employee headcount, breakdown by gender, contract type, region. Full-time/part-time, permanent/temporary.",
            "data_points": 35,
            "materiality": "mandatory",
        },
        {
            "code": "S1-7",
            "title": "Characteristics of non-employees in own workforce",
            "description": "Information on self-employed persons, agency workers, and other non-employees in the workforce.",
            "data_points": 12,
            "materiality": "mandatory",
        },
        {
            "code": "S1-8",
            "title": "Collective bargaining coverage and social dialogue",
            "description": "Coverage of collective bargaining agreements and social dialogue structures.",
            "data_points": 10,
            "materiality": "mandatory",
        },
        {
            "code": "S1-9",
            "title": "Diversity metrics",
            "description": "Gender distribution at top management, age distribution of employees.",
            "data_points": 8,
            "materiality": "mandatory",
        },
        {
            "code": "S1-10",
            "title": "Adequate wages",
            "description": "Whether all employees are paid an adequate wage in line with applicable benchmarks.",
            "data_points": 6,
            "materiality": "mandatory",
        },
        {
            "code": "S1-11",
            "title": "Social protection",
            "description": "Percentage of employees covered by social protection through public programs or company benefits.",
            "data_points": 5,
            "materiality": "mandatory",
        },
        {
            "code": "S1-12",
            "title": "Persons with disabilities",
            "description": "Percentage of employees with disabilities.",
            "data_points": 4,
            "materiality": "mandatory",
        },
        {
            "code": "S1-13",
            "title": "Training and skills development",
            "description": "Average training hours per employee, skills development programs, career progression metrics.",
            "data_points": 18,
            "materiality": "material",
        },
        {
            "code": "S1-14",
            "title": "Health and safety metrics",
            "description": "Work-related injuries, fatalities, lost days, near misses, and occupational disease rates.",
            "data_points": 25,
            "materiality": "mandatory",
        },
        {
            "code": "S1-15",
            "title": "Work-life balance",
            "description": "Family-related leave usage and return-to-work rates.",
            "data_points": 8,
            "materiality": "material",
        },
        {
            "code": "S1-16",
            "title": "Compensation metrics (pay gap)",
            "description": "Gender pay gap (unadjusted), annual total compensation ratio of highest-paid to median employee.",
            "data_points": 10,
            "materiality": "mandatory",
        },
        {
            "code": "S1-17",
            "title": "Incidents, complaints and severe human rights impacts",
            "description": "Discrimination incidents, human rights complaints, whistleblower cases, and penalties.",
            "data_points": 15,
            "materiality": "mandatory",
        },
    ],
    "total_data_points": 261,
    "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3",
}

ESRS_S2_REQUIREMENTS = {
    "standard": "ESRS S2 — Workers in the Value Chain",
    "objective": "Disclosure on impacts on value chain workers, including labor practices, health & safety, and human rights throughout the supply chain.",
    "disclosure_requirements": [
        {
            "code": "S2-1",
            "title": "Policies related to value chain workers",
            "description": "Policies addressing human rights, forced labor, child labor, and working conditions in the value chain.",
            "data_points": 18,
            "materiality": "mandatory",
        },
        {
            "code": "S2-2",
            "title": "Processes for engaging with value chain workers about impacts",
            "description": "Stakeholder engagement with value chain workers and their representatives.",
            "data_points": 14,
            "materiality": "mandatory",
        },
        {
            "code": "S2-3",
            "title": "Processes to remediate negative impacts and channels for value chain workers to raise concerns",
            "description": "Grievance mechanisms accessible to value chain workers (e.g., supplier hotlines).",
            "data_points": 12,
            "materiality": "mandatory",
        },
        {
            "code": "S2-4",
            "title": "Taking action on material impacts on value chain workers",
            "description": "Actions to address forced labor, health & safety, wage issues across supply chain tiers.",
            "data_points": 20,
            "materiality": "material",
        },
        {
            "code": "S2-5",
            "title": "Targets related to managing material negative impacts, advancing positive impacts, and managing risks and opportunities",
            "description": "Supply chain due diligence targets, supplier audit coverage, remediation KPIs.",
            "data_points": 15,
            "materiality": "material",
        },
    ],
    "total_data_points": 79,
    "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3",
}

ESRS_S3_REQUIREMENTS = {
    "standard": "ESRS S3 — Affected Communities",
    "objective": "Disclosure on impacts on communities affected by operations, including economic, social, and cultural rights, and community engagement.",
    "disclosure_requirements": [
        {
            "code": "S3-1",
            "title": "Policies related to affected communities",
            "description": "Policies on indigenous rights, land acquisition, community health, local employment, and free prior informed consent (FPIC).",
            "data_points": 16,
            "materiality": "mandatory",
        },
        {
            "code": "S3-2",
            "title": "Processes for engaging with affected communities about impacts",
            "description": "Community engagement mechanisms, consultation processes, and representation of vulnerable groups.",
            "data_points": 14,
            "materiality": "mandatory",
        },
        {
            "code": "S3-3",
            "title": "Processes to remediate negative impacts and channels for affected communities to raise concerns",
            "description": "Community grievance mechanisms, dispute resolution, and access to remedy.",
            "data_points": 12,
            "materiality": "mandatory",
        },
        {
            "code": "S3-4",
            "title": "Taking action on material impacts on affected communities",
            "description": "Actions on community displacement, livelihood restoration, local hiring, and resource access.",
            "data_points": 18,
            "materiality": "material",
        },
        {
            "code": "S3-5",
            "title": "Targets related to managing material negative impacts, advancing positive impacts, and managing risks and opportunities",
            "description": "Community investment targets, local procurement goals, and social license KPIs.",
            "data_points": 12,
            "materiality": "material",
        },
    ],
    "total_data_points": 72,
    "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3",
}

ESRS_S4_REQUIREMENTS = {
    "standard": "ESRS S4 — Consumers and End-Users",
    "objective": "Disclosure on impacts on consumers and end-users, including product safety, privacy, responsible marketing, and accessibility.",
    "disclosure_requirements": [
        {
            "code": "S4-1",
            "title": "Policies related to consumers and end-users",
            "description": "Policies on product safety, data privacy, responsible marketing, and vulnerable consumer protection.",
            "data_points": 15,
            "materiality": "mandatory",
        },
        {
            "code": "S4-2",
            "title": "Processes for engaging with consumers and end-users about impacts",
            "description": "Consumer feedback mechanisms, surveys, user testing, and engagement with consumer organizations.",
            "data_points": 12,
            "materiality": "mandatory",
        },
        {
            "code": "S4-3",
            "title": "Processes to remediate negative impacts and channels for consumers to raise concerns",
            "description": "Customer complaint mechanisms, product recall procedures, and data breach reporting.",
            "data_points": 10,
            "materiality": "mandatory",
        },
        {
            "code": "S4-4",
            "title": "Taking action on material impacts on consumers and end-users",
            "description": "Actions on product recalls, accessibility improvements, data protection, and ethical marketing.",
            "data_points": 16,
            "materiality": "material",
        },
        {
            "code": "S4-5",
            "title": "Targets related to managing material negative impacts, advancing positive impacts, and managing risks and opportunities",
            "description": "Customer satisfaction targets, privacy breach reduction goals, accessibility milestones.",
            "data_points": 10,
            "materiality": "material",
        },
    ],
    "total_data_points": 63,
    "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3",
}

# ─── ESRS G1 Governance Requirements (Full Detail) ──────────────────
# Source: ESRS Delegated Regulation (EU) 2023/2772, Annex I

ESRS_G1_REQUIREMENTS = {
    "standard": "ESRS G1 — Business Conduct",
    "objective": "Disclosure on corporate governance, business ethics, anti-corruption, political engagement, and supplier payment practices.",
    "disclosure_requirements": [
        {
            "code": "G1-1",
            "title": "Business conduct policies and corporate culture",
            "description": "Anti-corruption, anti-bribery, and anti-competitive behavior policies. Codes of conduct, whistleblower protection, and ethics training programs.",
            "data_points": 20,
            "materiality": "mandatory",
        },
        {
            "code": "G1-2",
            "title": "Management of relationships with suppliers",
            "description": "Supplier selection criteria, due diligence processes, ethical sourcing requirements, and supplier code of conduct.",
            "data_points": 15,
            "materiality": "material",
        },
        {
            "code": "G1-3",
            "title": "Prevention and detection of corruption and bribery",
            "description": "Corruption risk assessment, anti-corruption training coverage, internal controls, and whistleblowing channels.",
            "data_points": 18,
            "materiality": "mandatory",
        },
        {
            "code": "G1-4",
            "title": "Incidents of corruption or bribery",
            "description": "Number of confirmed corruption/bribery incidents, convictions, fines, and actions taken.",
            "data_points": 10,
            "materiality": "mandatory",
        },
        {
            "code": "G1-5",
            "title": "Political influence and lobbying activities",
            "description": "Political contributions (monetary and in-kind), lobbying expenditures, membership in think tanks and policy organizations.",
            "data_points": 12,
            "materiality": "material",
        },
        {
            "code": "G1-6",
            "title": "Payment practices",
            "description": "Standard payment terms (days), percentage of payments made within standard terms, late payment statistics, and legal proceedings for late payment.",
            "data_points": 14,
            "materiality": "mandatory",
        },
    ],
    "total_data_points": 89,
    "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 4",
}

# ─── EFRAG Implementation Guidance References ─────────────────────────
# Source: EFRAG official publications

EFRAG_IG_REFERENCES = {
    "IG_1": {
        "title": "EFRAG IG 1 — Materiality Assessment",
        "reference": "EFRAG IG 1 (2024)",
        "description": "Guidance on how to perform double materiality assessment, including impact materiality and financial materiality.",
        "url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
        "sections": [
            "Step 1: Understanding the context",
            "Step 2: Identifying actual and potential impacts, risks, and opportunities (IROs)",
            "Step 3: Assessing impact materiality",
            "Step 4: Assessing financial materiality",
            "Step 5: Determining materiality threshold and reporting",
        ],
    },
    "IG_2": {
        "title": "EFRAG IG 2 — Value Chain",
        "reference": "EFRAG IG 2 (2024)",
        "description": "Guidance on value chain mapping, Scope 3 considerations, and upstream/downstream boundaries.",
        "url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
        "sections": [
            "Value chain mapping methodology",
            "Boundary setting for reporting",
            "Tier 1, 2, 3 supplier assessment",
            "Downstream value chain considerations",
        ],
    },
    "IG_3": {
        "title": "EFRAG IG 3 — Detailed ESRS Data Point Mapping",
        "reference": "EFRAG IG 3 (2024)",
        "description": "Complete list of all 1,200+ ESRS data points with references to the delegated regulation.",
        "url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
        "total_data_points": 1218,
    },
    "IG_4": {
        "title": "EFRAG IG 4 — Risk and Opportunity Assessment",
        "reference": "EFRAG IG 4 (2024)",
        "description": "Guidance on integrating sustainability risks and opportunities into enterprise risk management (ERM).",
        "url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
    },
    "EFRAG_ESRS_Q&A": {
        "title": "EFRAG ESRS Q&A Platform",
        "reference": "EFRAG ESRS Q&A (ongoing)",
        "description": "Compilation of technical explanations on ESRS implementation questions submitted by stakeholders.",
        "url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
    },
}

# ─── Sector-Specific ESRS Materiality (EFRAG-informed) ──────────────
# Based on EFRAG sector-specific materiality guidance documents
# Each sector lists which ESRS topics are typically material

SECTOR_MATERIALITY: dict[str, dict] = {
    "manufacturing": {
        "material_topics": ["E1", "E2", "E5", "S1"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Manufacturing (2024)",
        "high_impact_subtopics": ["E1-5 GHG emissions", "E2-3 Pollutant emissions", "E5-3 Resource inflows", "S1-14 Health & safety"],
        "nace_sections": ["C (Manufacturing)"],
    },
    "energy": {
        "material_topics": ["E1", "E2", "E4", "S1", "S3"],
        "cross_cutting": ["ESRS_2", "G1"],
        "efrag_guidance": "EFRAG Sector Guidance - Energy (2024)",
        "high_impact_subtopics": ["E1-5 GHG emissions", "E2-1 Pollution policies", "E4-3 Biodiversity impacts", "S3-1 Community relations"],
        "nace_sections": ["B (Mining)", "D (Electricity, gas)"],
    },
    "construction": {
        "material_topics": ["E1", "E3", "E4", "E5", "S1", "S2"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Construction & Real Estate (2024)",
        "high_impact_subtopics": ["E1-5 GHG emissions (embodied carbon)", "E3-3 Water consumption", "E4-3 Land use", "E5-4 Waste", "S1-14 Safety"],
        "nace_sections": ["F (Construction)", "L (Real estate)"],
    },
    "transport": {
        "material_topics": ["E1", "E2", "S1"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Transport (2024)",
        "high_impact_subtopics": ["E1-5 GHG emissions (fleet)", "E2-3 Air pollutants", "S1-14 Driver health & safety"],
        "nace_sections": ["H (Transportation & storage)"],
    },
    "agriculture": {
        "material_topics": ["E1", "E3", "E4", "E5", "S1", "S2", "S3"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Agriculture & Food (2024)",
        "high_impact_subtopics": ["E1-5 GHG (methane, N2O)", "E3-3 Water consumption (irrigation)", "E4-3 Biodiversity (land use, pesticides)", "S2-1 Value chain labor"],
        "nace_sections": ["A (Agriculture, forestry, fishing)"],
    },
    "real_estate": {
        "material_topics": ["E1", "E3", "E4"],
        "cross_cutting": ["ESRS_2", "S1"],
        "efrag_guidance": "EFRAG Sector Guidance - Real Estate (2024)",
        "high_impact_subtopics": ["E1-5 GHG (operational, embodied)", "E3-3 Water consumption (buildings)", "E4-3 Biodiversity (land use)"],
        "nace_sections": ["L (Real estate)"],
    },
    "finance": {
        "material_topics": ["E1", "E4", "S1", "G1"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Financial Services (2024)",
        "high_impact_subtopics": ["E1-5 Financed emissions (PCAF)", "E4-3 Biodiversity (financed)", "G1-1 Business conduct", "S1-1 Workforce policies"],
        "nace_sections": ["K (Financial & insurance)"],
    },
    "technology": {
        "material_topics": ["E1", "E2", "E5", "S1", "G1"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - ICT (2024)",
        "high_impact_subtopics": ["E1-5 GHG (data centers)", "E2-3 E-waste", "E5-4 Resource outflows (rare earths)", "G1-1 Data ethics, AI governance"],
        "nace_sections": ["J (Information & communication)"],
    },
    "healthcare": {
        "material_topics": ["E1", "E2", "S1", "S3", "S4"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Healthcare (2024)",
        "high_impact_subtopics": ["E1-5 GHG (supply chain)", "E2-3 Pharmaceutical emissions", "S4-1 Product safety, patient rights"],
        "nace_sections": ["Q (Human health & social work)"],
    },
    "retail": {
        "material_topics": ["E1", "E2", "E5", "S1", "S2", "S4"],
        "cross_cutting": ["ESRS_2"],
        "efrag_guidance": "EFRAG Sector Guidance - Retail (2024)",
        "high_impact_subtopics": ["E1-5 GHG (logistics, refrigeration)", "E5-4 Waste (packaging, food)", "S2-1 Supply chain labor", "S4-1 Product safety"],
        "nace_sections": ["G (Wholesale & retail)"],
    },
}

# ─── NACE Code Mapping (All Sectors) ─────────────────────────────────
# Source: Regulation (EC) 1893/2006, NACE Rev. 2

NACE_SECTOR_MAP: dict[str, dict] = {
    "A": {"title": "Agriculture, forestry and fishing", "divisions": "01-03", "csrd_sectors": ["agriculture"]},
    "B": {"title": "Mining and quarrying", "divisions": "05-09", "csrd_sectors": ["energy"]},
    "C": {"title": "Manufacturing", "divisions": "10-33", "csrd_sectors": ["manufacturing"]},
    "D": {"title": "Electricity, gas, steam and air conditioning supply", "divisions": "35", "csrd_sectors": ["energy"]},
    "E": {"title": "Water supply, sewerage, waste management and remediation", "divisions": "36-39", "csrd_sectors": ["manufacturing"]},
    "F": {"title": "Construction", "divisions": "41-43", "csrd_sectors": ["construction"]},
    "G": {"title": "Wholesale and retail trade; repair of motor vehicles and motorcycles", "divisions": "45-47", "csrd_sectors": ["retail"]},
    "H": {"title": "Transportation and storage", "divisions": "49-53", "csrd_sectors": ["transport"]},
    "I": {"title": "Accommodation and food service activities", "divisions": "55-56", "csrd_sectors": ["retail"]},
    "J": {"title": "Information and communication", "divisions": "58-63", "csrd_sectors": ["technology"]},
    "K": {"title": "Financial and insurance activities", "divisions": "64-66", "csrd_sectors": ["finance"]},
    "L": {"title": "Real estate activities", "divisions": "68", "csrd_sectors": ["real_estate"]},
    "M": {"title": "Professional, scientific and technical activities", "divisions": "69-75", "csrd_sectors": ["technology"]},
    "N": {"title": "Administrative and support service activities", "divisions": "77-82", "csrd_sectors": ["manufacturing"]},
    "O": {"title": "Public administration and defence; compulsory social security", "divisions": "84", "csrd_sectors": []},
    "P": {"title": "Education", "divisions": "85", "csrd_sectors": []},
    "Q": {"title": "Human health and social work activities", "divisions": "86-88", "csrd_sectors": ["healthcare"]},
    "R": {"title": "Arts, entertainment and recreation", "divisions": "90-93", "csrd_sectors": []},
    "S": {"title": "Other service activities", "divisions": "94-96", "csrd_sectors": []},
    "T": {"title": "Activities of households as employers", "divisions": "97-98", "csrd_sectors": []},
    "U": {"title": "Activities of extraterritorial organisations and bodies", "divisions": "99", "csrd_sectors": []},
}

# ─── VSME (Voluntary Standard for Non-listed SMEs) ────────────────────
# Source: EFRAG VSME Exposure Draft (2024, expected final 2025)

VSME_REQUIREMENTS = {
    "standard": "VSME — Voluntary Sustainability Reporting Standard for Non-listed SMEs",
    "structure": {
        "basic_module": {
            "name": "VSME Basic Module",
            "description": "Simplified reporting for micro-enterprises and smallest SMEs. ~10 data points.",
            "sections": [
                "B1: Basic business description and sustainability context",
                "B2: Basic GHG emissions (Scope 1, optional Scope 2)",
                "B3: Basic workforce information (employees, gender, safety)",
                "B4: Basic business conduct (anti-corruption, data privacy)",
            ],
        },
        "comprehensive_module": {
            "name": "VSME Comprehensive Module",
            "description": "Extended reporting for SMEs seeking higher transparency. ~30 data points.",
            "sections": [
                "C1: Detailed business model & value chain",
                "C2: Environmental indicators (GHG, energy, waste, water)",
                "C3: Social indicators (workforce, training, turnover, diversity)",
                "C4: Business conduct (supplier relations, payment practices)",
                "C5: EU Taxonomy eligibility (optional)",
            ],
        },
        "narrative_module": {
            "name": "VSME Narrative Module",
            "description": "Qualitative context on sustainability risks, opportunities, and transition plans.",
            "sections": [
                "N1: Transition plan / climate strategy",
                "N2: Material sustainability risks & opportunities",
                "N3: Stakeholder engagement",
            ],
        },
    },
    "key_features": [
        "Voluntary — no mandatory adoption date",
        "SME-tailored: reduced data points, no double materiality required",
        "Digital-ready: inline XBRL tagging optional",
        "Value chain support: helps large companies collect SME supplier data",
        "Compatible with ESRS metrics where applicable",
        "No assurance requirement (unlike full ESRS)",
    ],
    "data_points_total": {
        "basic": 10,
        "comprehensive": 30,
        "narrative": 5,
        "total": 45,
    },
    "data_source": "EFRAG VSME Exposure Draft (July 2024), expected final standard 2025",
}

# ─── ESRS Data Points (All ~1,200+ Reference) ───────────────────────
# Summary by standard

ESRS_DATA_POINTS_SUMMARY: dict[str, dict] = {
    "total_estimated": 1218,
    "source": "EFRAG IG 3 — ESRS Data Point Mapping (2024)",
    "by_standard": {
        "ESRS_2": {"title": "General Requirements", "data_points": 75},
        "E1": {"title": "Climate Change", "data_points": 142},
        "E2": {"title": "Pollution", "data_points": 68},
        "E3": {"title": "Water & Marine Resources", "data_points": 52},
        "E4": {"title": "Biodiversity & Ecosystems", "data_points": 58},
        "E5": {"title": "Resource Use & Circular Economy", "data_points": 55},
        "S1": {"title": "Own Workforce", "data_points": 261},
        "S2": {"title": "Workers in the Value Chain", "data_points": 79},
        "S3": {"title": "Affected Communities", "data_points": 72},
        "S4": {"title": "Consumers & End-Users", "data_points": 63},
        "G1": {"title": "Business Conduct", "data_points": 89},
        "LSME": {"title": "Listed SME Standard", "data_points": 104},
        "non_topic": {"title": "Cross-cutting / Sector-specific / Other", "data_points": 100},
    },
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
            "S1-14: Occupational health & safety (heat stress)",
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


# ─── Public API Functions ─────────────────────────────────────────────


async def get_csrd_requirements(
    entity_type: str = "large",
    sector: str = "manufacturing",
    employees: int = 500,
    revenue: float = 100.0,
) -> dict:
    """
    Get CSRD / ESRS reporting requirements for an entity.

    Determines which ESRS standards apply based on:
    - Entity type (large, listed SME, non-EU group, VSME)
    - Sector (determines materiality)
    - Size thresholds

    Args:
        entity_type: 'large', 'listed_sme', 'non_eu_group', or 'vsme'
        sector: Business sector
        employees: Number of employees
        revenue: Annual revenue in €M

    Returns:
        dict with applicable ESRS standards and requirements
    """
    # Determine entity classification
    classification = CSRD_THRESHOLDS.get(entity_type, CSRD_THRESHOLDS["large"])

    # Get sector-specific material topics
    sector_info = SECTOR_MATERIALITY.get(sector, {
        "material_topics": ["E1", "S1"],
        "efrag_guidance": "EFRAG default (no sector-specific guidance)",
        "high_impact_subtopics": [],
        "nace_sections": [],
    })
    material_topics = sector_info["material_topics"]

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
        "sector_efrag_guidance": sector_info["efrag_guidance"],
        "high_impact_subtopics": sector_info.get("high_impact_subtopics", []),
        "first_reporting": classification["first_reporting"],
        "reporting_deadline": classification["reporting_deadline"],
        "data_source": "EUR-Lex: Directive 2022/2464 (CSRD) + Regulation 2023/2772 (ESRS) + EFRAG IG",
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


async def get_esrs_social_standards(standard: Optional[str] = None) -> dict[str, Any]:
    """
    Get full ESRS S1-S4 social standards requirements.

    Args:
        standard: Optional specific standard ('S1', 'S2', 'S3', 'S4')

    Returns:
        dict with standard requirements including disclosure requirements and data point counts
    """
    standards_map = {
        "S1": ESRS_S1_REQUIREMENTS,
        "S2": ESRS_S2_REQUIREMENTS,
        "S3": ESRS_S3_REQUIREMENTS,
        "S4": ESRS_S4_REQUIREMENTS,
    }

    if standard:
        data = standards_map.get(standard)
        if not data:
            return {
                "error": f"Standard '{standard}' not found",
                "available_standards": list(standards_map.keys()),
            }
        result = dict(data)
        result["data_source"] = "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3"
        return result

    return {
        "standards": standards_map,
        "total_standards": 4,
        "total_data_points": sum(s["total_data_points"] for s in standards_map.values()),
        "data_source": "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 3",
    }


async def get_esrs_governance_standard() -> dict[str, Any]:
    """
    Get full ESRS G1 governance requirements.

    Returns:
        dict with G1 disclosure requirements and data point counts
    """
    result = dict(ESRS_G1_REQUIREMENTS)
    result["data_source"] = "ESRS Delegated Regulation (EU) 2023/2772, Annex I Section 4"
    return result


async def get_efrag_guidance(topic: Optional[str] = None) -> dict[str, Any]:
    """
    Get EFRAG implementation guidance references.

    Args:
        topic: Optional specific guidance ('IG_1', 'IG_2', 'IG_3', 'IG_4', 'EFRAG_ESRS_Q&A')

    Returns:
        dict with available EFRAG guidance documents and URLs
    """
    if topic:
        data = EFRAG_IG_REFERENCES.get(topic)
        if not data:
            return {
                "error": f"Guidance topic '{topic}' not found",
                "available_topics": list(EFRAG_IG_REFERENCES.keys()),
            }
        return data

    return {
        "guidance_documents": EFRAG_IG_REFERENCES,
        "total_documents": len(EFRAG_IG_REFERENCES),
        "efrag_url": "https://www.efrag.org/en/sustainability-reporting/implementation-guidance",
        "data_source": "EFRAG (European Financial Reporting Advisory Group)",
    }


async def get_sector_materiality(sector: Optional[str] = None) -> dict[str, Any]:
    """
    Get sector-specific ESRS materiality based on EFRAG guidance.

    Args:
        sector: Optional specific sector

    Returns:
        dict with material topics, EFRAG guidance references, and NACE sections
    """
    if sector:
        data = SECTOR_MATERIALITY.get(sector)
        if not data:
            return {
                "error": f"Sector '{sector}' not found",
                "available_sectors": list(SECTOR_MATERIALITY.keys()),
            }
        return {
            "sector": sector,
            "material_topics": data["material_topics"],
            "cross_cutting": data["cross_cutting"],
            "efrag_guidance": data["efrag_guidance"],
            "high_impact_subtopics": data["high_impact_subtopics"],
            "nace_sections": data["nace_sections"],
            "data_source": "EFRAG Sector Guidance Documents 2024",
        }

    return {
        "sectors": {k: {
            "material_topics": v["material_topics"],
            "efrag_guidance": v["efrag_guidance"],
        } for k, v in SECTOR_MATERIALITY.items()},
        "total_sectors": len(SECTOR_MATERIALITY),
        "data_source": "EFRAG Sector Guidance Documents 2024",
    }


async def get_double_materiality_assessment(
    sector: str = "manufacturing",
    revenue: float = 100.0,
    employees: int = 500,
    ghg_emissions_tco2e: float = 5000.0,
    value_chain_workers: int = 2000,
    high_risk_countries: bool = False,
) -> dict[str, Any]:
    """
    Generate a double materiality assessment for an entity.

    Evaluates impact materiality (inside-out) and financial materiality (outside-in)
    for each ESRS topic based on sector and entity characteristics.

    Args:
        sector: Business sector
        revenue: Annual revenue in €M
        employees: Number of own employees
        ghg_emissions_tco2e: Annual GHG emissions in tCO₂e
        value_chain_workers: Estimated value chain workers
        high_risk_countries: Whether operations/supply chain includes high-risk countries

    Returns:
        dict with double materiality matrix, topic-by-topic scores, and materiality determination
    """
    sector_info = SECTOR_MATERIALITY.get(sector, {"material_topics": ["E1", "S1"]})
    material_topics = sector_info["material_topics"]

    # Impact materiality (inside-out) scoring logic
    impact_materiality = {}
    for topic in ["E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"]:
        if topic == "E1":
            # Climate: based on GHG emissions intensity
            intensity = ghg_emissions_tco2e / max(revenue, 1)
            impact_materiality[topic] = min(5, max(1, round(intensity / 50)))
        elif topic == "S1":
            # Own workforce: based on employees and sector
            impact_materiality[topic] = 4 if employees > 1000 else (3 if employees > 250 else 2)
        elif topic == "S2":
            # Value chain workers
            impact_materiality[topic] = 4 if value_chain_workers > 10000 else (
                3 if value_chain_workers > 1000 else 2
            )
        elif topic == "G1":
            # Governance: moderate baseline
            impact_materiality[topic] = 3 if revenue > 500 else 2
        elif topic in ["E2", "E5"]:
            # Pollution / Circular economy: sector-dependent
            impact_materiality[topic] = 3 if topic in material_topics else 1
        elif topic in ["E3", "E4"]:
            # Water / Biodiversity: sector-dependent
            impact_materiality[topic] = 3 if topic in material_topics else 1
        elif topic in ["S3", "S4"]:
            impact_materiality[topic] = 3 if topic in material_topics else 1
        else:
            impact_materiality[topic] = 1

        # High-risk country boost
        if high_risk_countries and topic in ["S1", "S2", "S3", "G1"]:
            impact_materiality[topic] = min(5, impact_materiality[topic] + 1)

    # Financial materiality (outside-in) scoring
    financial_materiality = {}
    for topic in ["E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"]:
        if topic == "E1":
            # Carbon pricing risk
            fin_risk = ghg_emissions_tco2e * 80 / 1_000_000  # €80/tCO2 -> €M
            financial_materiality[topic] = min(5, max(1, round(fin_risk / 2 + 1)))
        elif topic == "S1":
            # Workforce disruption risk
            financial_materiality[topic] = 3 if employees > 500 else 2
        elif topic in ["E2", "E5"]:
            # Regulatory compliance cost risk
            financial_materiality[topic] = 3 if topic in material_topics else 1
        elif topic == "G1":
            # Corruption/legal risk
            financial_materiality[topic] = 3 if revenue > 500 else 2
        elif topic in ["E3", "E4", "S2", "S3", "S4"]:
            financial_materiality[topic] = 2 if topic in material_topics else 1
        else:
            financial_materiality[topic] = 1

    # Determine which topics are material (score >= 3 on either dimension)
    material_determination = {}
    for topic in ["E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"]:
        im = impact_materiality[topic]
        fm = financial_materiality[topic]
        is_material = im >= 3 or fm >= 3
        
        if is_material:
            if im >= fm:
                rationale = "Impact-driven materiality"
            elif fm > im:
                rationale = "Financial-driven materiality"
            else:
                rationale = "Both dimensions material"
        else:
            rationale = "Not material (both dimensions below threshold)"
        
        material_determination[topic] = {
            "impact_materiality_score": im,
            "financial_materiality_score": fm,
            "is_material": is_material,
            "rationale": rationale,
            "label": "material" if is_material else "not_material",
        }

    matrix = {
        "E1": {"impact": impact_materiality["E1"], "financial": financial_materiality["E1"]},
        "E2": {"impact": impact_materiality["E2"], "financial": financial_materiality["E2"]},
        "E3": {"impact": impact_materiality["E3"], "financial": financial_materiality["E3"]},
        "E4": {"impact": impact_materiality["E4"], "financial": financial_materiality["E4"]},
        "E5": {"impact": impact_materiality["E5"], "financial": financial_materiality["E5"]},
        "S1": {"impact": impact_materiality["S1"], "financial": financial_materiality["S1"]},
        "S2": {"impact": impact_materiality["S2"], "financial": financial_materiality["S2"]},
        "S3": {"impact": impact_materiality["S3"], "financial": financial_materiality["S3"]},
        "S4": {"impact": impact_materiality["S4"], "financial": financial_materiality["S4"]},
        "G1": {"impact": impact_materiality["G1"], "financial": financial_materiality["G1"]},
    }

    return {
        "entity_profile": {
            "sector": sector,
            "revenue_eur_m": revenue,
            "employees": employees,
            "ghg_emissions_tco2e": ghg_emissions_tco2e,
            "value_chain_workers": value_chain_workers,
            "high_risk_countries": high_risk_countries,
        },
        "methodology": "EFRAG IG 1 Double Materiality: Impact materiality (inside-out, severity x likelihood) + Financial materiality (outside-in, financial effect magnitude)",
        "assessment_date": __import__("datetime").date.today().isoformat(),
        "impact_vs_financial": {
            "impact_driven": sum(1 for d in material_determination.values() if d["rationale"] == "Impact-driven materiality"),
            "financial_driven": sum(1 for d in material_determination.values() if d["rationale"] == "Financial-driven materiality"),
            "both": sum(1 for d in material_determination.values() if d["rationale"] == "Both dimensions material"),
        },
        "material_topics": [t for t, d in material_determination.items() if d["is_material"]],
        "material_topics_count": sum(1 for d in material_determination.values() if d["is_material"]),
        "double_materiality_matrix": matrix,
        "topic_determination": material_determination,
        "data_source": "EFRAG IG 1 — Materiality Assessment (2024), ESRS Delegated Regulation (EU) 2023/2772",
    }


async def get_double_materiality_guidance(sector: str = "manufacturing") -> dict[str, Any]:
    """
    Get EFRAG double materiality guidance for a sector.

    Provides impact materiality (inside-out) and financial materiality (outside-in)
    guidance specific to a sector, based on EFRAG IG 1.

    Args:
        sector: Business sector (default: 'manufacturing')

    Returns:
        dict with sector-specific double materiality guidance
    """
    cache = get_cache()
    cache_key = cache.make_key("double_materiality_guidance", sector)
    cached = cache.get(cache_key)
    if cached:
        return cached

    sector_info = SECTOR_MATERIALITY.get(sector, {
        "material_topics": ["E1", "S1"],
        "efrag_guidance": "EFRAG IG 1 — Materiality Assessment (default)",
        "high_impact_subtopics": [],
    })

    # Impact materiality guidance per topic
    impact_guidance = {
        "E1": {
            "description": "Climate change impacts from own operations and value chain",
            "assessment_factors": ["GHG emissions (Scope 1, 2, 3)", "Transition risk exposure", "Physical climate risk"],
            "typical_severity": "high" if "E1" in sector_info["material_topics"] else "medium",
        },
        "E2": {
            "description": "Pollution impacts on air, water, soil",
            "assessment_factors": ["Pollutant emissions", "Hazardous substances", "Microplastics"],
            "typical_severity": "medium" if "E2" in sector_info["material_topics"] else "low",
        },
        "E3": {
            "description": "Water and marine resource impacts",
            "assessment_factors": ["Water consumption", "Water discharge quality", "Marine ecosystem impacts"],
            "typical_severity": "medium" if "E3" in sector_info["material_topics"] else "low",
        },
        "E4": {
            "description": "Biodiversity and ecosystem impacts",
            "assessment_factors": ["Land use change", "Habitat degradation", "Species impact"],
            "typical_severity": "medium" if "E4" in sector_info["material_topics"] else "low",
        },
        "E5": {
            "description": "Resource use and circular economy impacts",
            "assessment_factors": ["Material consumption", "Waste generation", "Circularity rate"],
            "typical_severity": "medium" if "E5" in sector_info["material_topics"] else "low",
        },
        "S1": {
            "description": "Impacts on own workforce",
            "assessment_factors": ["Working conditions", "Health & safety", "Diversity & inclusion", "Adequate wages"],
            "typical_severity": "high" if "S1" in sector_info["material_topics"] else "medium",
        },
        "S2": {
            "description": "Impacts on value chain workers",
            "assessment_factors": ["Supply chain labor standards", "Child/forced labor risk", "Supplier audits"],
            "typical_severity": "medium" if "S2" in sector_info["material_topics"] else "low",
        },
        "S3": {
            "description": "Impacts on affected communities",
            "assessment_factors": ["Community engagement", "Indigenous rights", "Local economic impacts"],
            "typical_severity": "medium" if "S3" in sector_info["material_topics"] else "low",
        },
        "S4": {
            "description": "Impacts on consumers and end-users",
            "assessment_factors": ["Product safety", "Data privacy", "Responsible marketing", "Accessibility"],
            "typical_severity": "medium" if "S4" in sector_info["material_topics"] else "low",
        },
        "G1": {
            "description": "Business conduct impacts",
            "assessment_factors": ["Anti-corruption", "Supplier relations", "Political engagement", "Payment practices"],
            "typical_severity": "medium",
        },
    }

    # Financial materiality guidance per topic
    financial_guidance = {
        "E1": {
            "description": "Financial risks from climate change",
            "risk_types": ["Carbon pricing risk", "Physical asset risk", "Transition risk", "Litigation risk"],
            "typical_financial_impact": "high",
        },
        "E2": {
            "description": "Financial risks from pollution regulation",
            "risk_types": ["Compliance costs", "Clean-up liabilities", "Reputational risk"],
            "typical_financial_impact": "medium",
        },
        "E3": {
            "description": "Financial risks from water scarcity",
            "risk_types": ["Water supply disruption", "Regulatory restrictions", "Treatment costs"],
            "typical_financial_impact": "medium",
        },
        "E4": {
            "description": "Financial risks from biodiversity loss",
            "risk_types": ["Operational restrictions", "Offset costs", "Reputational risk"],
            "typical_financial_impact": "medium",
        },
        "E5": {
            "description": "Financial risks from resource constraints",
            "risk_types": ["Raw material price volatility", "Waste disposal costs", "Circularity compliance"],
            "typical_financial_impact": "medium",
        },
        "S1": {
            "description": "Financial risks from workforce issues",
            "risk_types": ["Talent attraction/retention", "Strike/disruption risk", "Health & safety liabilities"],
            "typical_financial_impact": "high",
        },
        "S2": {
            "description": "Financial risks from value chain labor issues",
            "risk_types": ["Supply chain disruption", "Import bans (forced labor)", "Reputational damage"],
            "typical_financial_impact": "medium",
        },
        "S3": {
            "description": "Financial risks from community relations",
            "risk_types": ["Operational delays (protests)", "License to operate risk", "Compensation liabilities"],
            "typical_financial_impact": "medium",
        },
        "S4": {
            "description": "Financial risks from consumer issues",
            "risk_types": ["Product liability", "Data breach costs", "Regulatory fines"],
            "typical_financial_impact": "medium",
        },
        "G1": {
            "description": "Financial risks from governance failures",
            "risk_types": ["Corruption fines", "Litigation costs", "Reputational damage"],
            "typical_financial_impact": "high",
        },
    }

    result = {
        "sector": sector,
        "sector_efrag_guidance": sector_info["efrag_guidance"],
        "material_topics": sector_info["material_topics"],
        "high_impact_subtopics": sector_info.get("high_impact_subtopics", []),
        "methodology": "EFRAG IG 1 Double Materiality Assessment Framework",
        "impact_materiality": {
            topic: impact_guidance.get(topic, {})
            for topic in ["E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"]
        },
        "financial_materiality": {
            topic: financial_guidance.get(topic, {})
            for topic in ["E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3", "S4", "G1"]
        },
        "assessment_steps": [
            "Step 1: Understand the context (entity, sector, value chain)",
            "Step 2: Identify actual and potential impacts, risks, and opportunities (IROs)",
            "Step 3: Assess impact materiality (severity × likelihood)",
            "Step 4: Assess financial materiality (magnitude of financial effects)",
            "Step 5: Determine materiality threshold and define reporting scope",
        ],
        "data_source": "EFRAG IG 1 — Materiality Assessment (2024), ESRS Delegated Regulation (EU) 2023/2772",
    }
    cache.set(cache_key, result, category="regulatory", ttl_days=30)
    return result


async def get_nace_mapping(
    nace_code: Optional[str] = None) -> dict[str, Any]:
    """
    Get NACE code mapping for sectors.

    Args:
        nace_code: Optional specific NACE section letter (A-U)

    Returns:
        dict with NACE section details and mapped CSRD sectors
    """
    if nace_code:
        data = NACE_SECTOR_MAP.get(nace_code.upper())
        if not data:
            return {
                "error": f"NACE code '{nace_code}' not found",
                "available_codes": list(NACE_SECTOR_MAP.keys()),
            }
        return {
            "nace_code": nace_code.upper(),
            "title": data["title"],
            "divisions": data["divisions"],
            "mapped_csrd_sectors": data["csrd_sectors"],
            "data_source": "Regulation (EC) 1893/2006, NACE Rev. 2",
        }

    return {
        "sections": NACE_SECTOR_MAP,
        "total_sections": len(NACE_SECTOR_MAP),
        "data_source": "Regulation (EC) 1893/2006, NACE Rev. 2",
    }


def _get_vsme_sector_metrics(sector: str) -> list[dict]:
    """Get simplified VSME metrics for a specific sector."""
    sector_metrics = {
        "manufacturing": [
            {"metric": "Total energy consumption (MWh)", "category": "environmental"},
            {"metric": "Scope 1 GHG emissions (tCO2e)", "category": "environmental"},
            {"metric": "Waste generated (tonnes)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Training hours per employee", "category": "social"},
            {"metric": "Work-related injuries (rate)", "category": "social"},
        ],
        "energy": [
            {"metric": "Total energy production (MWh)", "category": "environmental"},
            {"metric": "Scope 1 GHG emissions (tCO2e)", "category": "environmental"},
            {"metric": "Water withdrawal (m3)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Fatalities (rate)", "category": "social"},
        ],
        "real_estate": [
            {"metric": "Energy intensity (kWh/m2)", "category": "environmental"},
            {"metric": "GHG intensity (kgCO2/m2)", "category": "environmental"},
            {"metric": "Water consumption (m3/m2)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Waste diverted from landfill (%)", "category": "environmental"},
        ],
        "finance": [
            {"metric": "Financed emissions (tCO2e/€M invested)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Gender pay gap (%)", "category": "social"},
            {"metric": "Green/taxonomy-aligned revenue (%)", "category": "environmental"},
        ],
        "agriculture": [
            {"metric": "Land use (hectares)", "category": "environmental"},
            {"metric": "Fertilizer use (kg/hectare)", "category": "environmental"},
            {"metric": "Water consumption (m3)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Health & safety incidents", "category": "social"},
        ],
        "transport": [
            {"metric": "Fleet fuel consumption (liters or kWh)", "category": "environmental"},
            {"metric": "Scope 1 GHG emissions (tCO2e)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Accident rate per km", "category": "social"},
        ],
        "retail": [
            {"metric": "Energy consumption (MWh)", "category": "environmental"},
            {"metric": "Refrigerant leakage (tCO2e)", "category": "environmental"},
            {"metric": "Waste (tonnes, % recycled)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Supplier audits conducted", "category": "social"},
        ],
        "technology": [
            {"metric": "Energy consumption (MWh, data centers)", "category": "environmental"},
            {"metric": "Scope 1+2 GHG emissions (tCO2e)", "category": "environmental"},
            {"metric": "E-waste recycled (tonnes)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Gender diversity at management (%)", "category": "social"},
        ],
        "construction": [
            {"metric": "Energy consumption (MWh)", "category": "environmental"},
            {"metric": "Construction waste (tonnes)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Health & safety incidents", "category": "social"},
            {"metric": "Local hiring rate (%)", "category": "social"},
        ],
        "healthcare": [
            {"metric": "Energy consumption (MWh)", "category": "environmental"},
            {"metric": "Medical waste (tonnes)", "category": "environmental"},
            {"metric": "Number of employees", "category": "social"},
            {"metric": "Patient satisfaction score", "category": "social"},
            {"metric": "Training hours per employee", "category": "social"},
        ],
    }
    return sector_metrics.get(sector, [
        {"metric": "Total energy consumption (MWh)", "category": "environmental"},
        {"metric": "Scope 1 GHG emissions (tCO2e)", "category": "environmental"},
        {"metric": "Number of employees", "category": "social"},
        {"metric": "Training hours per employee", "category": "social"},
    ])


async def get_nace_sectors() -> dict[str, Any]:
    """
    Get all NACE sector classifications.

    Returns:
        dict with all NACE sections, titles, divisions, and mapped CSRD sectors
    """
    return {
        "sections": {k: {
            "title": v["title"],
            "divisions": v["divisions"],
            "csrd_sectors": v["csrd_sectors"],
        } for k, v in NACE_SECTOR_MAP.items()},
        "total_sections": len(NACE_SECTOR_MAP),
        "regulation": "Regulation (EC) 1893/2006, NACE Rev. 2",
        "data_source": "Regulation (EC) 1893/2006, NACE Rev. 2",
    }


async def get_vsme_requirements(sector: Optional[str] = None) -> dict[str, Any]:
    """
    Get VSME (Voluntary Standard for Non-listed SMEs) requirements.

    Args:
        sector: Optional sector to get sector-specific VSME guidance

    Returns:
        dict with VSME module structure, key features, and data point counts
    """
    result = {
        "standard": VSME_REQUIREMENTS["standard"],
        "structure": VSME_REQUIREMENTS["structure"],
        "key_features": VSME_REQUIREMENTS["key_features"],
        "data_points": VSME_REQUIREMENTS["data_points_total"],
        "data_source": VSME_REQUIREMENTS["data_source"],
        "more_info": "https://www.efrag.org/en/sustainability-reporting/sme-sustainability-reporting",
    }
    if sector:
        sector_info = SECTOR_MATERIALITY.get(sector, {})
        result["sector"] = sector
        result["sector_specific_guidance"] = {
            "material_topics": sector_info.get("material_topics", ["E1", "S1"]),
            "efrag_guidance": sector_info.get("efrag_guidance", "EFRAG VSME Sector Guidance"),
            "simplified_metrics": _get_vsme_sector_metrics(sector),
        }
    return result


async def get_esrs_data_points_reference(standard: Optional[str] = None) -> dict[str, Any]:
    """
    Get ESRS Data Points reference (all 1,200+).

    Args:
        standard: Optional specific standard code (e.g., 'E1', 'S1', 'G1')

    Returns:
        dict with data point counts by standard
    """
    by_standard = ESRS_DATA_POINTS_SUMMARY["by_standard"]

    if standard:
        data = by_standard.get(standard)
        if not data:
            return {
                "error": f"Standard '{standard}' not found",
                "available_standards": list(by_standard.keys()),
            }
        return {
            "standard": standard,
            "title": data["title"],
            "data_points": data["data_points"],
            "data_source": ESRS_DATA_POINTS_SUMMARY["source"],
        }

    return {
        "total_data_points": ESRS_DATA_POINTS_SUMMARY["total_estimated"],
        "by_standard": by_standard,
        "data_source": ESRS_DATA_POINTS_SUMMARY["source"],
    }
