"""
SBTi (Science Based Targets initiative) — Target Validation Checker.

Provides:
- SBTi target validation against 1.5°C / well-below-2°C pathways
- Sector-specific decarbonisation pathways and reduction rates
- Near-term (5-10 yr) and long-term (2050) target assessment
- Self-assessment reporting templates for CSRD/TCFD inclusion
- Full validation timeline: commit → develop → submit → validate → communicate → report

Sources:
- SBTi Corporate Manual (v2.0): https://sciencebasedtargets.org/resources/
- SBTi Sector Guidance: https://sciencebasedtargets.org/sectors/
- SBTi Target Validation Protocol: https://sciencebasedtargets.org/target-validation/
- SBTi Net-Zero Standard (v1.1, 2023)
- CRREM Consortium (cross-reference for real estate): https://www.crrem.org/
"""

import logging
from typing import Any, Optional

from ..cache import get_cache

logger = logging.getLogger(__name__)

# ─── SBTi Cross-Sector Reduction Rates ──────────────────────────────────
# Source: SBTi Corporate Manual v2.0, SBTi Net-Zero Standard (2023)
# Linear annual reduction rates from base year to target year

SBTI_REDUCTION_RATES: dict[str, dict[str, float]] = {
    "1.5c": {
        "scope_1_2": 4.2,    # 4.2% linear annual reduction (cross-sector)
        "scope_3": 2.5,      # 2.5% linear annual reduction (if >40% of total)
        "description": "Paris Agreement 1.5°C aligned — stringent pathway",
    },
    "well_below_2c": {
        "scope_1_2": 2.5,    # 2.5% linear annual reduction
        "scope_3": 2.5,      # 2.5% linear annual reduction
        "description": "Well-below 2°C aligned — moderate pathway",
    },
}

# ─── Sector-Specific Pathway Exceptions ────────────────────────────────
# Source: SBTi Sector Guidance documents
# Some sectors have bespoke pathways due to process emissions, abatement limits, etc.
# Rates are linear annual % reductions unless noted otherwise

SECTOR_PATHWAYS: dict[str, dict[str, Any]] = {
    "cross_sector": {
        "label": "Cross-Sector (General Industry)",
        "scope_1_2_rate_1.5c": 4.2,
        "scope_1_2_rate_wb2c": 2.5,
        "scope_3_rate": 2.5,
        "net_zero_year": 2050,
        "interim_target_pct": 50,  # % reduction by 2030 from base year
        "notes": "Default pathway for most sectors. Linear annual approach.",
    },
    "power_generation": {
        "label": "Power Generation / Utilities",
        "scope_1_2_rate_1.5c": 6.0,    # Faster: fuel switching + renewables
        "scope_1_2_rate_wb2c": 3.5,
        "scope_3_rate": 2.5,
        "net_zero_year": 2040,          # Power sector must decarbonise faster
        "interim_target_pct": 65,       # 65% by 2030
        "notes": "Accelerated pathway due to available RE alternatives. "
                 "Coal phase-out by 2030 (OECD) / 2040 (non-OECD).",
    },
    "steel": {
        "label": "Iron & Steel",
        "scope_1_2_rate_1.5c": 3.0,    # Slower due to process emissions
        "scope_1_2_rate_wb2c": 1.8,
        "scope_3_rate": 2.0,
        "net_zero_year": 2050,
        "interim_target_pct": 30,       # 30% by 2030
        "notes": "Process emissions from blast furnaces limit abatement. "
                 "Requires CCS, H2-DRI, or scrap-EAF transition. "
                 "SBTi allows intensity-based targets for this sector.",
    },
    "cement": {
        "label": "Cement & Concrete",
        "scope_1_2_rate_1.5c": 2.5,    # Slower: ~60% process emissions from clinker
        "scope_1_2_rate_wb2c": 1.5,
        "scope_3_rate": 2.0,
        "net_zero_year": 2050,
        "interim_target_pct": 25,       # 25% by 2030
        "notes": "Significant process emissions (CaCO₃ → CaO + CO₂). "
                 "CCS essential. Alternative binders and clinker substitution "
                 "are key levers. Intensity-based targets commonly used.",
    },
    "aviation": {
        "label": "Aviation",
        "scope_1_2_rate_1.5c": 2.0,    # Hard-to-abate sector
        "scope_1_2_rate_wb2c": 1.2,
        "scope_3_rate": 1.5,
        "net_zero_year": 2050,
        "interim_target_pct": 20,       # 20% by 2030
        "notes": "SBTi sector guidance under development. Current approach: "
                 "absolute reduction + sustainable aviation fuel (SAF) "
                 "uplift targets. Carbon offsets NOT accepted for scope 1.",
    },
    "maritime": {
        "label": "Maritime / Shipping",
        "scope_1_2_rate_1.5c": 2.5,
        "scope_1_2_rate_wb2c": 1.5,
        "scope_3_rate": 2.0,
        "net_zero_year": 2050,
        "interim_target_pct": 25,       # 25% by 2030
        "notes": "IMO-aligned pathway. Energy efficiency design index (EEDI) "
                 "and carbon intensity indicator (CII). Alternative fuels "
                 "(methanol, ammonia, hydrogen) needed post-2035.",
    },
    "buildings": {
        "label": "Buildings / Real Estate",
        "scope_1_2_rate_1.5c": 4.2,    # Same as cross-sector for operational emissions
        "scope_1_2_rate_wb2c": 2.5,
        "scope_3_rate": 3.0,           # Embodied carbon (upstream scope 3)
        "net_zero_year": 2050,
        "interim_target_pct": 50,       # 50% by 2030
        "notes": "Covers operational emissions (scope 1+2) and embodied "
                 "carbon (scope 3, A1-A3). CRREM pathways available for "
                 "asset-level alignment cross-reference. "
                 "See also: CRREM 1.5°C pathway data in crrem.py",
    },
    "chemicals": {
        "label": "Chemicals",
        "scope_1_2_rate_1.5c": 3.5,
        "scope_1_2_rate_wb2c": 2.0,
        "scope_3_rate": 2.5,
        "net_zero_year": 2050,
        "interim_target_pct": 35,       # 35% by 2030
        "notes": "Process emissions from steam cracking, ammonia production. "
                 "Electrification, hydrogen, and CCS as key levers. "
                 "SBTi sector guidance: 'Chemicals' published 2024.",
    },
    "transport": {
        "label": "Land Transport (Road & Rail)",
        "scope_1_2_rate_1.5c": 5.0,    # Faster due to EV transition
        "scope_1_2_rate_wb2c": 3.0,
        "scope_3_rate": 2.5,
        "net_zero_year": 2050,
        "interim_target_pct": 45,       # 45% by 2030
        "notes": "Electrification of light-duty vehicles. Heavy-duty: "
                 "battery-electric and hydrogen fuel cell. "
                 "Well-to-wheel accounting recommended.",
    },
    "oil_and_gas": {
        "label": "Oil & Gas (Upstream & Midstream)",
        "scope_1_2_rate_1.5c": 5.0,    # Mandated by SBTi O&G guidance
        "scope_1_2_rate_wb2c": 3.0,
        "scope_3_rate": 4.0,           # End-use (scope 3) must decline significantly
        "net_zero_year": 2050,
        "interim_target_pct": 55,       # 55% reduction in operated emissions by 2030
        "notes": "SBTi released dedicated O&G guidance in 2024. "
                 "Requires absolute emission reductions (no intensity-only). "
                 "Scope 3 end-use emissions must be included.",
    },
}

# ─── SBTi Validation Process Phases ────────────────────────────────────
# Source: SBTi Corporate Manual v2.0, SBTi Target Validation Protocol
# Realistic durations based on SBTi published statistics

SBTI_VALIDATION_PHASES: list[dict[str, Any]] = [
    {
        "phase": "commit",
        "phase_name": "Commit Letter Submission",
        "typical_duration_months": 0.5,
        "min_duration_months": 0.25,
        "max_duration_months": 2,
        "description": "Submit SBTi commitment letter signed by CEO/board. "
                       "Public announcement on SBTi website within 6 months.",
        "deliverables": [
            "Signed commitment letter on company letterhead",
            "Public announcement / press release",
            "Board-level approval documentation",
        ],
        "common_pitfalls": [
            "Lack of board-level authorisation",
            "Commitment letter not on official letterhead",
            "Missing public announcement within 6-month window",
        ],
    },
    {
        "phase": "develop",
        "phase_name": "Target Development",
        "typical_duration_months": 6,
        "min_duration_months": 3,
        "max_duration_months": 12,
        "description": "Develop science-based targets aligned with SBTi criteria. "
                       "Calculate base year emissions, select target year, "
                       "determine scope coverage and reduction pathway.",
        "deliverables": [
            "Completed emission inventory (scope 1, 2, 3)",
            "Base year emissions calculation document",
            "Target scenario modelling and pathway selection",
            "Draft target wording",
        ],
        "common_pitfalls": [
            "Incomplete scope 3 inventory (>40% of total must be covered)",
            "Base year too far in the past (>5 years)",
            "Target year too soon (<5 years from submission)",
            "Using carbon credits in reduction claims (not allowed)",
        ],
    },
    {
        "phase": "submit",
        "phase_name": "Target Submission",
        "typical_duration_months": 0.5,
        "min_duration_months": 0.25,
        "max_duration_months": 1,
        "description": "Submit full target proposal via SBTi online portal. "
                       "Includes all supporting documentation, emission "
                       "inventories, and methodology details.",
        "deliverables": [
            "SBTi target submission form (completed)",
            "Full emission inventory workbook",
            "Target calculation methodology document",
            "Board resolution / approval evidence",
        ],
        "common_pitfalls": [
            "Incomplete submission forms",
            "Mismatch between target narrative and quantitative data",
            "Missing board resolution documentation",
        ],
    },
    {
        "phase": "validate",
        "phase_name": "SBTi Validation Review",
        "typical_duration_months": 4,
        "min_duration_months": 2,
        "max_duration_months": 8,
        "description": "SBTi technical review team assesses target against "
                       "criteria. May request clarifications or revisions. "
                       "Average wait time: ~120 days as of 2025.",
        "deliverables": [
            "Response to SBTi review queries (if any)",
            "Revised target documentation (if requested)",
            "Final target approval letter",
        ],
        "common_pitfalls": [
            "Insufficient ambition (below minimum pathway)",
            "Scope 3 coverage gap (>40% but not covering all material categories)",
            "Biomass accounting errors",
            "Use of avoided emissions or offsets",
        ],
    },
    {
        "phase": "communicate",
        "phase_name": "Announce & Communicate",
        "typical_duration_months": 1,
        "min_duration_months": 0.25,
        "max_duration_months": 3,
        "description": "Once validated, companies must publicly announce "
                       "within 6 months. Targets appear on SBTi website.",
        "deliverables": [
            "Press release / public announcement",
            "Updated sustainability report or website",
            "Integration into investor communications",
        ],
        "common_pitfalls": [
            "Delayed announcement beyond 6-month window",
            "Inconsistent messaging vs submission materials",
            "Failure to update CDP disclosure with SBTi status",
        ],
    },
    {
        "phase": "report",
        "phase_name": "Annual Reporting & Disclosure",
        "typical_duration_months": 12,  # Ongoing annual cycle
        "min_duration_months": 12,
        "max_duration_months": 12,
        "description": "Report progress annually via CDP, annual report, "
                       "or sustainability report. SBTi monitors progress "
                       "and may remove targets if reporting is missed.",
        "deliverables": [
            "Annual emission data (scope 1, 2, 3)",
            "Progress against interim targets",
            "CDP disclosure (recommended channel)",
            "Explanation of any deviations or recalculations",
        ],
        "common_pitfalls": [
            "Missing annual reporting cycle",
            "Material changes without recalculation/revalidation",
            "Divestitures or acquisitions not reflected in baseline",
            "Target year approaching without recalibration",
        ],
    },
]

# ─── SBTi Target Rejection / Clarification Reasons ────────────────────
# Source: SBTi Target Validation Protocol, aggregated from published outcomes

REJECTION_REASONS: list[dict[str, Any]] = [
    {
        "reason": "Insufficient ambition",
        "severity": "critical",
        "description": "Reduction rate does not meet minimum SBTi pathway requirements. "
                       "Must align with 1.5°C (4.2%/yr) or well-below-2°C (2.5%/yr).",
        "fix": "Increase annual reduction rate to meet minimum pathway requirements.",
    },
    {
        "reason": "Scope 3 coverage gap",
        "severity": "critical",
        "description": "Scope 3 emissions >40% of total but target does not cover "
                       "at least 2/3 of scope 3 emissions.",
        "fix": "Expand scope 3 target to cover at least 67% of total scope 3 emissions.",
    },
    {
        "reason": "Offset use in target",
        "severity": "critical",
        "description": "Inclusion of carbon credits / offsets in emission reduction claims. "
                       "SBTi does not allow offsets to count toward target achievement.",
        "fix": "Remove offsets from reduction calculations. Only direct emission reductions qualify.",
    },
    {
        "reason": "Invalid base year",
        "severity": "major",
        "description": "Base year more than 5 years prior to submission year, "
                       "or no valid base year emissions data.",
        "fix": "Update base year to within 5 years of submission, or submit earliest "
               "available reliable data with justification.",
    },
    {
        "reason": "Missing scope 3 target justification",
        "severity": "major",
        "description": "Scope 3 >40% but company has not set a scope 3 target "
                       "or not provided justification for exclusion.",
        "fix": "Either set a scope 3 target or provide documented justification "
               "(e.g., regulatory barriers, data limitations).",
    },
    {
        "reason": "Biomass accounting errors",
        "severity": "major",
        "description": "Incorrect treatment of biogenic emissions or biomass "
                       "carbon stock changes.",
        "fix": "Apply SBTi biomass accounting guidance. Report biogenic emissions "
               "separately from fossil emissions.",
    },
    {
        "reason": "Target timeline outside acceptable range",
        "severity": "major",
        "description": "Near-term target must cover 5-10 years from submission. "
                       "Long-term target must be by 2050 at latest.",
        "fix": "Adjust target year to 5-10 year horizon for near-term, "
               "or by 2050 for net-zero targets.",
    },
    {
        "reason": "Intensity target without absolute commitment",
        "severity": "minor",
        "description": "Using intensity-based targets without an accompanying "
                       "absolute reduction commitment.",
        "fix": "Add absolute emission reduction target alongside intensity target.",
    },
    {
        "reason": "Material changes not recalculated",
        "severity": "minor",
        "description": "Significant structural changes (M&A, divestiture) without "
                       "baseline recalculation.",
        "fix": "Recalculate baseline emissions and submit recalculation to SBTi.",
    },
]


def _get_sector_pathway(sector: str) -> dict[str, Any]:
    """Get SBTi sector pathway data, falling back to cross-sector default."""
    sector_key = sector.lower().replace(" ", "_").replace("-", "_")
    pathway = SECTOR_PATHWAYS.get(sector_key)
    if pathway is None:
        pathway = SECTOR_PATHWAYS["cross_sector"]
    return pathway


def _get_annual_reduction_rate(sector_pathway: dict[str, Any], temperature_goal: str) -> tuple[float, float]:
    """Extract scope 1+2 and scope 3 annual reduction rates from a pathway dict.

    Args:
        sector_pathway: Sector pathway dict from SECTOR_PATHWAYS
        temperature_goal: '1.5c' or 'well_below_2c' (accepts variants)

    Returns:
        Tuple of (scope_1_2_rate, scope_3_rate) as percentages
    """
    goal = temperature_goal.lower().replace("-", "_").replace(" ", "_")

    if "1.5" in goal:
        s12_rate = sector_pathway.get("scope_1_2_rate_1.5c",
                                      SBTI_REDUCTION_RATES["1.5c"]["scope_1_2"])
        s3_rate = sector_pathway.get("scope_3_rate",
                                     SBTI_REDUCTION_RATES["1.5c"]["scope_3"])
    else:
        s12_rate = sector_pathway.get("scope_1_2_rate_wb2c",
                                      SBTI_REDUCTION_RATES["well_below_2c"]["scope_1_2"])
        s3_rate = sector_pathway.get("scope_3_rate",
                                     SBTI_REDUCTION_RATES["well_below_2c"]["scope_3"])

    return s12_rate, s3_rate


# ─── Public API Functions ──────────────────────────────────────────────


def check_sbti_target(
    company_emissions: dict[str, float],
    sector: str = "cross_sector",
    target_year: int = 2035,
    base_year: int = 2025,
    base_year_emissions: Optional[dict[str, float]] = None,
    temperature_goal: str = "1.5c",
    scope3_included: bool = True,
) -> dict[str, Any]:
    """Validate company emission targets against SBTi criteria.

    Compares the company's projected/target emissions at target_year against
    the required SBTi sector pathway. Performs both near-term (5-10 year)
    and long-term (2050) validation.

    Args:
        company_emissions: Dict with keys 'scope_1_2' and optionally 'scope_3'
                          representing target emissions (in tCO₂e) at target_year.
        sector: Sector name (cross_sector, power_generation, steel, cement,
                aviation, maritime, buildings, chemicals, transport, oil_and_gas)
        target_year: Year for which the target is set (near-term: 5-10 years)
        base_year: Base year of the target (default: 2025)
        base_year_emissions: Dict with base year emissions for context.
                            If None, company_emissions are used for ratio.
        temperature_goal: Climate scenario ('1.5c' or 'well_below_2c')
        scope3_included: Whether scope 3 is included in the target

    Returns:
        dict with validation result, status, reduction details, and recommendations
    """
    pathway = _get_sector_pathway(sector)
    rate_s12, rate_s3 = _get_annual_reduction_rate(pathway, temperature_goal)

    # Use base year emissions if provided, else use current as proxy
    base_s12 = base_year_emissions.get("scope_1_2", 0) if base_year_emissions else 0
    base_s3 = base_year_emissions.get("scope_3", 0) if base_year_emissions else 0
    target_s12 = company_emissions.get("scope_1_2", 0)
    target_s3 = company_emissions.get("scope_3", 0)

    years_to_target = max(target_year - base_year, 1)

    # Calculate required target based on linear annual reduction
    required_s12 = base_s12 * ((1 - rate_s12 / 100) ** years_to_target)
    required_s3 = base_s3 * ((1 - rate_s3 / 100) ** years_to_target)

    # Assess actual reduction rate from the data provided
    actual_s12_ratio = target_s12 / max(base_s12, 1)
    actual_s12_rate = (1 - actual_s12_ratio ** (1 / max(years_to_target, 1))) * 100 \
        if base_s12 > 0 else 0

    actual_s3_ratio = target_s3 / max(base_s3, 1)
    actual_s3_rate = (1 - actual_s3_ratio ** (1 / max(years_to_target, 1))) * 100 \
        if base_s3 > 0 else 0

    # Build validation checks
    checks = []
    issues = []
    warnings = []

    # Check 1: Scope 1+2 reduction rate
    if base_s12 > 0:
        if actual_s12_rate >= rate_s12:
            checks.append({
                "check": "scope_1_2_reduction_rate",
                "status": "pass",
                "detail": (
                    f"Scope 1+2 reduction rate: {actual_s12_rate:.2f}%/yr "
                    f"(required: {rate_s12}%/yr)"
                ),
            })
        elif actual_s12_rate >= rate_s12 * 0.75:
            checks.append({
                "check": "scope_1_2_reduction_rate",
                "status": "needs_improvement",
                "detail": (
                    f"Scope 1+2 reduction rate: {actual_s12_rate:.2f}%/yr "
                    f"(required: {rate_s12}%/yr) — approaches minimum but insufficient"
                ),
            })
            issues.append("Scope 1+2 reduction rate below required SBTi pathway threshold.")
        else:
            checks.append({
                "check": "scope_1_2_reduction_rate",
                "status": "fail",
                "detail": (
                    f"Scope 1+2 reduction rate: {actual_s12_rate:.2f}%/yr "
                    f"(required: {rate_s12}%/yr) — significantly below minimum"
                ),
            })
            issues.append("Scope 1+2 reduction rate is critically below SBTi pathway.")

    # Check 2: Scope 3 coverage and reduction
    if scope3_included and base_s3 > 0:
        s3_pct_of_total = base_s3 / max(base_s3 + base_s12, 1) * 100
        if s3_pct_of_total > 40:
            # SBTi mandates scope 3 target when >40% of total
            if actual_s3_rate >= rate_s3:
                checks.append({
                    "check": "scope_3_reduction_rate",
                    "status": "pass",
                    "detail": (
                        f"Scope 3 reduction rate: {actual_s3_rate:.2f}%/yr "
                        f"(required: {rate_s3}%/yr)"
                    ),
                })
            elif actual_s3_rate >= rate_s3 * 0.75:
                checks.append({
                    "check": "scope_3_reduction_rate",
                    "status": "needs_improvement",
                    "detail": (
                        f"Scope 3 reduction rate: {actual_s3_rate:.2f}%/yr "
                        f"(required: {rate_s3}%/yr)"
                    ),
                })
                issues.append("Scope 3 reduction rate below SBTi minimum.")
            else:
                checks.append({
                    "check": "scope_3_reduction_rate",
                    "status": "fail",
                    "detail": (
                        f"Scope 3 reduction rate: {actual_s3_rate:.2f}%/yr "
                        f"(required: {rate_s3}%/yr)"
                    ),
                })
                issues.append("Scope 3 reduction rate critically below SBTi pathway.")
        else:
            checks.append({
                "check": "scope_3_threshold",
                "status": "pass",
                "detail": (
                    f"Scope 3 is {s3_pct_of_total:.0f}% of total emissions "
                    f"(<40%), scope 3 target optional"
                ),
            })
    elif not scope3_included and base_s3 > 0:
        s3_pct_of_total = base_s3 / max(base_s3 + base_s12, 1) * 100
        if s3_pct_of_total > 40:
            checks.append({
                "check": "scope_3_inclusion",
                "status": "fail",
                "detail": (
                    f"Scope 3 is {s3_pct_of_total:.0f}% of total emissions "
                    f"(>40%), scope 3 target is REQUIRED by SBTi"
                ),
            })
            issues.append("Scope 3 emissions exceed 40% of total — target must include scope 3.")

    # Check 3: Target timeline
    if target_year - base_year < 5:
        warnings.append("Target horizon is less than 5 years — SBTi requires "
                        "minimum 5-year horizon for near-term targets.")
    elif target_year - base_year > 10:
        warnings.append("Target horizon exceeds 10 years — consider setting a "
                        "near-term target (5-10 yr) alongside long-term target.")

    # Check 4: Long-term (2050) alignment
    if target_year >= 2045:
        lt_years = 2050 - base_year
        lt_required = base_s12 * ((1 - rate_s12 / 100) ** lt_years)
        if lt_required <= 0:
            checks.append({
                "check": "long_term_net_zero",
                "status": "pass",
                "detail": "Pathway reaches net-zero or negative by 2050.",
            })
        elif actual_s12_rate >= rate_s12:
            checks.append({
                "check": "long_term_net_zero",
                "status": "pass",
                "detail": f"Target trajectory is consistent with net-zero by 2050.",
            })
        else:
            checks.append({
                "check": "long_term_net_zero",
                "status": "needs_improvement",
                "detail": "Current trajectory may not reach net-zero by 2050. "
                          "Consider a dedicated long-term target.",
            })
            warnings.append("Long-term (2050) net-zero alignment needs strengthening.")

    # Determine overall status
    fail_count = sum(1 for c in checks if c["status"] == "fail")
    improve_count = sum(1 for c in checks if c["status"] == "needs_improvement")
    pass_count = sum(1 for c in checks if c["status"] == "pass")

    if fail_count > 0:
        overall_status = "rejected"
    elif improve_count > 0:
        overall_status = "needs_improvement"
    else:
        overall_status = "approved"

    result = {
        "status": overall_status,
        "sector": sector,
        "sector_label": pathway.get("label", sector),
        "temperature_goal": temperature_goal,
        "base_year": base_year,
        "target_year": target_year,
        "years_to_target": years_to_target,
        "required_annual_reduction": {
            "scope_1_2_pct": rate_s12,
            "scope_3_pct": rate_s3,
        },
        "actual_annual_reduction": {
            "scope_1_2_pct": round(actual_s12_rate, 2),
            "scope_3_pct": round(actual_s3_rate, 2),
        },
        "required_target_emissions": {
            "scope_1_2_tco2e": round(required_s12, 1),
            "scope_3_tco2e": round(required_s3, 1),
        },
        "checks": checks,
        "issues": issues,
        "warnings": warnings,
        "recommendations": _generate_recommendations(overall_status, issues, checks, pathway),
        "net_zero_year": pathway.get("net_zero_year", 2050),
        "data_source": "SBTi Corporate Manual v2.0, SBTi Net-Zero Standard (2023)",
    }

    return result


def _generate_recommendations(
    status: str,
    issues: list[str],
    checks: list[dict],
    pathway: dict[str, Any],
) -> list[str]:
    """Generate actionable recommendations based on check results."""
    recs = []
    if status == "rejected":
        recs.append("Increase reduction rate to at least "
                     f"{pathway.get('scope_1_2_rate_1.5c', 4.2)}%/yr for scope 1+2.")
    if any("scope 3" in i.lower() for i in issues):
        recs.append("Include scope 3 in target covering at least 67% of "
                     "scope 3 emissions.")
    if "Scope 1+2 reduction rate is critically below" in str(issues):
        recs.append("Re-evaluate emission reduction levers: energy efficiency, "
                     "renewable energy procurement, electrification.")
    if status == "approved":
        recs.append("Target is aligned with SBTi criteria. Proceed with "
                     "submission and public communication.")
    recs.append("Ensure annual reporting via CDP to maintain SBTi validation status.")
    return recs


def get_sbti_required_reduction(
    sector: str = "cross_sector",
    temperature_goal: str = "1.5c",
    years_to_target: int = 10,
) -> dict[str, Any]:
    """Get minimum annual reduction rate required by SBTi for a sector.

    Provides sector-specific decarbonisation pathway data, including
    scope 1+2 and scope 3 required reduction rates. Cross-references
    CRREM pathways for the buildings/real estate sector.

    Args:
        sector: Sector name (see SECTOR_PATHWAYS keys for options)
        temperature_goal: '1.5c' or 'well_below_2c'
        years_to_target: Number of years from base to target (default: 10)

    Returns:
        dict with required rates, total reduction over period, and sector info
    """
    pathway = _get_sector_pathway(sector)
    rate_s12, rate_s3 = _get_annual_reduction_rate(pathway, temperature_goal)

    # Total reduction over the specified period
    total_reduction_s12 = 1 - (1 - rate_s12 / 100) ** years_to_target
    total_reduction_s3 = 1 - (1 - rate_s3 / 100) ** years_to_target

    result = {
        "sector": sector,
        "temperature_goal": temperature_goal,
        "years_to_target": years_to_target,
        "annual_reduction_rate": {
            "scope_1_2_pct_per_year": rate_s12,
            "scope_3_pct_per_year": rate_s3,
        },
        "total_reduction_required": {
            "scope_1_2_pct": round(total_reduction_s12 * 100, 1),
            "scope_3_pct": round(total_reduction_s3 * 100, 1),
        },
        "net_zero_year": pathway.get("net_zero_year", 2050),
        "interim_target_by_2030_pct": pathway.get("interim_target_pct", 50),
        "notes": pathway.get("notes", "Cross-sector pathway applied."),
        "crrem_cross_reference": None,
        "data_source": "SBTi Corporate Manual v2.0, Sector Guidance Documents",
    }

    # Cross-reference with CRREM for buildings/real estate
    if sector in ("buildings", "real_estate"):
        try:
            from .crrem import get_crrem_pathway  # type: ignore

            # CRREM covers buildings at 4.2%/yr (1.5°C) — same as SBTi cross-sector
            result["crrem_cross_reference"] = {
                "source": "CRREM Consortium Pathway v2",
                "scope_1_2_consistency": "aligned" if rate_s12 == 4.2 else "divergent",
                "note": (
                    "CRREM 1.5°C pathways use ~4.2%/yr linear reduction for "
                    "operational emissions (scope 1+2), consistent with SBTi "
                    "cross-sector pathway. See crrem.py for asset-level data."
                ),
            }
        except ImportError:
            result["crrem_cross_reference"] = {
                "source": "CRREM not available",
                "note": "CRREM module not found. Install climate-csrd-mcp with "
                        "full dependencies for cross-reference.",
            }

    return result


def get_sbti_sector_pathway(
    sector: str = "cross_sector",
    temperature_goal: str = "1.5c",
) -> dict[str, Any]:
    """Get sector-specific SBTi pathway data with carbon budget allocation.

    1.5°C aligned pathways default to 4.2%/yr linear reduction for cross-sector,
    with sector-specific exceptions for hard-to-abate industries (aviation,
    maritime, steel, cement) which have slower permissible rates due to
    process emissions and limited abatement options.

    Args:
        sector: Sector name
        temperature_goal: '1.5c' or 'well_below_2c'

    Returns:
        dict with pathway details, reduction rates, carbon budget, and notes
    """
    pathway = _get_sector_pathway(sector)
    rate_s12, rate_s3 = _get_annual_reduction_rate(pathway, temperature_goal)

    # Remaining carbon budget (global, illustrative — in GtCO₂)
    # Source: IPCC AR6 WGIII, SBTi alignment
    global_carbon_budgets: dict[str, dict[str, float]] = {
        "1.5c": {
            "from_2020_gtco2": 400,
            "probability_pct": 67,
            "note": "Remaining carbon budget for 67% chance of limiting warming to 1.5°C",
        },
        "well_below_2c": {
            "from_2020_gtco2": 1150,
            "probability_pct": 67,
            "note": "Remaining carbon budget for 67% chance of limiting warming to 2°C",
        },
    }

    budget = global_carbon_budgets.get(
        temperature_goal,
        global_carbon_budgets["1.5c"],
    )

    # Sector allocation (simplified proportional share, illustrative)
    sector_shares: dict[str, float] = {
        "cross_sector": None,  # Not applicable at cross-sector level
        "power_generation": 0.25,
        "steel": 0.07,
        "cement": 0.05,
        "aviation": 0.03,
        "maritime": 0.02,
        "buildings": 0.10,
        "chemicals": 0.04,
        "transport": 0.15,
        "oil_and_gas": 0.12,
    }
    sector_share = sector_shares.get(sector, None)
    sector_budget = round(budget["from_2020_gtco2"] * sector_share, 1) \
        if sector_share is not None else None

    result = {
        "sector": sector,
        "sector_label": pathway.get("label", sector),
        "temperature_goal": temperature_goal,
        "pathway_type": "linear_annual_reduction",
        "annual_reduction_rate": {
            "scope_1_2_pct_per_year": rate_s12,
            "scope_3_pct_per_year": rate_s3,
        },
        "milestones": {
            "2030": f"{pathway.get('interim_target_pct', 50)}% reduction from base year",
            "2050": f"{pathway.get('net_zero_year', 2050) - 2020}% reduction (~net-zero)",
        },
        "net_zero_year": pathway.get("net_zero_year", 2050),
        "carbon_budget": {
            "global_remaining_gtco2": budget["from_2020_gtco2"],
            "probability": f"{budget['probability_pct']}% chance of limiting to "
                           f"{temperature_goal.replace('_', ' ').replace('c', '°C')}",
            "sector_share": sector_share,
            "sector_budget_gtco2": sector_budget,
            "note": "Sector budget allocation is illustrative. Actual company-level "
                    "budgets depend on baseline emissions and fair-share allocation.",
        },
        "sector_exception": sector in ("aviation", "maritime", "steel", "cement"),
        "notes": pathway.get("notes", ""),
        "all_sectors": list(SECTOR_PATHWAYS.keys()),
        "data_source": "SBTi Sector Guidance, IPCC AR6 WGIII Carbon Budgets",
    }

    return result


def get_sbti_reporting_template(
    company_data_dict: dict[str, Any],
) -> dict[str, Any]:
    """Generate SBTi self-assessment and reporting templates.

    Produces ready-to-use template text for:
    - Self-assessment against SBTi criteria
    - Emission reduction roadmap outline
    - Target language for CSRD/TCFD disclosure inclusion
    - SBTi commitment letter template

    Args:
        company_data_dict: Dict with keys:
            company_name, sector, base_year, base_emissions (dict),
            target_year, target_emissions (dict), temperature_goal,
            scope3_included (bool), contact_name, contact_title

    Returns:
        dict with all templates as formatted text
    """
    name = company_data_dict.get("company_name", "[Company Name]")
    sector = company_data_dict.get("sector", "cross_sector")
    base_year = company_data_dict.get("base_year", 2025)
    target_year = company_data_dict.get("target_year", 2035)
    contact = company_data_dict.get("contact_name", "[Name]")
    title = company_data_dict.get("contact_title", "[Title]")
    temp_goal = company_data_dict.get("temperature_goal", "1.5c")

    pathway = _get_sector_pathway(sector)
    rate_s12, rate_s3 = _get_annual_reduction_rate(pathway, temp_goal)

    # Build self-assessment checklist
    base_emissions = company_data_dict.get("base_emissions", {})
    target_emissions = company_data_dict.get("target_emissions", {})
    scope3_inc = company_data_dict.get("scope3_included", True)

    s12_base = base_emissions.get("scope_1_2", 0)
    s3_base = base_emissions.get("scope_3", 0)
    total_base = s12_base + s3_base
    s3_pct = round(s3_base / max(total_base, 1) * 100, 1)

    assessment_items = [
        {
            "criterion": "Emission inventory completeness",
            "status": "complete" if total_base > 0 else "missing",
            "detail": f"Base year emissions: {total_base:,.0f} tCO₂e (scope 1+2: {s12_base:,.0f}, "
                      f"scope 3: {s3_base:,.0f})",
        },
        {
            "criterion": "Scope 3 coverage (>40% threshold check)",
            "status": "required" if s3_pct > 40 else "optional",
            "detail": f"Scope 3 is {s3_pct}% of total — "
                      f"{'SBTi scope 3 target REQUIRED' if s3_pct > 40 else 'scope 3 target optional'}",
        },
        {
            "criterion": "Target timeline (5-10 year horizon)",
            "status": "valid" if 5 <= (target_year - base_year) <= 10 else "check",
            "detail": f"{target_year - base_year} year horizon from {base_year} to {target_year}",
        },
        {
            "criterion": "Reduction ambition",
            "status": "pending_validation",
            "detail": f"Requires {rate_s12}%/yr scope 1+2 reduction ({temp_goal} pathway)",
        },
    ]

    return {
        "self_assessment": {
            "company": name,
            "sector": sector,
            "assessment_date": "[Date]",
            "items": assessment_items,
            "overall_readiness": "ready" if total_base > 0 else "needs_preparation",
        },
        "emission_reduction_roadmap": _build_roadmap_template(name, base_year, target_year,
                                                               rate_s12, rate_s3, pathway),
        "csrd_tcfd_target_language": _build_target_language(name, sector, base_year,
                                                             target_year, temp_goal, rate_s12),
        "sbti_commitment_letter": _build_commitment_letter(name, contact, title, sector,
                                                            base_year, target_year, temp_goal),
        "data_source": "SBTi Corporate Manual v2.0, SBTi Net-Zero Standard (2023)",
    }


def _build_roadmap_template(
    company_name: str,
    base_year: int,
    target_year: int,
    rate_s12: float,
    rate_s3: float,
    pathway: dict[str, Any],
) -> dict[str, Any]:
    """Generate emission reduction roadmap outline."""
    milestones = []
    interim_year = base_year + 5

    for yr in [base_year, interim_year, target_year, 2050]:
        if yr == base_year:
            pct = 0
        elif yr == 2050:
            pct = pathway.get("interim_target_pct", 100)
        else:
            yrs_out = yr - base_year
            pct = round((1 - (1 - rate_s12 / 100) ** yrs_out) * 100, 1)

        milestones.append({
            "year": yr,
            "reduction_pct_from_base": pct,
            "description": {
                base_year: "Baseline inventory",
                interim_year: "Interim milestone",
                target_year: "Target year",
                2050: "Net-zero year",
            }.get(yr, "Milestone"),
        })

    return {
        "company": company_name,
        "base_year": base_year,
        "target_year": target_year,
        "net_zero_year": 2050,
        "roadmap_milestones": milestones,
        "key_levers": [
            "Energy efficiency improvements (lighting, HVAC, motors)",
            "Renewable energy procurement (PPAs, on-site generation)",
            "Electrification of heat and processes",
            "Supply chain engagement (supplier targets)",
            "Low-carbon product design and materials",
            "Carbon removals (for residual emissions post-2050)",
        ],
        "note": "This roadmap is a template. Adjust milestones and levers "
                "based on company-specific feasibility assessment.",
    }


def _build_target_language(
    company_name: str,
    sector: str,
    base_year: int,
    target_year: int,
    temperature_goal: str,
    rate_s12: float,
) -> str:
    """Generate disclosure-ready target language for CSRD/TCFD reporting."""
    goal_label = "1.5°C" if "1.5" in temperature_goal else "well-below 2°C"
    reduction_pct = round((1 - (1 - rate_s12 / 100) ** (target_year - base_year)) * 100, 0)

    template = (
        f"Science-Based Target — {company_name}\n"
        f"{'=' * 60}\n\n"
        f"{company_name} commits to reduce absolute scope 1 and 2 greenhouse gas "
        f"emissions by {reduction_pct:.0f}% by {target_year} from a {base_year} base year. "
        f"{company_name} also commits to reduce absolute scope 3 greenhouse gas emissions "
        f"from [relevant categories] by [X]% over the same period.\n\n"
        f"This target has been approved by the Science Based Targets initiative (SBTi) "
        f"as consistent with the Paris Agreement {goal_label} pathway, requiring a "
        f"minimum {rate_s12}% linear annual reduction rate for scope 1 and 2 emissions.\n\n"
        f"**Target boundary:**\n"
        f"- Scope 1: Direct emissions from owned/controlled sources\n"
        f"- Scope 2: Indirect emissions from purchased electricity, steam, heat, cooling\n"
        f"- Scope 3: [Value chain emissions — specify categories]\n\n"
        f"**Progress reporting:**\n"
        f"Progress against this target will be reported annually via CDP disclosure "
        f"and in {company_name}'s sustainability report in accordance with CSRD (ESRS E1) "
        f"and TCFD recommendations.\n\n"
        f"**Base year intensity:** [X] tCO₂e / [unit]\n"
        f"**Target year intensity:** [X] tCO₂e / [unit]\n\n"
        f"Note: Carbon credits or offsets are NOT used to achieve this target. "
        f"All reductions are from direct emission abatement within the value chain."
    )
    return template


def _build_commitment_letter(
    company_name: str,
    contact_name: str,
    contact_title: str,
    sector: str,
    base_year: int,
    target_year: int,
    temperature_goal: str,
) -> str:
    """Generate SBTi commitment letter template."""
    template = (
        f"[Date]\n\n"
        f"Science Based Targets initiative\n"
        f"c/o CDP Worldwide\n"
        f"3rd Floor, Queen's House\n"
        f"55-56 Lincoln's Inn Fields\n"
        f"London WC2A 3LJ\n"
        f"United Kingdom\n\n"
        f"**Subject: Commitment to set science-based targets — {company_name}**\n\n"
        f"Dear SBTi Team,\n\n"
        f"{company_name} hereby commits to developing and submitting science-based "
        f"emission reduction targets for validation by the Science Based Targets "
        f"initiative (SBTi).\n\n"
        f"We understand that this commitment requires us to:\n\n"
        f"1. Develop a comprehensive greenhouse gas emission inventory covering "
        f"scope 1, scope 2, and material scope 3 categories, using a {base_year} "
        f"base year or the most recent auditable data.\n\n"
        f"2. Set near-term targets covering a minimum of 5 years and a maximum of "
        f"10 years from the date of submission, aligned with a {temperature_goal.upper()} "
        f"scenario.\n\n"
        f"3. Set long-term targets to reach net-zero emissions by no later than 2050.\n\n"
        f"4. Submit targets for SBTi validation within 24 months of this commitment "
        f"letter.\n\n"
        f"5. Publicly announce our approved targets within 6 months of validation "
        f"and report progress annually.\n\n"
        f"As part of this commitment, {company_name} will:\n"
        f"- Allocate sufficient resources for target development\n"
        f"- Obtain board-level approval for the targets\n"
        f"- Engage relevant internal stakeholders (sustainability, finance, operations)\n"
        f"- Disclose emission data and progress through CDP annually\n\n"
        f"We look forward to working with the SBTi team throughout this process.\n\n"
        f"Yours sincerely,\n\n\n"
        f"{contact_name}\n"
        f"{contact_title}\n"
        f"{company_name}"
    )
    return template


def get_sbti_validation_timeline(
    sector: str = "cross_sector",
    target_year: int = 2035,
) -> dict[str, Any]:
    """Get the full SBTi validation process timeline with milestones.

    Process flow: commit → develop → submit → validate → communicate → report.
    Each phase includes typical durations, deliverables, and common pitfalls.

    Args:
        sector: Sector name (for context in timeline annotations)
        target_year: Intended target year (used for scheduling guidance)

    Returns:
        dict with phases, total timeline, common pitfalls, documentation checklist
    """
    pathway = _get_sector_pathway(sector)

    # Calculate total timeline
    total_months = sum(p["typical_duration_months"] for p in SBTI_VALIDATION_PHASES)
    total_months_min = sum(p["min_duration_months"] for p in SBTI_VALIDATION_PHASES)
    total_months_max = sum(p["max_duration_months"] for p in SBTI_VALIDATION_PHASES)

    # Build phase schedule
    current_month = 0
    phases = []
    for phase in SBTI_VALIDATION_PHASES:
        start_month = current_month
        end_month = start_month + phase["typical_duration_months"]
        phases.append({
            "phase": phase["phase"],
            "phase_name": phase["phase_name"],
            "description": phase["description"],
            "start_month": start_month,
            "end_month": end_month,
            "typical_duration_months": phase["typical_duration_months"],
            "deliverables": phase["deliverables"],
            "common_pitfalls": phase["common_pitfalls"],
        })
        current_month = end_month

    # Collect all common pitfalls across phases
    all_pitfalls = []
    for phase in SBTI_VALIDATION_PHASES:
        for pitfall in phase["common_pitfalls"]:
            all_pitfalls.append({
                "phase": phase["phase"],
                "pitfall": pitfall,
            })

    # Collect rejection/clarification reasons
    rejection_reasons = [
        {"reason": r["reason"], "severity": r["severity"], "fix": r["fix"]}
        for r in REJECTION_REASONS
    ]

    # Documentation checklist
    doc_checklist = []
    for phase in SBTI_VALIDATION_PHASES:
        for doc in phase["deliverables"]:
            doc_checklist.append({
                "phase": phase["phase"],
                "deliverable": doc,
            })

    # Suggested timeline based on target year
    suggested_start_year = target_year - 3  # ~3 years before target
    if suggested_start_year < 2025:
        suggested_start_year = 2025

    result = {
        "sector": sector,
        "target_year": target_year,
        "suggested_start_year": suggested_start_year,
        "sector_notes": pathway.get("notes", ""),
        "total_timeline": {
            "typical_months": total_months,
            "minimum_months": total_months_min,
            "maximum_months": total_months_max,
            "typical_years": round(total_months / 12, 1),
        },
        "phases": phases,
        "common_pitfalls_summary": all_pitfalls,
        "rejection_reasons_summary": rejection_reasons,
        "documentation_checklist": doc_checklist,
        "process_summary": (
            "The SBTi validation process follows six phases: (1) Commit — submit "
            "a signed commitment letter within 2 months; (2) Develop — build your "
            "emission inventory and target model over 3-12 months; (3) Submit — "
            "file complete target documentation via SBTi portal (~2 weeks); "
            "(4) Validate — SBTi technical review over 2-8 months (avg. 4 months); "
            "(5) Communicate — publicly announce approved targets within 6 months; "
            "(6) Report — disclose progress annually via CDP. Total typical duration: "
            f"{round(total_months / 12, 1)} years from commit to announcement."
        ),
        "data_source": "SBTi Corporate Manual v2.0, SBTi Target Validation Protocol",
    }

    return result
