"""
CBAM Calculator — CSRD Climate MCP.

CBAM certificate obligations, product scope mapping,
transition phase analysis, and import declaration templates
per Regulation (EU) 2023/956.

Sources:
- Regulation (EU) 2023/956, Annex I–IV
- EC CBAM Guidance (2024)
- EU ETS free allocation benchmarks (2021/447)
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

EU_ETS_CARBON_PRICE_2026 = 88.0  # €/tCO₂ central forecast

# ─── CBAM Product Scope ──────────────────────────────────────────────
CBAM_PRODUCT_SCOPE: dict[str, dict[str, Any]] = {
    "cement": {"category": "Cement", "cn_chapters": ["2523"],
               "cn_codes_sample": ["2523.10 — Clinker", "2523.21 — Portland white"],
               "default_embedded_emissions_tco2e_per_ton": 0.78, "unit": "t CO₂/t product",
               "eu_ets_benchmark_key": "cement", "nace": "C23.51, C23.52"},
    "electricity": {"category": "Electricity", "cn_chapters": ["2716"],
                    "cn_codes_sample": ["2716.00 — Electrical energy"],
                    "default_embedded_emissions_tco2e_per_ton": 0.45, "unit": "t CO₂/MWh",
                    "eu_ets_benchmark_key": "power_generation", "nace": "D35.11",
                    "note": "No free allocation applies"},
    "fertilisers": {"category": "Fertilisers",
                    "cn_chapters": ["2808", "2814", "2834", "3102", "3103", "3104", "3105"],
                    "cn_codes_sample": ["2808.00 — Nitric acid", "2814.10 — Ammonia",
                                        "3102.30 — Ammonium nitrate"],
                    "default_embedded_emissions_tco2e_per_ton": 1.50, "unit": "t CO₂/t product",
                    "eu_ets_benchmark_key": "chemicals", "nace": "C20.15"},
    "iron_steel": {"category": "Iron & Steel",
                   "cn_chapters": ["7201-7229", "7301-7326"],
                   "cn_codes_sample": ["7201.10 — Pig iron", "7208.51 — Flat-rolled"],
                   "default_embedded_emissions_tco2e_per_ton": 1.85, "unit": "t CO₂/t product",
                   "eu_ets_benchmark_key": "steel", "nace": "C24.1, C24.2, C24.3"},
    "aluminium": {"category": "Aluminium",
                  "cn_chapters": ["7601-7616"],
                  "cn_codes_sample": ["7601.10 — Unwrought aluminium", "7610.10 — Structures"],
                  "default_embedded_emissions_tco2e_per_ton": 1.67, "unit": "t CO₂/t product",
                  "eu_ets_benchmark_key": "aluminium", "nace": "C24.42, C24.43, C24.44"},
    "hydrogen": {"category": "Hydrogen", "cn_chapters": ["2804"],
                 "cn_codes_sample": ["2804.10 — Hydrogen"],
                 "default_embedded_emissions_tco2e_per_ton": 9.0, "unit": "t CO₂/t H₂ (grey H₂)",
                 "eu_ets_benchmark_key": "chemicals", "nace": "C20.11",
                 "note": "Default: SMR without CCS"},
}

# ─── Free Allocation Phase-Down ──────────────────────────────────────
FREE_ALLOCATION_SCHEDULE: dict[int, float] = {
    2026: 97.5, 2027: 95.0, 2028: 90.0, 2029: 82.5,
    2030: 72.5, 2031: 60.0, 2032: 45.0, 2033: 27.5, 2034: 0.0,
}

# ─── Origin Carbon Prices (€/tCO₂, 2025 est.) ────────────────────────
# Source: World Bank Carbon Pricing Dashboard, ICAP
ORIGIN_CARBON_PRICES: dict[str, float] = {
    "CN": 10.5, "IN": 0.0, "US": 0.0, "RU": 1.2, "KR": 8.5, "JP": 3.8,
    "UK": 45.0, "CA": 50.0, "AU": 0.0, "BR": 0.0, "TR": 0.0, "ZA": 8.0,
    "ID": 2.1, "MX": 3.5, "SG": 5.0, "NZ": 45.0, "CH": 120.0,
    "NO": 88.0, "IS": 88.0, "LI": 88.0, "DEFAULT": 0.0,
}

# ─── EU ETS Benchmarks ───────────────────────────────────────────────
EU_BENCHMARK_EMISSIONS: dict[str, float] = {
    "cement": 0.78, "steel": 1.85, "aluminium": 1.67,
    "fertilisers": 1.50, "electricity": 0.45, "hydrogen": 9.0,
}

# ─── Key Milestones ──────────────────────────────────────────────────
CBAM_MILESTONES: list[dict[str, Any]] = [
    {"date": "2023-10-01", "phase": "Transitional — Reporting Only",
     "desc": "Quarterly emissions reporting. No financial adjustment."},
    {"date": "2025-12-31", "phase": "End of Reporting-Only Period",
     "desc": "Final transitional reports. Apply for authorised declarant."},
    {"date": "2026-01-01", "phase": "Full Implementation",
     "desc": "CBAM certificates mandatory. Free allocation at 97.5%."},
    {"date": "2030-01-01", "phase": "Phase-Down (72.5%)",
     "desc": "Majority emissions require certificates."},
    {"date": "2034-01-01", "phase": "Full Liability (0%)",
     "desc": "Full CBAM liability — no free allocation."},
]


def _get_free_allocation_pct(year: int) -> float:
    return 100.0 if year < 2026 else FREE_ALLOCATION_SCHEDULE.get(year, 0.0)


def _get_origin_carbon_price(country_code: str) -> float:
    return ORIGIN_CARBON_PRICES.get(country_code.upper(), ORIGIN_CARBON_PRICES["DEFAULT"])


def _calculate_embedded_emissions_adjustment(actual: float, sector: str) -> dict[str, Any]:
    benchmark = EU_BENCHMARK_EMISSIONS.get(sector)
    if benchmark is None:
        return {"adjusted": actual, "benchmark": None,
                "note": f"No EU benchmark for '{sector}'; using actual."}
    adjusted = min(actual, benchmark)
    return {"adjusted": round(adjusted, 4), "benchmark": benchmark,
            "actual": actual,
            "note": f"EU benchmark: {benchmark}. CBAM uses lower of actual/benchmark."}


# ═══════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════

async def calculate_cbam_obligation(
    import_goods_tons: float,
    embedded_emissions_tco2e_per_ton: float,
    origin_country: str,
    sector: str,
    carbon_price_origin: float = 0.0,
) -> dict[str, Any]:
    """
    Calculate CBAM certificate obligation for imported goods.

    Obligation = emissions × tonnes × CBAM_liable% × (EU_ETS_price − origin_price)

    Returns yearly breakdown (2026-2034), financial totals, and methodology.
    """
    await asyncio.sleep(0)
    if sector not in CBAM_PRODUCT_SCOPE:
        return {"error": f"Unknown sector '{sector}'.",
                "available_sectors": list(CBAM_PRODUCT_SCOPE.keys())}
    eu = EU_ETS_CARBON_PRICE_2026
    op = carbon_price_origin or _get_origin_carbon_price(origin_country)
    pd = max(0.0, eu - op)
    adj = _calculate_embedded_emissions_adjustment(embedded_emissions_tco2e_per_ton, sector)
    ae = adj["adjusted"]
    total = round(import_goods_tons * ae, 2)
    yearly, cum = [], 0.0
    for yr in range(2026, 2035):
        fp = _get_free_allocation_pct(yr)
        cp = 100.0 - fp
        liable = round(total * (cp / 100.0), 2)
        cost = round(liable * pd, 2)
        cum += cost
        yearly.append({"year": yr, "free_pct": fp, "cbam_pct": cp,
                       "liable_tco2e": liable, "cost_eur": cost})
    return {
        "import_summary": {"tons": import_goods_tons, "sector": sector,
                           "origin": origin_country,
                           "origin_cp_eur": op, "eu_ets_cp_eur": eu,
                           "price_diff_eur_per_tco2e": round(pd, 2)},
        "embedded_emissions": {"actual": embedded_emissions_tco2e_per_ton,
                               "adjusted": ae, "eu_benchmark": adj.get("benchmark"),
                               "total_tco2e": total},
        "free_allocation_schedule": {str(y): p for y, p in sorted(FREE_ALLOCATION_SCHEDULE.items())},
        "yearly_obligations": yearly,
        "totals": {"years": "2026-2034",
                   "total_liable_tco2e": round(sum(o["liable_tco2e"] for o in yearly), 2),
                   "cumulative_cost_eur": round(cum, 2)},
        "methodology": (f"CBAM = emissions × tons × (1−free%) × ({eu}€−{op}€). "
                        f"Free allocation: 97.5% (2026) → 0% (2034)."),
        "data_source": "CBAM Regulation (EU) 2023/956",
        "disclaimer": "Estimates based on default parameters. Actual obligations may vary.",
    }


async def get_cbam_product_scope(product_category: str) -> dict[str, Any]:
    """
    Get CBAM product scope details: CN codes, default embedded emissions, phasing timeline.

    Args:
        product_category: CBAM sector key (cement, electricity, fertilisers, iron_steel, aluminium, hydrogen)

    Returns:
        dict with product scope details
    """
    await asyncio.sleep(0)
    scope = CBAM_PRODUCT_SCOPE.get(product_category)
    if scope is None:
        return {"error": f"'{product_category}' not found.",
                "available_categories": list(CBAM_PRODUCT_SCOPE.keys())}

    return {
        "product_category": product_category,
        "category_name": scope["category"],
        "cn_code_chapters": scope["cn_chapters"],
        "cn_codes": scope.get("cn_codes_sample", []),
        "default_embedded_emissions": {
            "value": scope["default_embedded_emissions_tco2e_per_ton"],
            "unit": scope["unit"],
            "method": "Default values (CBAM Annex III)",
            "note": "Use actual data where possible to minimise liability.",
        },
        "eu_ets_benchmark_ref": scope["eu_ets_benchmark_key"],
        "nace_codes": scope["nace"],
        "phasing_timeline": {
            "2023-10_to_2025-12": "Reporting only, no financial adjustment",
            "2026_start": "Full financial adjustment, free allocation 97.5%",
            "2026-2034": "Free allocation phased down to 0%",
            "2034+": "Full CBAM liability",
        },
        "data_source": "CBAM Reg. Annex I; EC 2021/447",
    }


async def get_cbam_transition_phase(year: int) -> dict[str, Any]:
    """
    Get CBAM transition phase details for a given year (2023-2034).

    Returns reporting obligations, free allocation, and key milestones.

    Args:
        year: Calendar year

    Returns:
        dict with phase details, deadlines, milestones
    """
    await asyncio.sleep(0)
    if year < 2023 or year > 2034:
        return {"error": f"Year {year} outside CBAM timeline (2023-2034).",
                "valid_years": list(range(2023, 2035))}

    if year <= 2025:
        phase = "Transitional — Reporting Only"
        desc = ("Quarterly embedded emission reports. No financial obligation. "
                "Default values allowed. Reports due 1 month after quarter-end.")
        q_dl = {"Q1": f"Apr 30, {year}", "Q2": f"Jul 31, {year}",
                "Q3": f"Oct 31, {year}", "Q4": f"Jan 31, {year+1}"}
    else:
        pct = FREE_ALLOCATION_SCHEDULE.get(year, 0.0)
        phase = f"Full Implementation (free allocation {pct}%)"
        desc = (f"CBAM certificates mandatory. Free allocation at {pct}% "
                f"({year}). Annual declaration by May 31 of following year.")
        q_dl = {"annual_declaration": f"May 31, {year+1}",
                "certificate_purchase": "Before import",
                "certificate_surrender": f"May 31, {year+1}"}

    return {
        "year": year, "phase": phase,
        "financial_adjustment_active": year >= 2026,
        "phase_description": desc,
        "free_allocation": {"current_year_pct": _get_free_allocation_pct(year),
                            "schedule": {str(y): p for y, p in sorted(FREE_ALLOCATION_SCHEDULE.items())
                                         if y >= year}},
        "deadlines": q_dl,
        "key_milestones": [m for m in CBAM_MILESTONES if m["date"].startswith(str(year))],
        "data_source": "CBAM Regulation (EU) 2023/956",
    }


async def get_cbam_import_declaration(
    product_category: str,
    value_eur: float,
    origin_country: str,
) -> dict[str, Any]:
    """
    Generate a CBAM import declaration template.

    Returns declaration fields, required documents, authorised declarant
    requirements, and quarterly deadlines.

    Args:
        product_category: CBAM sector key
        value_eur: Customs value (EUR)
        origin_country: ISO country code

    Returns:
        dict with declaration template structure
    """
    await asyncio.sleep(0)
    scope = CBAM_PRODUCT_SCOPE.get(product_category)
    if scope is None:
        return {"error": f"'{product_category}' not found.",
                "available_categories": list(CBAM_PRODUCT_SCOPE.keys())}

    return {
        "declaration_header": {
            "type": "CBAM Annual Declaration",
            "regulation": "Regulation (EU) 2023/956",
            "declarant_type": "Authorised CBAM Declarant",
            "deadline": "May 31 of year following reporting year",
            "format": "Electronic via CBAM Transitional Registry",
        },
        "importer_info": {
            "authorised_declarant_id": "ECO-ID (national competent authority)",
            "company_name": "[Company legal name]",
            "vat_number": "[EU VAT]",
            "eori_number": "[EORI]",
            "registered_address": "[EU member state address]",
        },
        "authorised_declarant_requirements": {
            "criteria": [
                "Established in an EU member state",
                "Registered for CBAM in that state",
                "No serious customs/tax infringements",
                "Financial solvency demonstrated",
                "Technical capability for emissions calculation",
            ],
            "process": "Apply to national competent authority. Approval within 3 months.",
            "validity": "Indefinite, annual review",
        },
        "import_details": {
            "product_category": product_category,
            "category_name": scope["category"],
            "cn_codes": scope.get("cn_codes_sample", []),
            "origin_country": origin_country,
            "customs_value_eur": value_eur,
            "quantity_tons": "[Quantity in metric tons]",
            "origin_carbon_price_eur": _get_origin_carbon_price(origin_country),
        },
        "emissions_declaration": {
            "method": "[Actual / Default / Benchmark]",
            "actual_tco2e_per_ton": "[If actual: verified value]",
            "default_value": scope["default_embedded_emissions_tco2e_per_ton"],
            "total_tco2e": "[Calculated]",
            "verifier": "[Accredited verifier name]",
        },
        "required_documents": [
            "CBAM declaration form (electronic)",
            "Emissions calculation methodology document",
            "Verification report (accredited verifier)",
            "Proof of carbon price paid in origin country",
            "Customs import declaration (SAD/IM-4)",
            "Commercial invoice and packing list",
            "Certificate of origin",
            "Authorised CBAM declarant approval letter",
        ],
        "certificate_account": {
            "certificates_purchased_tco2e": "[Total held]",
            "surrendered_tco2e": "[Surrendered]",
            "price_per_tco2e_eur": "[Price at purchase]",
            "balance_forward_tco2e": "[Unused]",
            "registry_account": "[CBAM Registry number]",
        },
        "quarterly_deadlines": {
            "Q1": "Apr 30", "Q2": "Jul 31", "Q3": "Oct 31",
            "Q4": "Jan 31 (next year)",
            "annual_declaration": "May 31",
            "certificate_surrender": "May 31 (preceding year)",
        },
        "data_source": "CBAM Regulation (EU) 2023/956, EC Guidance (2024)",
        "note": "Template structure. Submit via CBAM Transitional Registry. [Bracketed] values require actual data.",
    }
