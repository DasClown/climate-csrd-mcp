"""
auto_report.py — Automated CSRD Report Generation.

Integration layer combining climate risk, emissions, ESRS E1–E5, S1–S4, G1,
TCFD, TNFD, SBTi, CSDDD, and ESG rating into ready-to-submit report packages.

Functions:
    generate_csrd_report()         — Single-site CSRD report (ESRS E1)
    generate_executive_summary()   — Key findings as narrative text
    generate_full_report_package() — Multi-standard reporting package
    verify_report_completeness()   — ESRS coverage check with gap analysis
"""

from __future__ import annotations
import asyncio, importlib, logging
from datetime import date
from typing import Any, Optional

logger = logging.getLogger(__name__)
_IMPORT_CACHE: dict[str, Any] = {}
_REF_MAP = {"E1":"Climate Change","E2":"Pollution","E3":"Water & Marine Resources",
            "E4":"Biodiversity & Ecosystems","E5":"Resource Use & Circular Economy",
            "S1":"Own Workforce","S2":"Workers in the Value Chain",
            "S3":"Affected Communities","S4":"Consumers & End-Users","G1":"Business Conduct"}
_ESRS_DP = {
    "E1":["E1-1 Transition plan","E1-2 Climate risk assessment","E1-3 Climate targets",
          "E1-4 Energy metrics","E1-5 GHG emissions","E1-6 GHG verification",
          "E1-7 Adaptation actions","E1-8 Internal carbon pricing","E1-9 Financial effects"],
    "E2":["E2-1 Pollution policy","E2-2 Air emissions","E2-3 Water pollution","E2-4 Soil pollution"],
    "E3":["E3-1 Water policy","E3-2 Water consumption","E3-3 Water targets"],
    "E4":["E4-1 Biodiversity policy","E4-2 Impact on biodiversity","E4-3 Biodiversity targets"],
    "E5":["E5-1 Resource inflows","E5-2 Resource outflows","E5-3 Circular economy targets"],
    "S1":["S1-1 Workforce characteristics","S1-2 Working conditions","S1-3 Health & safety",
          "S1-4 Diversity metrics","S1-5 Social targets"],
    "S2":["S2-1 Value chain workers policy","S2-2 Value chain engagement"],
    "S3":["S3-1 Community impact","S3-2 Community engagement"],
    "S4":["S4-1 Consumer health & safety","S4-2 Consumer privacy"],
    "G1":["G1-1 Business conduct policy","G1-2 Anti-corruption","G1-3 Supplier relationships"]}

def _today() -> str: return date.today().isoformat()
def _rl(c: int) -> str: return {1:"very low",2:"low",3:"moderate",4:"high",5:"very high"}.get(c,"unknown")
def _load(m: str):
    if m not in _IMPORT_CACHE: _IMPORT_CACHE[m] = importlib.import_module(m)
    return _IMPORT_CACHE[m]

async def _call(m: str, f: str, *a, **kw):
    fn = getattr(_load(m), f)
    return await fn(*a, **kw) if asyncio.iscoroutinefunction(fn) else fn(*a, **kw)

# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION 1 — generate_csrd_report
# ═══════════════════════════════════════════════════════════════════════════

async def generate_csrd_report(
    site_lat: float, site_lon: float, site_name: str, sector: str,
    company_name: str, company_data_dict: Optional[dict] = None,
) -> dict:
    """CSRD-compliant report for one site covering all ESRS E1 requirements."""
    cd = company_data_dict or {}
    entity_type = cd.get("entity_type", "large")
    employees = cd.get("employees", 500)
    revenue = cd.get("revenue_eur_m", 100.0)
    horizon = cd.get("year_horizon", 2030)
    own_ei = cd.get("own_emission_intensity")

    risk = await _call("climate_csrd_mcp.server","assess_climate_risk",site_lat,site_lon,site_name,horizon)
    bench = await _call("climate_csrd_mcp.server","get_emission_benchmarks",sector)
    csrd = await _call("climate_csrd_mcp.server","get_csrd_requirements",entity_type,sector,employees,revenue)

    flood = risk.get("flood_risk",{}).get("class",2)
    hot = risk.get("heat_risk",{}).get("projected_hot_days_per_year",15)
    drought = risk.get("drought_risk",{}).get("class",2)
    trigs = await _call("climate_csrd_mcp.data_sources.eurlex","get_location_specific_triggers",flood,hot,drought)

    matrix = [{"standard":s,"title":_REF_MAP.get(s,s),"mandatory":s in csrd.get("core_mandatory_standards",[]),"location_triggered":False}
              for s in csrd.get("all_applicable_standards",[])]
    for t in trigs:
        for r in t.get("requirements",[]):
            matrix.append({"standard":r,"title":_REF_MAP.get(r,r),"mandatory":False,"location_triggered":True,"trigger_reason":t.get("trigger","")})

    comp = None
    if own_ei is not None and bench.get("data",{}).get("average",0):
        avg = bench["data"]["average"]
        pct = ((own_ei-avg)/avg)*100
        comp = {"own_intensity":own_ei,"sector_average":avg,"difference_pct":round(pct,1),"status":"above_average" if pct>0 else "below_average"}

    overall = risk.get("overall_risk",{}).get("score",3)
    heat_cls = risk.get("heat_risk",{}).get("class",2)
    recs = []
    if flood>=4: recs.append({"priority":"high","area":"flood","esrs_ref":"E1-7"})
    elif flood>=3: recs.append({"priority":"medium","area":"flood"})
    if heat_cls>=4: recs.append({"priority":"high","area":"heat","esrs_ref":"E1-2, S1-8"})
    if drought>=4: recs.append({"priority":"high","area":"water","esrs_ref":"E3-1, E3-3"})
    if overall>=4: recs.append({"priority":"high","area":"adaptation_strategy","esrs_ref":"E1-1, E1-7"})
    if not recs: recs.append({"priority":"low","area":"monitoring","esrs_ref":"E1-2"})

    stds_in_use = [e["standard"] for e in matrix]
    e1 = {
        "e1_1_transition_plan":{"required":"E1-1" in stds_in_use,"status":"to_be_developed" if overall>=3 else "low_priority"},
        "e1_2_climate_risk_assessment":{"required":True,"summary":f"Overall physical climate risk score: {overall}/5 ({_rl(overall)})"},
        "e1_3_targets":{"required":"E1-3" in stds_in_use,"suggested_areas":["GHG reduction","renewable energy","adaptation"]},
        "e1_4_metrics":{"required":True},
        "e1_5_ghg_emissions":{"required":True,"benchmark":bench.get("data",{}).get("average")},
        "e1_6_verified":{"required":own_ei is not None},
        "e1_7_adaptation":{"required":overall>=3,"risk_factors":[k for k,v in [("flood",flood),("heat",heat_cls),("drought",drought)] if v>=3]},
        "e1_8_internal_carbon_price":{"required":False,"recommendation":"Consider internal carbon pricing if emissions >10,000 tCO2e"},
        "e1_9_financial_effects":{"required":overall>=3},
    }

    return {
        "report_metadata":{"generated_at":_today(),"company":company_name,"site":site_name or f"{site_lat:.4f},{site_lon:.4f}","sector":sector,"entity_type":entity_type},
        "physical_climate_risk":risk,
        "emission_benchmarks":bench,
        "benchmark_comparison":comp,
        "csrd_applicability":{"entity_classification":csrd.get("classification",""),"first_reporting":csrd.get("first_reporting",""),
                              "core_mandatory":csrd.get("core_mandatory_standards",[]),"applicable_standards":csrd.get("all_applicable_standards",[])},
        "esrs_applicability_matrix":matrix,
        "location_specific_triggers":trigs,
        "esrs_e1_climate_change":e1,
        "recommendations":recs,
    }

# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION 2 — generate_executive_summary
# ═══════════════════════════════════════════════════════════════════════════

def generate_executive_summary(full_report: dict) -> str:
    """Extract key findings and format as narrative executive summary."""
    m = full_report.get("report_metadata",{})
    risk = full_report.get("physical_climate_risk",{})
    ov = risk.get("overall_risk",{})
    score, label = ov.get("score",3), ov.get("label",_rl(ov.get("score",3)))
    bench = full_report.get("emission_benchmarks",{})
    bv = bench.get("data",{}).get("average","N/A")
    csrd = full_report.get("csrd_applicability",{})
    recs = full_report.get("recommendations",[])
    lines = [
        f"## Executive Summary — {m.get('company','N/A')}",
        f"**Site:** {m.get('site','N/A')} | **Sector:** {m.get('sector','N/A')} | **Report:** {m.get('generated_at','N/A')}",
        "",
        "### Key Climate Risk",
        f"Overall physical climate risk: **{label}** (score: **{score}/5**)."]
    perils = [("Flood",risk.get("flood_risk",{}).get("class")),("Heat",risk.get("heat_risk",{}).get("class")),
              ("Drought",risk.get("drought_risk",{}).get("class")),("Storm",risk.get("storm_risk",{}).get("class")),
              ("Sea-level rise",risk.get("sea_level_rise_risk",{}).get("class")),("Wildfire",risk.get("wildfire_risk",{}).get("class"))]
    active = [f"{n}: {_rl(c)} ({c}/5)" for n,c in perils if c and c>=3]
    if active: lines.append("Elevated hazards: "+"; ".join(active))
    lines.append("")
    lines.append("### Key Emission Metrics")
    if bv!="N/A": lines.append(f"Sector benchmark: **{bv}**")
    comp = full_report.get("benchmark_comparison")
    if comp: lines.append(f"Your intensity: **{comp['own_intensity']}** ({comp['status'].replace('_',' ')} vs sector avg by {abs(comp['difference_pct']):.1f}%)")
    lines.append("")
    lines.append("### CSRD Readiness")
    lines.append(f"Classification: **{csrd.get('entity_classification','N/A')}** | First reporting: **{csrd.get('first_reporting','N/A')}**")
    lines.append(f"Mandatory standards: {', '.join(csrd.get('core_mandatory',[])) or 'TBD'}")
    lines.append("")
    lines.append("### Required Actions")
    if recs:
        for r in recs:
            ref = f" ({r.get('esrs_ref','')})" if r.get("esrs_ref") else ""
            lines.append(f"- **{r['priority'].upper()}** — {r['area'].replace('_',' ').title()}{ref}")
    else: lines.append("- No critical actions identified.")
    lines.append("")
    lines.append("---\n*Auto-generated executive summary.*")
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION 3 — generate_full_report_package
# ═══════════════════════════════════════════════════════════════════════════

async def generate_full_report_package(site_lat: float, site_lon: float, company_data: dict) -> dict:
    """Full reporting package — climate risk, emissions, ESRS E1–E5, S1–S4, TCFD, TNFD, SBTi, CSDDD, ESG."""
    sn = company_data.get("site_name","")
    sector = company_data.get("sector","manufacturing")
    cn = company_data.get("company_name","")
    emp = company_data.get("employees",500)
    rev = company_data.get("revenue_eur_m",100.0)
    etype = company_data.get("entity_type","large")
    regions = company_data.get("regions",["EU"])

    core = await generate_csrd_report(site_lat,site_lon,sn,sector,cn,company_data)
    dm = await _call("climate_csrd_mcp.data_sources.eurlex","get_double_materiality_assessment",sector=sector,revenue=rev,employees=emp)
    social = await _call("climate_csrd_mcp.data_sources.eurlex","get_esrs_social_standards")
    risk = core.get("physical_climate_risk",{})
    ov_score = risk.get("overall_risk",{}).get("score",3)

    # Supplementary reports (graceful fallback on error)
    supp = {}
    for mod, fn, kwargs in [
        ("climate_csrd_mcp.data_sources.tcfd","get_tcfd_report",{"sector":sector,"risk_score":ov_score}),
        ("climate_csrd_mcp.data_sources.tnfd","get_tnfd_report",{"sector":sector,"regions":regions}),
        ("climate_csrd_mcp.data_sources.sbti","check_sbti_target",{"sector":sector,"current_emissions":company_data.get("ghg_emissions_tco2e",5000)}),
        ("climate_csrd_mcp.data_sources.csddd","get_csddd_due_diligence",{"sector":sector,"regions":regions}),
    ]:
        try: supp[fn.replace("get_","").replace("_tcfd","_tcfd").replace("_tnfd","_tnfd")] = await _call(mod,fn,**kwargs)
        except Exception as e: supp[fn] = {"error":str(e)}

    esg = company_data.get("esg_score",50.0)
    try: rating = await _call("climate_csrd_mcp.data_sources.esg_rating","simulate_msci_rating",env_score=esg,social_score=esg,gov_score=esg,sector=sector)
    except Exception as e: rating = {"error":str(e)}

    sec_spec = {}
    if company_data.get("asset_type"):
        try: sec_spec["crrem_pathway"] = await _call("climate_csrd_mcp.data_sources.crrem","get_crrem_pathway",
                     asset_type=company_data["asset_type"],country=company_data.get("country","DE"),target_year=company_data.get("year_horizon",2030))
        except Exception: pass
    if company_data.get("building_energy_kwh_sqm"):
        try: sec_spec["energy_certificate"] = await _call("climate_csrd_mcp.data_sources.real_estate","get_energy_certificate",
                     lat=site_lat,lon=site_lon,energy_kwh=company_data["building_energy_kwh_sqm"],building_type=company_data.get("building_type","office"))
        except Exception: pass

    csrd_ctx = core.get("csrd_applicability",{})
    return {
        "executive_summary": generate_executive_summary(core),
        "detailed_report": {"core_csrd_e1":core,"esrs_e2_e5_double_materiality":dm,"esrs_s1_s4_social":social},
        "supplementary_reports":{**supp,"esg_rating":rating},
        "sector_specific": sec_spec,
        "csrd_context":{"entity_classification":csrd_ctx.get("entity_classification",""),
                        "first_reporting_year":csrd_ctx.get("first_reporting",""),
                        "applicable_standards":csrd_ctx.get("applicable_standards",[]),
                        "core_mandatory":csrd_ctx.get("core_mandatory",[])},
        "data_sources":{"climate_risk":"Copernicus + DWD + UBA","emissions":"EU ETS",
                        "csrd":"EUR-Lex Directive 2022/2464 + ESRS Reg 2023/2772",
                        "tcfd":"TCFD/ISSB IFRS S2","tnfd":"TNFD LEAP v0.4",
                        "sbti":"SBTi Corporate Manual v2.0","csddd":"CSDDD Directive 2024/1760",
                        "esg":"MSCI ESG / Sustainalytics methodology"},
    }

# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION 4 — verify_report_completeness
# ═══════════════════════════════════════════════════════════════════════════

def _check_e1(report: dict) -> tuple[list[str], list[str]]:
    e1 = report.get("esrs_e1_climate_change",{})
    covered, missing = [], []
    for dp in _ESRS_DP["E1"]:
        key = dp.split(" ")[0].replace("-","_").lower()
        match = [k for k in e1 if key in k.replace("-","_")]
        if match and isinstance(e1[match[0]],dict) and e1[match[0]].get("required"): covered.append(dp)
        else: missing.append(dp)
    return covered, missing

def verify_report_completeness(report_dict: dict) -> dict:
    """Check ESRS data point coverage; return coverage score, gaps, and remediation."""
    csrd = report_dict.get("csrd_applicability",{})
    applicable = csrd.get("applicable_standards",[])
    if not applicable:
        matrix = report_dict.get("esrs_applicability_matrix",[])
        applicable = list(dict.fromkeys(e["standard"] for e in matrix))
    if not applicable: applicable = list(_ESRS_DP.keys())

    total_covered = total_possible = 0
    gaps: list[str] = []
    cov_by_std: dict[str,dict] = {}

    for std in applicable:
        cats = _ESRS_DP.get(std,[])
        if not cats: continue
        if std == "E1": covered, missing = _check_e1(report_dict)
        else:
            dm = report_dict.get("esrs_e2_e5_double_materiality",{})
            social = report_dict.get("esrs_s1_s4_social",{})
            std_map = {"E2":"Pollution","E3":"Water","E4":"Biodiversity","E5":"Circular Economy"}
            if std in std_map and dm and std_map[std] in str(dm.get("material_topics",[])):
                covered, missing = cats.copy(), []
            elif std in ("S1","S2","S3","S4"):
                sd = social.get("standards",{}).get(std,{})
                covered, missing = (cats.copy(), []) if sd and sd.get("total_data_points",0)>0 else ([], cats.copy())
            else: covered, missing = [], cats.copy()
        total_covered += len(covered); total_possible += len(cats)
        gaps.extend(missing)
        cov_by_std[std] = {"covered":len(covered),"total":len(cats),
                           "percentage":round(len(covered)/len(cats)*100,1) if cats else 0,"missing":missing}

    pct = round(total_covered/total_possible*100,1) if total_possible else 0
    remediations = {g: f"Collect data and disclose per ESRS {g.split()[0]}" for g in gaps[:10]}
    for g in gaps[:10]:
        if "Transition" in g: remediations[g]="Develop a climate transition plan aligned with ESRS requirements"
        elif "GHG" in g: remediations[g]="Compile Scope 1,2,3 GHG inventory and engage third-party verification"
        elif "Adaptation" in g or "Financial" in g: remediations[g]="Conduct climate scenario analysis for adaptation needs and financial effects"
        elif "Target" in g: remediations[g]="Set measurable, time-bound climate targets (e.g., SBTi-aligned)"

    return {"coverage_by_standard":cov_by_std,"overall_coverage_score":pct,
            "total_data_points_covered":total_covered,"total_data_points_applicable":total_possible,
            "gaps":gaps,"top_remediation_suggestions":[remediations[g] for g in gaps[:8]],
            "report_generated_at":report_dict.get("report_metadata",{}).get("generated_at",_today())}
