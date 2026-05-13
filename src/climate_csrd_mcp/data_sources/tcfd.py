"""
TCFD Report Generator — 11 recommended disclosures across 4 pillars
(Governance, Strategy, Risk Management, Metrics & Targets) with
sector-specific transition risks and CSRD/ESRS E1 cross-references.

References: TCFD Final Recommendations (2017), TCFD Status Report (2023),
ESRS E1 — Climate Change (EFRAG, 2023).
"""

from __future__ import annotations
from typing import Any
from ..utils import (
    CSRD_DISCLAIMER, get_rcp_scenario, get_esrs_ref, risk_label, financial_risk_estimate, today_iso,
)

# ── 11 TCFD Disclosures ──────────────────────────────────────────────────
TCFD_DISCLOSURES: list[dict[str, str]] = [
    {"pillar":"Governance","id":"G-a","title":"Board oversight of climate risks and opportunities","description":"Describe the board's oversight of climate-related risks and opportunities.","esrs_e1":"E1-1, E1-2"},
    {"pillar":"Governance","id":"G-b","title":"Management's role in assessing climate risks","description":"Describe management's role in assessing and managing climate-related risks.","esrs_e1":"E1-2"},
    {"pillar":"Strategy","id":"S-a","title":"Climate risks and opportunities identified","description":"Describe climate risks and opportunities over short, medium, long term.","esrs_e1":"E1-1, E1-7"},
    {"pillar":"Strategy","id":"S-b","title":"Impact on business, strategy, financial planning","description":"Describe impact of climate risks on business strategy and financial planning.","esrs_e1":"E1-7, E1-9"},
    {"pillar":"Strategy","id":"S-c","title":"Resilience of strategy under climate scenarios","description":"Describe strategic resilience considering different climate scenarios.","esrs_e1":"E1-1, E1-7"},
    {"pillar":"Risk Management","id":"RM-a","title":"Process for identifying climate risks","description":"Describe processes for identifying and assessing climate-related risks.","esrs_e1":"E1-2, E1-7"},
    {"pillar":"Risk Management","id":"RM-b","title":"Process for managing climate risks","description":"Describe processes for managing climate-related risks.","esrs_e1":"E1-2, E1-3"},
    {"pillar":"Risk Management","id":"RM-c","title":"Integration into overall risk management","description":"Describe integration with enterprise risk management (ERM).","esrs_e1":"E1-1, G1-1"},
    {"pillar":"Metrics & Targets","id":"MT-a","title":"Climate-related metrics","description":"Disclose metrics used to assess climate risks and opportunities.","esrs_e1":"E1-4, E1-5"},
    {"pillar":"Metrics & Targets","id":"MT-b","title":"Scope 1, 2, 3 GHG emissions","description":"Disclose Scope 1, 2, and applicable Scope 3 GHG emissions.","esrs_e1":"E1-5"},
    {"pillar":"Metrics & Targets","id":"MT-c","title":"Climate-related targets","description":"Describe targets used to manage climate risks and opportunities.","esrs_e1":"E1-3, E1-1"},
]

# ── Sector-Specific Transition Risk Tables ───────────────────────────────
SECTOR_TRANSITION_RISKS: dict[str, list[dict[str, Any]]] = {
    "manufacturing": [
        {"cat":"Policy & Legal","s":4,"n":"EU ETS expansion, product efficiency regulations, extended producer responsibility."},
        {"cat":"Technology","s":3,"n":"Low-carbon process transition; capex for circular production."},
        {"cat":"Market","s":3,"n":"Customer demand shift to sustainable products; green premium potential."},
        {"cat":"Reputation","s":2,"n":"Moderate scrutiny on supply chain emissions and material sourcing."},
        {"cat":"Legal","s":3,"n":"Litigation risk from emissions disclosures and greenwashing claims."},
    ],
    "energy": [
        {"cat":"Policy & Legal","s":5,"n":"EU ETS, CBAM, renewable mandates, fossil fuel phase-out policies — highest exposure."},
        {"cat":"Technology","s":5,"n":"Rapid shift to renewables/storage; stranded asset risk for fossil infrastructure."},
        {"cat":"Market","s":4,"n":"Falling LCOE of renewables erodes fossil competitiveness; PPA market transformation."},
        {"cat":"Reputation","s":5,"n":"Divestment movements, activist campaigns, ESG fund exclusion thresholds."},
        {"cat":"Legal","s":4,"n":"Climate liability claims, regulatory non-compliance, disclosure failures."},
    ],
    "construction": [
        {"cat":"Policy & Legal","s":4,"n":"Energy performance standards (EPBD), embodied carbon regulations, green certification."},
        {"cat":"Technology","s":3,"n":"Low-carbon concrete/steel/timber adoption; BIM-driven efficiency."},
        {"cat":"Market","s":4,"n":"Green-certified building demand; investor premium for low-carbon assets."},
        {"cat":"Reputation","s":3,"n":"Scrutiny on embodied emissions; community opposition to carbon-intensive projects."},
        {"cat":"Legal","s":2,"n":"Building code non-compliance and green marketing claims."},
    ],
    "transport": [
        {"cat":"Policy & Legal","s":4,"n":"ZEV mandates, fuel efficiency standards, emission zones, SAF quotas."},
        {"cat":"Technology","s":4,"n":"EV transition, hydrogen HDV, fleet electrification infrastructure."},
        {"cat":"Market","s":3,"n":"Green logistics demand; carbon-conscious procurement."},
        {"cat":"Reputation","s":3,"n":"Scrutiny on fleet emissions; 'flight shaming' in aviation."},
        {"cat":"Legal","s":2,"n":"Emission zone violations, fuel compliance, reporting accuracy."},
    ],
    "agriculture": [
        {"cat":"Policy & Legal","s":4,"n":"CAP reform, methane targets, fertiliser regulations, deforestation-free rules."},
        {"cat":"Technology","s":3,"n":"Precision agriculture, alternative proteins, methane capture."},
        {"cat":"Market","s":4,"n":"Sustainable food demand; carbon farming credits; retailer pressure."},
        {"cat":"Reputation","s":4,"n":"Deforestation, water use, pesticide impacts, Scope 3 livestock emissions."},
        {"cat":"Legal","s":3,"n":"Fertiliser runoff, deforestation compliance, biodiversity offsets."},
    ],
    "finance": [
        {"cat":"Policy & Legal","s":4,"n":"EU Taxonomy, SFDR, ECB stress tests, Pillar III, Basel ESG capital."},
        {"cat":"Technology","s":2,"n":"Green fintech and climate analytics — limited direct tech risk."},
        {"cat":"Market","s":4,"n":"Green/ESG AUM growth; credit repricing for high-emission sectors."},
        {"cat":"Reputation","s":4,"n":"Financed emissions scrutiny; fossil fuel lending controversies."},
        {"cat":"Legal","s":3,"n":"Greenwashing litigation; fiduciary duty on climate disclosure."},
    ],
}

# Default fallback for any sector not listed above
_TRANSITION_DEFAULTS: list[dict[str, Any]] = SECTOR_TRANSITION_RISKS["manufacturing"]

SECTOR_GOV_PROFILES: dict[str, dict[str, Any]] = {
    "manufacturing": dict(size=14, cc=4, freq="Quarterly", roles=["CSO","VP Env Affairs","Head Risk","VP Supply Chain"]),
    "energy": dict(size=12, cc=5, freq="Monthly", roles=["CSO","Dir Climate Strategy","CRO","VP Low-Carbon","Head Regulatory"]),
    "finance": dict(size=15, cc=4, freq="Monthly", roles=["CSO","CRO","Head ESG","Head Climate Risk"]),
    "construction": dict(size=12, cc=3, freq="Quarterly", roles=["Dir Sust. Constr.","Head Risk","VP ESG"]),
    "transport": dict(size=11, cc=3, freq="Quarterly", roles=["CSO","Fleet Decarb Dir","Head Corp Affairs"]),
    "agriculture": dict(size=10, cc=3, freq="Quarterly", roles=["Dir Reg. Ag.","CSO","Head SC & Env."]),
    "technology": dict(size=10, cc=3, freq="Quarterly", roles=["VP Sust.","Chief Ethics","Head Climate"]),
    "healthcare": dict(size=12, cc=3, freq="Semi-annual", roles=["CSO","Dir Env Ops","Head SC Resilience"]),
    "retail": dict(size=11, cc=3, freq="Quarterly", roles=["VP Sust.","Dir SC Sust.","Head Prod Compl."]),
}
_GOV_DEFAULT = dict(size=12, cc=3, freq="Quarterly", roles=["CSO","Head Risk Management"])

# ── Helpers ──────────────────────────────────────────────────────────────
def _norm(sector: str) -> str:
    s = sector.lower().strip().replace(" ","_")
    known = set(SECTOR_TRANSITION_RISKS.keys())
    if s in known:
        return s
    alias = {"industrials":"manufacturing","industrial":"manufacturing","utilities":"energy","electricity":"energy",
             "oil_and_gas":"energy","renewables":"energy","banking":"finance","insurance":"finance",
             "real_estate":"construction","property":"construction","logistics":"transport","shipping":"transport",
             "aviation":"transport","farming":"agriculture","food_and_beverage":"agriculture","tech":"technology",
             "it":"technology","pharma":"healthcare","pharmaceutical":"healthcare","retail":"retail"}
    return alias.get(s, "manufacturing")

def _sev(s: int) -> str:
    return {1:"Low",2:"Low-Moderate",3:"Moderate",4:"High",5:"Very High"}.get(s,"Unknown")

def _prob(s: int) -> str:
    return {1:"Very Unlikely (<5%)",2:"Unlikely (5-20%)",3:"Possible (20-50%)",4:"Likely (50-80%)",5:"Very Likely (>80%)"}.get(s,"Possible")

def _timeframe(cat: str, score: int) -> str:
    if cat in ("Policy & Legal","Legal") and score >=4:
        return "Short-term (0-3y)"
    if cat in ("Technology","Market"):
        return "Medium-term (3-10y)"
    return ("Short-to-medium (0-5y)" if score>=4 else "Medium-to-long (5-15y)")

def _fin_impact(cat: str, sec: str) -> str:
    m = {"Policy & Legal":{"energy":"Carbon cost €40-100/tCO₂ by 2030","default":"Compliance cost increase"},
         "Technology":{"energy":"Stranded asset risk for fossil infrastructure","default":"R&D + capex investment"},
         "Market":{"energy":"Declining fossil vs growing renewables revenue","default":"Low-carbon competitor pressure"},
         "Reputation":{"energy":"Higher financing cost from ESG exclusions","default":"Brand value risk"},
         "Legal":{"energy":"Climate liability exposure","default":"Regulatory penalty risk"}}
    return m.get(cat,{}).get(sec, m.get(cat,{}).get("default","Financial impact to be assessed"))

def _gov_profile(sec: str) -> dict:
    return SECTOR_GOV_PROFILES.get(_norm(sec), _GOV_DEFAULT)

def _trans_risks(sec: str) -> list:
    return SECTOR_TRANSITION_RISKS.get(_norm(sec), _TRANSITION_DEFAULTS)

# ── Public: Governance (G-a, G-b) ────────────────────────────────────────
def get_tcfd_governance(sector: str, board_oversight: str = "yes") -> dict[str, Any]:
    """TCFD Governance disclosures — board oversight and management's role (G-a, G-b)."""
    prof = _gov_profile(sector)
    has = board_oversight.strip().lower() in ("yes","true","y","1")
    bn = (f"The board oversees climate risks through a dedicated "
          f"{'Sustainability' if sector in ('energy','finance') else 'ESG'} Committee "
          f"({prof['cc']} members; {prof['freq']} reviews). Board charter includes climate oversight."
          if has else
          f"Climate risks managed at executive level; board receives annual briefings. "
          f"Enhanced oversight under consideration.")
    mn = (f"Led by {prof['roles'][0]} reporting to {'CEO' if has else 'CFO'}. "
          f"Cross-functional Climate Risk Working Group meets {prof['freq'].lower()}.")
    return {
        "pillar":"Governance","disclosures":["G-a","G-b"],
        "board_oversight":{"has_dedicated":has,"committee_size":prof['cc'],"board_size":prof['size'],
                           "frequency":prof['freq'],"narrative":bn},
        "management_role":{"responsible":prof['roles'][0],
                           "reporting":f"Reports to {'CEO' if has else 'CFO'}",
                           "working_group":f"Cross-functional ({prof['freq']})",
                           "responsibilities":["Annual risk ID","Scenario analysis","Target monitoring",
                                               "ERM integration","Disclosure preparation","Financial impact assessment"]},
        "recommended_structure":f"Board → Climate Committee → {prof['roles'][0]} → Risk Working Group",
        "esrs_cross_reference":{"G-a":get_esrs_ref("E1","E1-1 / E1-2"),"G-b":get_esrs_ref("E1","E1-2")},
    }

# ── Public: Strategy (S-a, S-b, S-c) ─────────────────────────────────────
def get_tcfd_strategy(sector: str, risk_score: int, scenario: str = "rcp_4.5", horizon: int = 2030) -> dict[str, Any]:
    """TCFD Strategy — scenario analysis, transition/physical risk, resilience (S-a, S-b, S-c)."""
    sec = _norm(sector)
    sc = get_rcp_scenario(scenario)
    temp_h = sc.get("temp_rise_c",{}).get("by_2100" if horizon>=2100 else "by_2050",1.8)
    sev = _sev(risk_score)

    t_table = [{"category":r["cat"],"score":r["s"],"severity":_sev(r["s"]),
                "timeframe":_timeframe(r["cat"],r["s"]),"description":r["n"],
                "financial_implication":_fin_impact(r["cat"],sec)} for r in _trans_risks(sec)]

    if risk_score >= 4:
        resilience = (f"Significant resilience challenges at {sev} risk. Adaptation "
                      f"measures (diversification, rerouting, hardening) needed within 2-5y.")
    elif risk_score >= 3:
        resilience = f"Moderate gaps — strategic adjustments needed as risks intensify toward {horizon}."
    else:
        resilience = "Adequate resilience for current risk level. Continue monitoring."

    return {
        "pillar":"Strategy","disclosures":["S-a","S-b","S-c"],
        "scenario_analysis":{
            "scenario":scenario,"label":sc.get("label",scenario),"horizon":horizon,
            "temp_rise_c":temp_h,
            "narrative":(
                f"Under {sc.get('label',scenario)}, global temperature rise ~{temp_h}°C by {horizon}. "
                f"CO₂ ~{sc.get('co2_concentration_ppm',{}).get('by_2050','N/A')}ppm by 2050. "
                f"Sea level ~{sc.get('sea_level_rise_cm',{}).get('by_2050','N/A')}cm by 2050. "
                f"Probability: {sc.get('probability','See IPCC AR6')}.")},
        "transition_risks":t_table,
        "physical_risks":{"score":risk_score,"label":risk_label(risk_score),"severity":sev,
                          "narrative":f"{sev} physical risk for {sec}: supply chain disruption, asset damage, heat stress."},
        "business_resilience":{"assessment":resilience,"risk_level":risk_score,
                               "adaptation_gap":"High" if risk_score>=4 else ("Moderate" if risk_score>=3 else "Low")},
        "financial_planning":{
            "opex_impact":f"+0.{risk_score}% to +{risk_score}.0% annual opex (adaptation)",
            "capex_impact":f"{2+risk_score}–{5+risk_score*3}% of total capex (decarbonisation + adaptation)",
            "revenue_exposure":f"~{risk_score*10}–{risk_score*20}% revenue exposed to physical risk",
            "planning_horizon":horizon,"scenario_used":scenario},
        "esrs_cross_reference":{"S-a":get_esrs_ref("E1","E1-1 / E1-7"),"S-b":get_esrs_ref("E1","E1-7 / E1-9"),
                                "S-c":get_esrs_ref("E1","E1-1 / E1-7")},
    }

# ── Public: Risk Management (RM-a, RM-b, RM-c) ──────────────────────────
def get_tcfd_risk_management(sector: str, risk_score: int, flood_risk: int = 2,
                            heat_risk: int = 2, drought_risk: int = 2) -> dict[str, Any]:
    """TCFD Risk Management — risk ID, prioritisation, mitigation, ERM (RM-a, RM-b, RM-c)."""
    sec = _norm(sector)
    register = [{"type":"Transition","category":r["cat"],"score":r["s"],"severity":_sev(r["s"]),
                 "probability":_prob(r["s"]),"timeframe":_timeframe(r["cat"],r["s"]),"description":r["n"]}
                for r in _trans_risks(sec)]
    for name, s in [("Flood / Pluvial",flood_risk),("Heat Stress",heat_risk),("Drought / Water Stress",drought_risk)]:
        if s >= 1:
            register.append({"type":"Physical","category":name,"score":s,"severity":_sev(s),
                             "probability":_prob(s),
                             "timeframe":"Short-to-medium (0-5y)" if s>=3 else "Medium-to-long (5-15y)",
                             "description":f"{name} score {s}/5 — operational disruption, asset damage risk for {sec}."})
    register.sort(key=lambda r: r["score"], reverse=True)

    return {
        "pillar":"Risk Management","disclosures":["RM-a","RM-b","RM-c"],
        "risk_identification":{
            "process":("Structured annual process aligned with TCFD + ISO 14091: "
                       "horizon scanning, IPCC physical hazard screening, stakeholder engagement, expert workshops."),
            "frequency":"Annual (quarterly updates for emerging risks)","methodology":"TCFD + ISO 14091 + ERM"},
        "risk_register":register,
        "risk_prioritisation":{"method":"5×5 severity × probability matrix","total":len(register),
                               "high_priority":[r for r in register if r["score"]>=4]},
        "mitigation_strategies":{
            "transition":{
                "policy_compliance":"Regulatory monitoring + policy engagement.",
                "technology":"Low-carbon technology roadmap with R&D/capex allocation.",
                "market_adaptation":"Low-carbon product diversification; supplier engagement on targets."},
            "physical":{
                "asset_protection":"Site-level risk assessments; flood defences, cooling, water efficiency.",
                "business_continuity":"BCP updates for climate disruption; insurance coverage."}},
        "erm_integration":{"status":"Integrated",
            "narrative":("Climate risks embedded in ERM as cross-cutting category. "
                        "Business units include climate in annual risk registers. "
                        "Quarterly Risk Committee review with escalation for risks ≥15/25."),
            "framework":"COSO ERM 2017"},
        "esrs_cross_reference":{"RM-a":get_esrs_ref("E1","E1-2 / E1-7"),
                                "RM-b":get_esrs_ref("E1","E1-2 / E1-3"),
                                "RM-c":get_esrs_ref("E1","E1-1 / G1-1")},
    }

# ── Public: Metrics & Targets (MT-a, MT-b, MT-c) ─────────────────────────
def get_tcfd_metrics_targets(sector: str, emission_scopes_dict: dict[str,float]|None = None) -> dict[str, Any]:
    """TCFD Metrics & Targets — GHG emissions, metrics, SBTi-aligned targets (MT-a, MT-b, MT-c)."""
    sec = _norm(sector)
    DEFAULT = {"manufacturing":(45_000,32_000,180_000),"energy":(120_000,15_000,350_000),
               "construction":(28_000,22_000,220_000),"transport":(65_000,12_000,95_000),
               "agriculture":(80_000,15_000,250_000),"finance":(500,2_000,500_000),
               "technology":(3_000,15_000,80_000),"healthcare":(8_000,12_000,60_000),
               "retail":(15_000,25_000,200_000)}
    s1,s2,s3 = emission_scopes_dict.values() if emission_scopes_dict else DEFAULT.get(sec,(45_000,32_000,180_000))
    if emission_scopes_dict:
        s1 = emission_scopes_dict.get("scope1",0)
        s2 = emission_scopes_dict.get("scope2",0)
        s3 = emission_scopes_dict.get("scope3",0)
    total = s1 + s2 + s3
    rate = 4.2 if sec in ("energy","manufacturing") else 3.5
    target_2030_s12 = round((s1+s2) * 0.58)  # 42% reduction
    target_2030_s3 = round(s3 * 0.75)

    return {
        "pillar":"Metrics & Targets","disclosures":["MT-a","MT-b","MT-c"],
        "ghg_emissions":{
            "scope1":{"tco2e":s1,"description":"Direct from owned sources (combustion, process, fugitive, fleet)","methodology":"GHG Protocol Corporate Standard"},
            "scope2":{"tco2e":s2,"description":"Purchased electricity/steam/heat (location-based)","methodology":"GHG Protocol Scope 2 Guidance"},
            "scope3":{"tco2e":s3,"description":"Value chain (purchased goods, transport, investments, product use)","methodology":"GHG Protocol Corporate Value Chain",
                      "categories_included":["1 — Purchased goods","3 — Fuel & energy","4 — Upstream transport","6 — Business travel","9 — Downstream transport","11 — Product use"]},
            "total_gross_tco2e":total,"reporting_year":2025,"base_year":2024,
            "verification":"Limited assurance (third-party)"},
        "climate_metrics":{
            "emission_intensity":{"value":round(total/100,1),"unit":"tCO₂e/€M revenue","trend":"Decreasing" if sec in("technology","finance","healthcare") else "Stable"},
            "energy_intensity":{"value":round(total*0.45/100,1),"unit":"MWh/€M revenue","renewable_share_pct":45 if sec in("technology","finance") else 25},
            "internal_carbon_price":{"price_eur_tco2":85,"scope":"Scope 1+2 (EU)","year":2025,"planned":"€100/t by 2026, €150/t by 2030",
                                     "purpose":"Investment appraisal, risk assessment, business cases"},
            "peer_comparison":{"company":round(total/100,1),"sector_avg":round(total*1.15/100,1),
                               "best_in_class":round(total*0.55/100,1),"unit":"tCO₂e/€M revenue"}},
        "targets":{
            "near_term":[
                {"name":"GHG reduction Scope 1+2","base":2024,"target":2030,"reduction_pct":42,
                 "trajectory":f"~{round(total*0.58)} tCO₂e","status":"Approved" if rate>=4.2 else "Under review"},
                {"name":"GHG reduction Scope 3","base":2024,"target":2030,"reduction_pct":25,
                 "trajectory":f"~{round(s3*0.75)} tCO₂e","status":"Target setting in progress"}],
            "long_term":[{"name":"Net-zero","target_year":2050,"scope":"Scope 1+2+3",
                          "milestones":{2030:"50% vs base",2040:"75% vs base"},"status":"Committed"}],
            "sbti_alignment":{"status":"Committed to SBTi" if rate>=4.2 else "SBTi commitment developing",
                              "annual_reduction_pct":rate,"sbti_threshold":4.2,"aligned":rate>=4.2,
                              "note":f"SBTi requires 4.2% annual linear reduction for 1.5°C alignment. "
                                     f"This trajectory is {'aligned' if rate>=4.2 else 'not yet aligned'}."}},
        "esrs_cross_reference":{"MT-a":get_esrs_ref("E1","E1-4 / E1-5"),"MT-b":get_esrs_ref("E1","E1-5"),
                                "MT-c":get_esrs_ref("E1","E1-3 / E1-1"),"carbon_pricing":get_esrs_ref("E1","E1-8")},
    }

# ── Public: Full TCFD Report ─────────────────────────────────────────────
def get_tcfd_report(
    company_name: str, sector: str, revenue_eur_m: float,
    emissions_scopes: dict[str,float]|None = None,
    climate_risk: dict[str,int]|None = None, country: str = "EU",
) -> dict[str, Any]:
    """Full TCFD-aligned report — all 11 disclosures across 4 pillars with ESRS E1 cross-references."""
    sec = _norm(sector)
    cr = climate_risk or {"overall":3,"flood":2,"heat":3,"drought":2}
    rs = cr.get("overall",3)
    governance = get_tcfd_governance(sec)
    strategy = get_tcfd_strategy(sec, rs)
    risk_mgmt = get_tcfd_risk_management(sec, rs, cr.get("flood",2), cr.get("heat",3), cr.get("drought",2))
    metrics = get_tcfd_metrics_targets(sec, emissions_scopes)
    financial = financial_risk_estimate(rs, sec, revenue_eur_m)

    return {
        "meta":{"title":f"TCFD Climate Disclosure — {company_name}","company":company_name,
                "sector":sec,"country":country,"reporting_year":2025,"report_date":today_iso(),
                "framework":"TCFD (2017)","csrd_framework":"CSRD / ESRS E1",
                "revenue_eur_m":revenue_eur_m,"risk_score":rs,"risk_label":risk_label(rs),
                "note":"Modelled estimates — not audited disclosure."},
        "governance":governance,"strategy":strategy,
        "risk_management":risk_mgmt,"metrics_and_targets":metrics,
        "financial_impacts":{"physical_risk_estimate":financial,"currency":"EUR",
                             "methodology":"IPCC AR6, EIOPA 2022, NGFS sector loss tables"},
        "esrs_mapping":tcfd_to_esrs_mapping(),
        "disclaimer":f"{CSRD_DISCLAIMER} Prepared per TCFD (2017) and ESRS E1 for dual-reporting.",
        "disclosures_covered":[d["id"] for d in TCFD_DISCLOSURES],
    }

# ── Public: TCFD → ESRS Mapping ─────────────────────────────────────────
def tcfd_to_esrs_mapping() -> dict[str, Any]:
    """Map all 11 TCFD disclosures to ESRS E1 data points for dual-reporting companies."""
    table = []
    for d in TCFD_DISCLOSURES:
        table.append({"tcfd_id":d["id"],"tcfd_title":d["title"],"pillar":d["pillar"],
                      "esrs_e1_ref":d["esrs_e1"],
                      "mapping_note":{
            "G-a":"E1-1 (governance) + E1-2 (policies) jointly cover G-a.",
            "G-b":"E1-2 management responsibility → direct equivalent.",
            "S-a":"E1-1 (transition risks) + E1-7 (financial effects).",
            "S-b":"E1-7 (financial effects) + E1-9 (opportunities).",
            "S-c":"E1-1 requires scenario analysis; TCFD more prescriptive on multi-scenario resilience.",
            "RM-a":"E1-2 (risk processes) + E1-7 (risk identification).",
            "RM-b":"E1-2 (policies) + E1-3 (mitigation targets).",
            "RM-c":"E1-1 (ERM integration) + G1-1 (internal control).",
            "MT-a":"E1-4 (energy metrics) + E1-5 (GHG emissions).",
            "MT-b":"E1-5 detailed Scope 1-3 disclosure — add location/market-based breakdown.",
            "MT-c":"E1-3 (targets) — add methodology, milestones, SBTi status.",
        }.get(d["id"],"Direct mapping to ESRS E1.")})

    gaps = [
        {"tcfd_id":"S-c","gap":"ESRS E1-1 less prescriptive on multi-scenario resilience than TCFD.",
         "recommendation":"Use TCFD S-c narrative; connect explicitly to transition plan."},
        {"tcfd_id":"MT-b","gap":"ESRS E1-5 requires location vs market-based Scope 2 and biogenic CO₂.",
         "recommendation":"Supplement TCFD data with E1-5 breakdowns."},
        {"tcfd_id":"MT-c","gap":"ESRS E1-3 requires target methodology, milestones, SBTi status.",
         "recommendation":"Enrich TCFD MT-c with E1-3 granularity."},
    ]

    return {
        "mapping_table":table,
        "summary":{"total":11,"per_pillar":{"Governance":2,"Strategy":3,"Risk Management":3,"Metrics & Targets":3},
                   "direct_equivalents":8,"partial_coverage":3},
        "dual_reporting_guidance":(
            "Produce unified climate report around 4 TCFD pillars. "
            "Annotate each disclosure with ESRS E1 data point. "
            "Add ESRS-specific granularity for E1-3 (targets), E1-5 (emissions), E1-8 (carbon pricing)."),
        "coverage_gaps":gaps,
    }
