"""CSDDD (Corporate Sustainability Due Diligence Directive, EU 2024/1760).

Assessment: trigger checks, HR/env risk scoring, action plans, CSRD overlay.
Sources: CSDDD Art.5-22, ILO, HRW, EPI 2024, ESRS S1-S3/E1-E5.
"""

import asyncio
from typing import Any
from ..cache import get_cache
from ..utils import risk_label, risk_color, get_esrs_ref, today_iso

# ─── High-Risk Sectors (CSDDD Annex I Part I, NACE categories A/B/C/F) ─
HIGH_RISK_SECTORS: dict[str, str] = {
    "textiles": "C", "agriculture": "A", "fishing": "A", "forestry": "A",
    "minerals": "B", "quarrying": "B", "metal_ore_mining": "B", "coal_mining": "B",
    "petroleum": "B", "construction": "F", "manufacturing": "C",
    "food_processing": "C", "electronics": "C", "battery": "C", "chemicals": "C",
}
SECTOR_CATS: dict[str, str] = {s: f"hr_cat_{c}" for s, c in HIGH_RISK_SECTORS.items()}
for s in ["energy", "transport", "retail", "finance", "technology",
          "healthcare", "real_estate", "hospitality", "education"]:
    SECTOR_CATS[s] = "default"

# ─── CSDDD Implementation Thresholds ───────────────────────────────────
CSDDD_THRESHOLDS: list[dict[str, Any]] = [
    {"wave": 1, "year": 2027, "min_emp": 5000, "min_rev_m": 1500,
     "label": "Wave 1 — €1500M+ turnover, 5000+ employees"},
    {"wave": 2, "year": 2028, "min_emp": 3000, "min_rev_m": 900,
     "label": "Wave 2 — €900M+ turnover, 3000+ employees"},
    {"wave": 3, "year": 2029, "min_emp": 500, "min_rev_m": 150,
     "label": "Wave 3 — €150M+ turnover, 500+ employees"},
]

# ─── Human Rights Risk Index by Region (1-5, sources: ILO, HRW, Freedom House, ITUC)
REGION_HR: dict[str, dict[str, int]] = {
    "northern_europe":{"o":1,"c":1,"f":1,"fr":1,"d":1}, "western_europe":{"o":1,"c":1,"f":1,"fr":1,"d":2},
    "southern_europe":{"o":2,"c":2,"f":1,"fr":2,"d":2}, "eastern_europe":{"o":3,"c":2,"f":3,"fr":3,"d":3},
    "north_america":{"o":2,"c":2,"f":2,"fr":2,"d":2}, "central_america":{"o":4,"c":4,"f":3,"fr":3,"d":4},
    "south_america":{"o":3,"c":3,"f":3,"fr":3,"d":3}, "north_africa":{"o":4,"c":4,"f":3,"fr":4,"d":4},
    "sub_saharan_africa":{"o":5,"c":5,"f":4,"fr":4,"d":4}, "middle_east":{"o":4,"c":3,"f":5,"fr":5,"d":4},
    "central_asia":{"o":4,"c":4,"f":4,"fr":4,"d":3}, "south_asia":{"o":5,"c":5,"f":4,"fr":4,"d":5},
    "southeast_asia":{"o":4,"c":4,"f":4,"fr":3,"d":3}, "east_asia":{"o":3,"c":2,"f":3,"fr":4,"d":3},
    "oceania":{"o":2,"c":1,"f":2,"fr":1,"d":3},
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

# ─── Environmental Risk Index by Region (1-5, sources: EPI 2024, Germanwatch, UNEP)
REGION_ENV: dict[str, dict[str, int]] = {
    "northern_europe":  {"o":1,"cl":2,"b":1,"p":1,"ci":1}, "western_europe":  {"o":2,"cl":2,"b":2,"p":2,"ci":2},
    "southern_europe":  {"o":3,"cl":4,"b":3,"p":2,"ci":2}, "eastern_europe":  {"o":3,"cl":3,"b":3,"p":4,"ci":3},
    "north_america":    {"o":3,"cl":3,"b":3,"p":3,"ci":3}, "central_america": {"o":4,"cl":4,"b":4,"p":4,"ci":4},
    "south_america":    {"o":4,"cl":3,"b":5,"p":4,"ci":4}, "north_africa":    {"o":4,"cl":5,"b":4,"p":4,"ci":4},
    "sub_saharan_africa":{"o":5,"cl":5,"b":4,"p":4,"ci":5}, "middle_east":     {"o":4,"cl":5,"b":4,"p":4,"ci":3},
    "central_asia":     {"o":4,"cl":4,"b":3,"p":5,"ci":4}, "south_asia":      {"o":5,"cl":5,"b":4,"p":5,"ci":4},
    "southeast_asia":   {"o":4,"cl":4,"b":5,"p":4,"ci":3}, "east_asia":       {"o":3,"cl":3,"b":3,"p":4,"ci":2},
    "oceania":          {"o":3,"cl":3,"b":4,"p":2,"ci":2},
}

# ─── Due Diligence Phases (CSDDD Articles 5-11) ───────────────────────
DD_PHASES: list[dict[str, Any]] = [
    {"p": 1, "s": "Integration into policies & mgmt systems",
     "a": ["Adopt DD policy (Art.5)", "Governance integration", "Appoint responsible body",
           "Supplier code of conduct"],
     "e": ["S1-1", "G1-1", "E1-2", "E2-1"], "m": 6},
    {"p": 2, "s": "Identification & assessment of adverse impacts",
     "a": ["Map ops + value chain", "ID HR impacts", "ID env impacts",
           "Prioritise severity/likelihood", "Stakeholder engagement"],
     "e": ["S1-2", "S2-2", "S3-2", "E1-7", "E2-5"], "m": 12},
    {"p": 3, "s": "Prevention & mitigation",
     "a": ["Prevention action plan", "Contractual cascading", "SME capacity building",
           "Suspend non-compliant partners", "Climate transition plan (1000+ emp)"],
     "e": ["S1-4", "S2-4", "S3-4", "E1-1"], "m": 18},
    {"p": 4, "s": "Remediation of adverse impacts",
     "a": ["Provide/contribute remediation", "Grievance mechanism", "Stakeholder dialogue",
           "Track remediation effectiveness"],
     "e": ["S1-3", "S2-3", "S3-3"], "m": 24},
    {"p": 5, "s": "Monitoring & effectiveness tracking",
     "a": ["Periodic assessments", "KPI tracking", "Annual risk update", "Adjust measures"],
     "e": ["S1-5", "S2-5", "S3-5", "E1-3"], "m": 30},
    {"p": 6, "s": "Public communication & reporting",
     "a": ["Annual DD statement (Art.11)", "Website publication", "Impact + measures report",
           "Align with CSRD/ESRS"],
     "e": ["S1-17", "G1-1"], "m": 36},
]

# ─── Sector Adverse Impacts ─────────────────────────────────────────−−─
SECTOR_IMPACTS: dict[str, dict[str, list[str]]] = {
    "textiles": {"hr": ["Child labour (cotton)", "Forced labour (garments)", "Unsafe conditions",
                        "Wage violations", "FoA suppression"],
                 "env": ["Water pollution (dyeing)", "High water use", "Microplastics",
                         "Chemical waste", "High carbon footprint"]},
    "agriculture": {"hr": ["Child labour (cocoa/coffee)", "Debt bondage", "Land rights violations",
                           "Pesticide exposure", "Migrant discrimination"],
                    "env": ["Deforestation", "Excessive irrigation", "Agrochemical runoff",
                            "Soil degradation", "Biodiversity loss", "Livestock GHG"]},
    "minerals": {"hr": ["Child labour (artisanal DRC)", "Forced labour", "Conflict minerals",
                        "Toxic exposure (Hg/As)", "Indigenous displacement"],
                 "env": ["Deforestation", "Acid mine drainage", "Heavy metals",
                         "Biodiversity destruction", "Tailings dam risk", "High energy"]},
    "construction": {"hr": ["Forced labour (migrant)", "Unsafe conditions (falls)",
                            "Wage theft", "FoA restrictions", "Discrimination"],
                     "env": ["GHG (cement/steel)", "PM air pollution", "Land conversion",
                             "High water use", "C&D waste", "Sand depletion"]},
    "default": {"hr": ["Tier 2-3 child/forced labour", "OHS risks", "Wage compliance",
                       "FoA in supply chain", "Discrimination"],
                "env": ["Scope 1-3 emissions", "Waste gaps", "Resource efficiency",
                        "Supply chain pollution", "Biodiversity impact"]},
}

# ─── Penalty Framework ─────────────────────────────────────────────────
PENALTIES = {
    "max_fine_pct": 5,
    "max_fine_label": "Up to 5% of net worldwide annual turnover",
    "civil_liability": "Art.22 — liability for damages from non-compliance",
    "enforcement": "Member State supervisory authorities: investigate, fine, injunctions",
    "aggravating": ["Repeated non-compliance", "Failure to remediate impacts",
                    "Obstruction of investigation", "No stakeholder cooperation"],
    "mitigating": ["Voluntary remediation", "Cooperation with authority",
                   "Preventive measures before infringement", "Stakeholder engagement"],
}

# ─── Stakeholder Types (Art.13) ────────────────────────────────────────
STAKEHOLDERS = {
    "employees": {"methods": ["Works councils", "Union consultations", "Surveys", "Whistleblower"],
                  "esrs": "S1-2"},
    "supply_chain": {"methods": ["Supplier audits", "Worker hotlines", "Multi-stakeholder initiatives",
                                 "Grievance mechanisms"], "esrs": "S2-2"},
    "communities": {"methods": ["FPIC", "Community meetings", "Liaison offices", "NGO partnerships"],
                    "esrs": "S3-2"},
    "consumers": {"methods": ["Consumer panels", "Complaint mechanisms", "Safety reporting"],
                  "esrs": "S4-2"},
    "civil_society": {"methods": ["NGO consultations", "HRIA participation", "Public comment"],
                      "esrs": "S1-2, S3-2"},
}


# ─── Helpers ───────────────────────────────────────────────────────────

def _applicable_wave(emp: int, rev_m: float) -> dict | None:
    for t in reversed(CSDDD_THRESHOLDS):
        if emp >= t["min_emp"] and rev_m >= t["min_rev_m"]:
            return t
    return None

def _is_high_risk(sector: str) -> bool:
    return sector.lower() in HIGH_RISK_SECTORS

def _region_hr(regions: list[str]) -> list[dict]:
    return [{"region": r, "label": REGION_LABELS.get(r, r),
             **REGION_HR.get(r, {"o": 3, "c": 3, "f": 3, "fr": 3, "d": 3})}
            for r in regions]

def _region_env(regions: list[str]) -> list[dict]:
    return [{"region": r, "label": REGION_LABELS.get(r, r),
             **REGION_ENV.get(r, {"o": 3, "cl": 3, "b": 3, "p": 3, "ci": 3})}
            for r in regions]

def _avg_risk(scores: list[dict], key: str = "o") -> int:
    if not scores:
        return 1
    return max(1, min(5, round(sum(s.get(key, 3) for s in scores) / len(scores))))

def _impacts(sector: str) -> dict[str, list[str]]:
    return SECTOR_IMPACTS.get(sector.lower(), SECTOR_IMPACTS["default"])

def _esrs_refs(refs: list[str]) -> list[str]:
    return [get_esrs_ref(r) for r in refs]


# ─── Public API ────────────────────────────────────────────────────────

async def get_csddd_due_diligence(sector: str, regions: list[str],
                                  revenue_eur_m: float = 0) -> dict[str, Any]:
    """Full CSDDD due diligence assessment: trigger checks, risk scoring,
    adverse impacts, due diligence phases, and penalties framework."""
    cache = get_cache()
    ck = cache.make_key("csddd_dd", sector, *sorted(regions or ["?"]), str(revenue_eur_m))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    is_hr = _is_high_risk(s)
    cat = SECTOR_CATS.get(s, "default")
    nace = HIGH_RISK_SECTORS.get(s, "N/A")
    meets = revenue_eur_m >= 150

    hr_reg = _region_hr(regions)
    hr_o = _avg_risk(hr_reg)
    if is_hr:
        hr_o = min(5, hr_o + 1)
    env_reg = _region_env(regions)
    env_o = _avg_risk(env_reg)
    if is_hr:
        env_o = min(5, env_o + 1)
    overall = max(hr_o, env_o)

    imps = _impacts(s)
    steps = DD_PHASES if (meets or is_hr) else DD_PHASES[:3]
    max_fine = round(revenue_eur_m * PENALTIES["max_fine_pct"] / 100, 2)

    result = {
        "assessment": {"sector": s, "category": cat, "nace": nace, "high_risk": is_hr,
                       "regions": regions, "revenue_eur_m": revenue_eur_m,
                       "meets_threshold": meets,
                       "overall_risk": overall, "label": risk_label(overall), "color": risk_color(overall)},
        "triggers": {"sector": is_hr, "revenue": meets,
                     "note": "Sector: high-risk (textiles/ag/minerals/construction). Revenue: >=€150M"},
        "human_rights": {"score": hr_o, "label": risk_label(hr_o), "color": risk_color(hr_o),
                         "regions": hr_reg, "esrs": _esrs_refs(["S1-1", "S2-1", "S3-1"])},
        "environmental": {"score": env_o, "label": risk_label(env_o), "color": risk_color(env_o),
                          "regions": env_reg, "esrs": _esrs_refs(["E1-1", "E2-1", "E3-1", "E4-1", "E5-1"])},
        "adverse_impacts": {"human_rights": imps["hr"], "environmental": imps["env"]},
        "due_diligence_phases": [
            {"phase": p["p"], "step": p["s"], "actions": p["a"],
             "esrs": _esrs_refs(p["e"]), "timeline_months": p["m"]}
            for p in steps],
        "penalties": {"max_fine_pct": PENALTIES["max_fine_pct"], "max_fine_eur_m": max_fine,
                      "civil_liability": PENALTIES["civil_liability"],
                      "enforcement": PENALTIES["enforcement"],
                      "aggravating": PENALTIES["aggravating"],
                      "mitigating": PENALTIES["mitigating"]},
        "recommendations": [
            "Full DD (6 phases) mandatory" if is_hr or meets else "Below thresholds — voluntary DD",
            *(["CRITICAL: Immediate action needed"] if overall >= 4 else []),
            *(["HR risk elevated — conduct HRIA"] if hr_o >= 4 else []),
            *(["Env risk elevated — strengthen env DD"] if env_o >= 4 else []),
            "Establish cross-functional CSDDD compliance team + board oversight",
            "Integrate DD into CSRD/ESRS reporting",
        ],
        "disclaimer": f"Generated {today_iso()}. Indicative — consult legal counsel.",
    }
    cache.set(ck, result, "csddd", 14)
    return result


async def get_csddd_human_rights_risk(sector: str, regions: list[str]) -> dict[str, Any]:
    """Human rights risk assessment: child labour, forced labour, freedom of
    association, discrimination. Cross-references ESRS S1,S2,S3."""
    cache = get_cache()
    ck = cache.make_key("csddd_hr", sector, *sorted(regions or ["?"]))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    is_hr = _is_high_risk(s)
    reg_scores = _region_hr(regions)

    dims = {"child_labour": "c", "forced_labour": "f",
            "freedom_association": "fr", "discrimination": "d"}
    dim_results = {}
    for label, key in dims.items():
        vals = [r.get(key, 3) for r in reg_scores]
        adj = 1 if (is_hr and key in ("c", "f")) else 0
        sc = max(1, min(5, round(sum(vals)/max(len(vals),1)) + adj))
        dim_results[label] = {"score": sc, "label": risk_label(sc), "color": risk_color(sc)}

    overall = min(5, _avg_risk(reg_scores) + (1 if is_hr else 0))

    esrs_map = {
        "child_labour": _esrs_refs(["S1", "S1-17"]),
        "forced_labour": _esrs_refs(["S1", "S2-1"]),
        "freedom_association": _esrs_refs(["S1-8"]),
        "discrimination": _esrs_refs(["S1-9"]),
    }

    result = {
        "overall": overall, "label": risk_label(overall), "color": risk_color(overall),
        "sector": s, "high_risk": is_hr, "dimensions": dim_results,
        "regions": reg_scores, "esrs": esrs_map,
        "sources": "ILO 2024, HRW 2025, Freedom House 2024, ITUC GR 2025",
        "recommendations": ["Conduct HRIA in high-risk regions", "Supply chain HR DD per Art.5",
                            "Grievance mechanisms", "Trade union engagement", "HR KPIs in contracts"],
    }
    cache.set(ck, result, "csddd", 14)
    return result


async def get_csddd_environmental_risk(sector: str, regions: list[str]) -> dict[str, Any]:
    """Environmental due diligence risk assessment: climate, biodiversity,
    pollution, circular economy. Cross-references ESRS E1-E5."""
    cache = get_cache()
    ck = cache.make_key("csddd_env", sector, *sorted(regions or ["?"]))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    is_hr = _is_high_risk(s)
    reg_scores = _region_env(regions)

    dims = {"climate": "cl", "biodiversity": "b", "pollution": "p", "circular_economy": "ci"}
    dim_results = {}
    esrs_dim = {"climate": "E1", "biodiversity": "E4", "pollution": "E2", "circular_economy": "E5"}
    for label, key in dims.items():
        vals = [r.get(key, 3) for r in reg_scores]
        adj = 1 if (is_hr and key in ("cl", "p")) else 0
        sc = max(1, min(5, round(sum(vals)/max(len(vals),1)) + adj))
        dim_results[label] = {"score": sc, "label": risk_label(sc), "color": risk_color(sc),
                              "esrs": esrs_dim.get(label, "")}

    overall = min(5, _avg_risk(reg_scores) + (1 if is_hr else 0))

    result = {
        "overall": overall, "label": risk_label(overall), "color": risk_color(overall),
        "sector": s, "high_risk": is_hr, "dimensions": dim_results,
        "regions": reg_scores, "esrs": _esrs_refs(["E1-1", "E1-5", "E2-3", "E3-3", "E4-3", "E5-3"]),
        "sources": "EPI 2024 (Yale/Columbia), Germanwatch CRI 2025, WEF GRR 2025, UNEP 2023",
        "recommendations": ["Env DD: climate, biodiversity, pollution, circularity",
                            "Paris-aligned climate transition plan (Art.7)",
                            "Map biodiversity impacts", "Pollution prevention across SC",
                            "Circular economy KPIs"],
    }
    cache.set(ck, result, "csddd", 14)
    return result


async def get_csddd_action_plan(sector: str, risk_level: int = 3,
                                revenue_eur_m: float = 0) -> dict[str, Any]:
    """Mandatory CSDDD action plan: prevention, mitigation, remediation,
    KPIs, stakeholder engagement, climate transition alignment."""
    cache = get_cache()
    ck = cache.make_key("csddd_plan", sector, str(risk_level), str(revenue_eur_m))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    is_hr = _is_high_risk(s)
    rl = max(1, min(5, risk_level))

    urgency = {5: "immediate", 4: "immediate", 3: "high", 2: "moderate", 1: "standard"}[rl]
    timeline = {5: "0-6mo monthly", 4: "0-12mo monthly", 3: "6-18mo quarterly",
                2: "12-24mo semi-annual", 1: "18-36mo annual"}[rl]
    max_fine = round(revenue_eur_m * 0.05, 2)

    imps = _impacts(s)

    phases = {
        "prevention": {
            "actions": ["Adopt HR+env DD policy", "Supply chain mapping tiers 1-3",
                        "Supplier code of conduct", "Contractual cascading",
                        "SME capacity building", "Climate transition plan (1.5°C)",
                        "Stakeholder engagement integration"],
            "kpis": ["% suppliers with code of conduct (100% @12mo)", "HR training sessions/qtr",
                     "% high-risk suppliers audited (100% @18mo)"],
            "mo": 12, "esrs": _esrs_refs(["S1-1", "S2-1", "S3-1", "G1-1", "E1-2"]),
        },
        "mitigation": {
            "actions": ["Risk-based action plans per impact", "Supplier corrective actions",
                        "Suspend non-compliant suppliers", "Alternative sourcing",
                        "Deploy grievance mechanisms", "Follow-up audits"],
            "kpis": ["Impact incidents tracked", "% with active mitigation plans",
                     "Avg corrective action time (days)", "% non-compliant partners terminated"],
            "mo": 24, "esrs": _esrs_refs(["S1-4", "S2-4", "S3-4", "E1-3", "E2-1"]),
        },
        "remediation": {
            "actions": ["Scope remediation per impact", "Provide/cooperate in remediation",
                        "Remediation tracking system", "Stakeholder engagement on remediation",
                        "Recurrence monitoring", "Post-remediation satisfaction assessment"],
            "kpis": ["Remediated cases", "Total compensation (€)",
                     "% no recurrence (>90%)", "Corrective action completion rate"],
            "mo": 36, "esrs": _esrs_refs(["S1-3", "S2-3", "S3-3", "S1-17"]),
        },
    }

    result = {
        "company": {"sector": s, "high_risk": is_hr, "risk_level": rl,
                     "label": risk_label(rl), "revenue_eur_m": revenue_eur_m,
                     "max_fine_eur_m": max_fine},
        "action_plan": {"urgency": urgency, "timeline": timeline, "phases": phases,
                        "impacts_addressed": imps},
        "stakeholder_engagement": {
            "requirement": "CSDDD Art.13 mandates engagement throughout DD",
            "stakeholders": [{"type": k, "methods": v["methods"], "esrs": v["esrs"]}
                             for k, v in STAKEHOLDERS.items()],
            "frequency": "Quarterly (high-risk) / Annually (standard)"},
        "climate_transition": {
            "requirement": "CSDDD Art.7 (1000+ employees)",
            "status": "required" if rl >= 3 else "recommended",
            "elements": ["Paris-aligned Scope 1-3 targets", "Decarbonisation levers",
                         "Progress metrics", "ESRS E1-1", "Board oversight"]},
        "sources": "CSDDD (EU 2024/1760) Art.5-13; EC FAQ Mar 2025; EFRAG IG 2024",
    }
    cache.set(ck, result, "csddd", 14)
    return result


async def get_csddd_reporting_checklist(sector: str, employees: int = 0,
                                        revenue_eur_m: float = 0) -> dict[str, Any]:
    """CSDDD vs CSRD overlay checklist: wave applicability, itemised checklist,
    implementation timeline, penalties. Covers 2027/2028/2029 waves."""
    cache = get_cache()
    ck = cache.make_key("csddd_check", sector, str(employees), str(revenue_eur_m))
    cached = cache.get(ck)
    if cached:
        return cached
    await asyncio.sleep(0)

    s = sector.lower().strip()
    is_hr = _is_high_risk(s)
    nace = HIGH_RISK_SECTORS.get(s, "N/A")
    wave = _applicable_wave(employees, revenue_eur_m)
    in_scope = wave is not None or is_hr
    wave_num = wave["wave"] if wave else (1 if is_hr else 0)

    timeline = [
        {"year": t["year"], "wave": t["wave"], "label": t["label"],
         "threshold": f"{t['min_emp']}+ emp, €{t['min_rev_m']}M+",
         "applies": employees >= t["min_emp"] and revenue_eur_m >= t["min_rev_m"]}
        for t in CSDDD_THRESHOLDS]

    checklist = [
        {"id": f"DD-{i:02d}", "req": r, "esrs": e, "wv": w, "prio": p}
        for i, (r, e, w, p) in enumerate([
            ("Adopt HR+env DD policy (Art.5)", "S1-1, G1-1", 1, "critical"),
            ("Map value chain adverse impacts (Art.6)", "S1-6, S2-1", 1, "critical"),
            ("ID + prioritise human rights impacts (Art.6)", "S1-17", 1, "critical"),
            ("ID + prioritise environmental impacts (Art.6)", "E1-7, E2-5, E4-4", 1, "critical"),
            ("Prevention + mitigation plan (Art.7)", "S1-4, S2-4, S3-4", 1, "high"),
            ("Climate transition plan Paris-aligned (Art.7)", "E1-1", 1, "high"),
            ("Grievance mechanism (Art.10)", "S1-3, S2-3, S3-3", 1, "high"),
            ("Remediate adverse impacts (Art.8)", "S1-3", 1, "high"),
            ("Stakeholder engagement (Art.13)", "S1-2, S2-2, S3-2", 1, "high"),
            ("Monitor DD effectiveness (Art.10)", "S1-5, S2-5, S3-5", 1, "medium"),
            ("Annual public DD report (Art.11)", "G1-1", 2, "medium"),
            ("Contractual cascading (Art.7)", "G1-2", 2, "medium"),
            ("SME capacity building (Art.7)", "S2-4", 2, "medium"),
            ("Terminate non-compliant partners (Art.7)", "G1-1", 2, "medium"),
            ("Publicise impact + remediation (Art.11)", "S1-17", 3, "low"),
        ], 1)]
    for item in checklist:
        item["applies"] = item["wv"] <= wave_num if in_scope else False

    max_fine = round(revenue_eur_m * 0.05, 2)

    result = {
        "company": {"sector": s, "high_risk": is_hr, "nace": nace,
                     "employees": employees, "revenue_eur_m": revenue_eur_m, "in_scope": in_scope},
        "wave": {"applies": in_scope, "wave": wave_num,
            "year": wave["year"] if wave else (2027 if is_hr else None),
            "reason": (f"Wave {wave['wave']}: {wave['min_emp']}+ emp, €{wave['min_rev_m']}M+"
                       if wave else ("High-risk sector trigger" if is_hr else "Not applicable"))},
        "timeline": timeline, "checklist": checklist,
        "summary": {"total": len(checklist),
                     "applicable": sum(1 for c in checklist if c["applies"]),
                     "critical": sum(1 for c in checklist if c["applies"] and c["prio"] == "critical"),
                     "high": sum(1 for c in checklist if c["applies"] and c["prio"] == "high")},
        "csrd_overlay": {
            "note": "CSRD (ESRS) and CSDDD complementary — align disclosures with DD outcomes",
            "overlaps": ["E1-1 (transition) ↔ Art.7 (climate plan)",
                         "S1-1..S3-5 (HR) ↔ Art.5-11 (DD)",
                         "G1-2 (supplier mgmt) ↔ Art.7 (cascading)",
                         "S1-17 (incidents) ↔ Art.8 (remediation)"]},
        "penalties": {"regulatory": {"max_pct": 5, "max_eur_m": max_fine,
                           "enforcement": "Member State supervisory authorities"},
            "civil": PENALTIES["civil_liability"],
            "reputational": "Public naming, investor exclusion, partner termination"},
        "sources": "CSDDD (2024/1760); CSRD (2022/2464); EC FAQ Mar 2025; EFRAG IG 2024",
        "disclaimer": f"Generated {today_iso()}. Indicative — consult legal counsel.",
    }
    cache.set(ck, result, "csddd", 14)
    return result
