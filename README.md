# Climate CSRD MCP Server 🌍📊

**MCP Server for Climate Risk Analysis & CSRD/ESRS Reporting — 27 Tools**

Combines Copernicus satellite data, DWD weather, EU ETS benchmarks, UBA environmental data, and EU ESRS regulations into a location-based risk assessment delivered as a **Model Context Protocol (MCP)** server.

---

## Tools 🛠️ (27 Total)

### Physical Climate Risk
| Tool | Description |
|------|-------------|
| `assess_climate_risk` | 6-dimension physical risk (flood, heat, drought, storm, SLR, wildfire) → weighted score 1-5 |
| `compare_sites` | Multi-site side-by-side comparison with heatmap & ranking |
| `portfolio_risk` | Portfolio-wide risk aggregation + financial exposure |
| `ngfs_scenarios` | NGFS scenario comparison (Net Zero 2050 → Current Policies) |
| `get_supply_chain_risk` | Supply chain climate risk by sector & region |

### CSRD, ESRS & Regulatory
| Tool | Description |
|------|-------------|
| `get_csrd_requirements` | ESRS disclosure obligations by entity size & sector |
| `csrd_report` | Full CSRD-compliant report (risk + benchmarks + ESRS matrix) |
| `get_double_materiality` | ESRS double materiality (impact + financial) |
| `get_csddd_assessment` | CSDDD supply chain due diligence (human rights + env risk, 3 waves 2027-2029) |
| `get_de_specific_assessment` | Germany: BISKO, KSG compliance (65%/88%/2045), LkSG |

### Emissions, Carbon & Energy
| Tool | Description |
|------|-------------|
| `get_emission_benchmarks` | EU ETS sector benchmarks (t CO₂/€ revenue, top/bottom 10%) |
| `get_carbon_forecast` | EU ETS carbon price forecast 2025-2040 |
| `get_crrem_pathways` | CRREM decarbonization pathways (15 countries, 5 asset types) |
| `check_sbti_target` | SBTi target validation (1.5°C/WB2C, 9 sectors) |
| `calculate_cbam_obligation` | CBAM carbon border adjustment (CN/US/UK carbon prices, free allocation 2026-2034) |

### ESG & Financial
| Tool | Description |
|------|-------------|
| `get_financial_climate_risk` | Financial loss estimate from physical risks (€M) |
| `get_insurance_estimate` | Business interruption insurance premium ranges |
| `get_esg_rating` | ESG rating simulation (MSCI AAA-CCC, Sustainalytics 0-100) |

### Biodiversity & Circular Economy
| Tool | Description |
|------|-------------|
| `get_tnfd_assessment` | TNFD biodiversity LEAP assessment (IUCN biomes, nature risk score) |
| `get_circular_economy_metrics` | ESRS E5 circular economy (circularity score, waste benchmarks) |

### Social
| Tool | Description |
|------|-------------|
| `get_social_sustainability` | ESRS S1-S4 social (HR due diligence, workforce benchmarks, UNGP) |

### Real Estate & Buildings
| Tool | Description |
|------|-------------|
| `get_real_estate_assessment` | Energy certificates (EPC A+-H), renovation roadmaps, KfW Effizienzhaus |

### TCFD
| Tool | Description |
|------|-------------|
| `get_tcfd_report` | Full TCFD report (governance, strategy, risk mgmt, metrics + ESRS mapping) |

### Agriculture (crop-mcp synergy)
| Tool | Description |
|------|-------------|
| `get_climate_synergy` | NDVI, drought & frost data — Growing Season Quality Index |

### Funding
| Tool | Description |
|------|-------------|
| `get_kfw_funding` | KfW/BAFA funding programs for climate measures |
| `get_funding_check` | EU Taxonomy alignment + funding eligibility check |

### Automated Reporting
| Tool | Description |
|------|-------------|
| `generate_full_report_package` | Combined CSRD report: E1-E5, S1-S4, G1, TCFD, TNFD, SBTi, CSDDD, ESG — single call |

---

## Data Sources 📡

| Source | Data | Access |
|--------|------|--------|
| [Copernicus CDS](https://cds.climate.copernicus.eu/) | Flood, drought, land use, NDVI, storm, SLR, wildfire | API key (optional) |
| [DWD OpenData](https://opendata.dwd.de/) | Hot days, frost, tropical nights, climate reference | Free |
| [EU ETS](https://ec.europa.eu/clima/eu-ets/) | Emission benchmarks, carbon price history | Public |
| [UBA](https://www.umweltbundesamt.de/) | Air quality (LUQ, PM10), groundwater, soil | Free |
| [EUR-Lex](https://eur-lex.europa.eu/) | ESRS standards, CSRD, CSDDD, CBAM, EU Taxonomy | Public |
| [KfW](https://www.kfw.de/) | KfW/BAFA/BEG funding programs | Public |
| [CRREM](https://www.crrem.org/) | Real estate decarbonization pathways | Public |
| [ILO](https://www.ilo.org/) / [HRW](https://www.hrw.org/) | Human rights risk indices (CSDDD, ESRS S1-S4) | Public |
| [IPCC AR6](https://www.ipcc.ch/) | Climate scenarios, sector loss tables | Public |

---

## Installation 🛠️

```bash
# Via pip
pip install git+https://github.com/DasClown/climate-csrd-mcp.git

# Or clone
git clone https://github.com/DasClown/climate-csrd-mcp.git
cd climate-csrd-mcp

# Python environment
uv venv
source .venv/bin/activate

# Install
uv pip install -e .

# Optional: Copernicus CDS support
uv pip install -e ".[copernicus]"
```

## Configuration ⚙️

```bash
cp .env.example .env
# Edit .env:
#   CDS_API_KEY = your-copernicus-api-key
#   CLIMATE_CACHE_PATH = /path/to/cache.db
```

**The server works without a CDS key** — it uses embedded reference data (EEA, DWD, IPCC, ILO).

## Usage 📋

### As MCP Server (stdio)

```bash
climate-csrd-mcp
```

### Hermes Agent Integration

In `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  climate-csrd:
    command: "climate-csrd-mcp"
    timeout: 120
```

### Direct Test (MCP Inspector)

```bash
npx @modelcontextprotocol/inspector climate-csrd-mcp
```

---

## Examples 📊

### Climate risk for a site (2030)

```python
assess_climate_risk(lat=48.1351, lon=11.5820, location_name="Munich", year_horizon=2030)
```

### Full CSRD report + ESG + SBTi + TCFD in one call

```python
generate_full_report_package(
    site_lat=48.1351, site_lon=11.5820,
    company_name="Example GmbH",
    sector="manufacturing",
    employees=1200, revenue=250.0
)
```

### ESG Rating

```python
get_esg_rating(
    sector="manufacturing",
    emissions_intensity=0.5,
    risk_score=3,
    methodology="msci"
)
```

### CBAM Carbon Border Adjustment

```python
calculate_cbam_obligation(
    import_goods_tons=5000,
    embedded_emissions=1.5,
    origin_country="CN",
    sector="iron_steel"
)
```

---

## ESRS Compliance ✅

- **CSRD**: Directive (EU) 2022/2464
- **ESRS 1-2**: General requirements & disclosures
- **ESRS E1**: Climate change (full)
- **ESRS E2**: Pollution
- **ESRS E3**: Water & marine resources
- **ESRS E4**: Biodiversity & ecosystems (TNFD-aligned)
- **ESRS E5**: Resource use & circular economy
- **ESRS S1**: Own workforce (17 disclosure requirements)
- **ESRS S2**: Workers in value chain
- **ESRS S3**: Affected communities
- **ESRS S4**: Consumers & end-users
- **ESRS G1**: Business conduct

## TCFD Alignment ✅

All 11 TCFD recommended disclosures mapped to ESRS E1 data points.

## TNFD Alignment ✅

Full LEAP (Locate-Evaluate-Assess-Prepare) methodology with IUCN biome classification.

---

## Project Structure 📁

```
climate-csrd-mcp/
├── pyproject.toml          # Package config (v2.0.0)
├── README.md
├── LICENSE
├── .env.example
├── .gitignore
└── src/
    └── climate_csrd_mcp/
        ├── __init__.py
        ├── server.py           # FastMCP server — 27 tools
        ├── cache.py            # SQLite cache layer
        ├── utils.py            # Risk scoring, ESRS mapping, financial estimates
        └── data_sources/
            ├── __init__.py
            ├── copernicus.py   # Flood, drought, NDVI, storm, SLR, wildfire, frost
            ├── dwd.py          # Hot days, frost, tropical nights, climate reference
            ├── eu_ets.py       # Emission benchmarks, carbon price history/forecast
            ├── uba.py          # Air quality, groundwater, soil moisture
            ├── eurlex.py       # ESRS/CSRD regulations, double materiality
            ├── kfw.py          # KfW/BAFA/BEG funding programs
            ├── crrem.py        # CRREM decarbonization pathways
            ├── tcfd.py         # TCFD report generator (governance, strategy, risk, metrics)
            ├── tnfd.py         # TNFD biodiversity LEAP assessment
            ├── sbti.py         # SBTi target validation
            ├── cbam.py         # CBAM border adjustment calculator
            ├── esrs_e5.py      # ESRS E5 circular economy
            ├── esrs_social.py  # ESRS S1-S4 social sustainability
            ├── csddd.py        # CSDDD supply chain due diligence
            ├── de_specific.py  # Germany: BISKO, KSG, LkSG
            ├── esg_rating.py   # MSCI/Sustainalytics ESG rating simulation
            ├── real_estate.py  # Energy certificates, KfW, renovation roadmaps
            └── auto_report.py # Combined CSRD report generation
```

---

## Cache 🗄️

- **SQLite-based**: Auto-created on first start (`climate_cache.db`)
- **TTL**: Climate 30d, emissions 7d, CSRD 30d, weather 1d
- **Location-keyed**: Same coordinates → same cache key → no redundant computation

---

## Disclaimer ⚠️

> *As of: [DATE]. This is not audited reporting. Final responsibility for accuracy and completeness lies with the company. Funding programs are subject to change.*

---

## License 📄

MIT License — see [LICENSE](LICENSE).

---

**Built for use with Hermes Agent, Claude Code, and MCP-compatible clients.**
