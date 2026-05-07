# Changelog

## [2.2.0] — 2026-05-07

### 🚀 Neu
- **Global Expansion**: 162 Flood-Risikozonen (alle Kontinente) — vorher ~60 (nur EU)
  - Asien: Indien, China, Japan, Korea, Indonesien, Philippinen, Vietnam, Thailand, Malaysia, Bangladesch, Pakistan, Sri Lanka
  - Afrika: Nigeria, Ägypten, Südafrika, Kenia, Tansania, Mosambik, Ghana, Kamerun, Senegal
  - Nordamerika: USA (Miami, Houston, New Orleans, NY, Boston, LA, SF, Seattle), Kanada, Mexiko
  - Südamerika: Brasilien, Argentinien, Chile, Peru, Kolumbien, Ecuador, Venezuela
  - Ozeanien: Australien (6 Städte), Neuseeland, Fidschi
- **GitHub Deployment**: Repo unter `github.com/DasClown/climate-csrd-mcp` (public)
- **Hermes Integration**: MCP Server in Hermes Config eingetragen
- **Daily Cron**: Tägliches Climate Briefing um 07:00 UTC
- **Token Persistenz**: GitHub Token in `/root/.hermes/.env` gespeichert

### 🛠️ Neue Tools (16 total)
| Tool | Beschreibung |
|------|-------------|
| `compare_sites` | Heatmap-Visualisierung mit `format='heatmap'` |
| `portfolio_risk` | Portfolio-Risiko mit finanzieller Exposure |
| `ngfs_scenarios` | 4 NGFS-Szenarien + `revenue_exposure_eur_m` |
| `get_carbon_forecast` | EUA €80→€260/t bis 2040 |
| `get_crrem_pathways` | 15 Länder, 5 Asset-Typen, 3 Szenarien |
| `get_supply_chain_risk` | 10 Regionen, Lieferketten-Risiko |
| `get_climate_synergy` | NDVI/Frost/Dürre (crop-mcp Integration) |
| `get_double_materiality` | ESRS Impact + Financial Materiality |
| `get_financial_climate_risk` | €-Auswirkungen nach Sektor/Risk |
| `get_insurance_estimate` | Versicherungsprämien (€/€M Revenue) |
| `get_funding_check` | EU-Taxonomy-Konformitäts-Check |

### 📊 Datenquellen-Verbesserungen
- **DWD**: 62 Wetterstationen (vorher 20)
- **DWD**: RCP 2.6/4.5/7.0/8.5 Szenarien-Unterstützung
- **UBA**: Luftqualität für 16 Bundesländer + 15 EU-Städte
- **EU ETS**: Carbon Price Forecast 2025-2040
- **CRREM**: Neu! Immobilien-Dekarbonisierungspfade
- **ESRS**: Vollständige S1-S4, G1, Double Materiality

### 🧮 Risk Scoring
- Weighted 6-Dimension-Modell (Flood 25%, Heat 20%, Drought 20%, Storm 12%, SLR 8%, Fire 15%)
- Financial Impact Estimates (IPCC AR6, EIOPA 2022, NGFS basiert)
- Supply Chain Risk (10 globale Regionen)

---

## [1.0.0] — 2026-05-07

### 🚀 Initial Release
- 5 MCP Tools: `assess_climate_risk`, `get_emission_benchmarks`, `get_csrd_requirements`, `csrd_report`, `get_kfw_funding`
- Datenquellen: Copernicus (DE/EU), DWD (20 Städte), EU ETS, UBA, EUR-Lex, KfW
- SQLite Cache mit TTL
- FastMCP-basierter MCP Server
- 23/23 Tests bestanden
