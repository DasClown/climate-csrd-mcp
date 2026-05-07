# Climate CSRD MCP Server 🌍📊

**MCP-Server für Klimarisiko-Analyse und CSRD/ESRS-Berichterstattung.**

Kombiniert Copernicus-Satellitendaten, DWD-Wetterdaten, EU-ETS-Emissionsbenchmarks, UBA-Umweltdaten und EU-ESRS-Regularien zu einer standortbezogenen Risikobewertung — bereitgestellt als **Model Context Protocol (MCP)** Server.

## Tools 🛠️

### 1. `assess_climate_risk`
Bewertet das physische Klimarisiko eines Standorts. Kombiniert Hochwasser, Hitze, Dürre, Sturm, Meeresspiegel und Waldbrand zu einem gewichteten Gesamt-Risikoscore (1-5).

### 2. `compare_sites`
Vergleicht mehrere Standorte hinsichtlich ihres physischen Klimarisikos. Gibt eine Side-by-Side-Analyse mit Ranking aus.

### 3. `get_emission_benchmarks`
Holt Branchen-Emissionsbenchmarks aus dem EU ETS und der EEA-Datenbank. Liefert Durchschnitt, Top/Bottom 10% und Trend.

### 4. `get_csrd_requirements`
Ermittelt ESRS-Offenlegungspflichten nach CSRD. Basiert auf Art. 3, Sektor-Materialität und Unternehmensgröße.

### 5. `csrd_report`
Erstellt einen CSRD-konformen Berichtsbaustein für einen Standort inkl. Risikoanalyse, Emissionsbenchmark und ESRS-Matrix.

### 6. `get_kfw_funding`
Findet KfW- und BAFA-Förderprogramme für Klimaschutz- und Anpassungsmaßnahmen.

### 7. `get_carbon_forecast`
EU ETS Kohlenstoffpreis-Prognose 2025-2040. Enthält Jahresprojektionen (min/max/central), Szenarien und Quellen.

### 8. `get_crrem_pathways`
CRREM Dekarbonisierungspfade für Immobilien. Unterstützt 5 Asset-Typen (office, retail, residential, logistics, hotel) in 15 EU-Ländern mit 3 Szenarien.

### 9. `get_supply_chain_risk`
Bewertet Klimarisiken in der Lieferkette basierend auf Sektoren und Regionen der Lieferanten.

### 10. `get_climate_synergy`
Liefert NDVI, Dürre- und Frostdaten für landwirtschaftliche Anwendungen (crop-mcp Integration). Growing Season Quality Index.

### 11. `get_double_materiality`
ESRS Double Materiality Assessment. Ermittelt Impact- und Financial-Materiality für einen Sektor unter Berücksichtigung von Standortrisiken.

### 12. `get_financial_climate_risk`
Schätzt die finanziellen Auswirkungen von physischen Klimarisiken basierend auf Risikoscore, Sektor und Umsatz.

### 13. `get_insurance_estimate`
Schätzt die Kosten für Betriebsunterbrechungs-Versicherung basierend auf Standortrisiko und Branche.

### 14. `get_funding_check`
Prüft die EU-Taxonomy-Konformität und Fördermittel-Eignung für Klimaschutz- und Anpassungsmaßnahmen.

### 15. `portfolio_risk`
Bewertet das Klimarisiko eines gesamten Standort-Portfolios. Aggregiert Einzelrisiken, berechnet finanzielle Exposure und priorisiert Maßnahmen. Erwartet eine Liste von Standorten mit Namen, Koordinaten, Sektor und Umsatzanteil.

### 16. `ngfs_scenarios`
Vergleicht Klimarisiken über verschiedene NGFS-Szenarien (Net Zero 2050, Below 2°C, NDCs, Current Policies). Mappt NGFS-Szenarien auf RCP-Pfade und vergleicht die Risikoprofile.

## Helper Functions

### `_prepare_report_text(report)`
Formatiert die Ausgabe des `csrd_report`-Tools als strukturierten Markdown-Text, geeignet für PDF-Export. Enthält Header, physikalische Risikozusammenfassung, ESRS-Matrix, Empfehlungen und Disclaimer.

## Datenquellen 📡

| Quelle | Daten | Zugang |
|--------|-------|--------|
| [Copernicus CDS](https://cds.climate.copernicus.eu/) | Hochwasser, Dürre, Landnutzung | API-Key (optional) |
| [DWD OpenData](https://opendata.dwd.de/) | Hitzetage, Klimareferenz | Kostenlos |
| [EU ETS](https://ec.europa.eu/clima/eu-ets/) | Emissionsbenchmarks | Öffentlich |
| [UBA](https://www.umweltbundesamt.de/) | Luftqualität, Grundwasser | Kostenlos |
| [EUR-Lex](https://eur-lex.europa.eu/) | ESRS-Standards | Öffentlich |
| [KfW](https://www.kfw.de/) | Förderprogramme | Öffentlich |

## Installation 🛠️

```bash
# 1. Via pip (empfohlen)
pip install git+https://github.com/DasClown/climate-csrd-mcp.git

# 2. Oder Repository klonen
git clone https://github.com/DasClown/climate-csrd-mcp.git
cd climate-csrd-mcp

# 2. Python-Umgebung
uv venv
source .venv/bin/activate  # oder: .venv\Scripts\activate (Windows)

# 3. Installieren
uv pip install -e .

# Optional: Copernicus CDS-Unterstützung
uv pip install -e ".[copernicus]"
```

## Konfiguration ⚙️

```bash
cp .env.example .env
# .env bearbeiten:
#   CDS_API_KEY = dein-copernicus-api-key
#   CLIMATE_CACHE_PATH = /pfad/zur/cache.db
```

**Copernicus CDS API-Key beantragen:**
1. Registrierung: https://cds.climate.copernicus.eu/profile
2. API-Key erstellen: https://cds.climate.copernicus.eu/api-how-to
3. In `.env` eintragen

Der Server funktioniert **auch ohne CDS-Key** — dann werden hinterlegte Referenzdaten (EEA, DWD) verwendet.

## Verwendung 📋

### Als MCP-Server (stdio)

```bash
climate-csrd-mcp
```

### Integration in Hermes Agent

In `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  climate-csrd:
    command: "climate-csrd-mcp"
    timeout: 120
```

Dann stehen die Tools als `mcp_climate-csrd_assess_climate_risk`, etc. zur Verfügung.

### Direkt-Test (über MCP Inspector)

```bash
npx @modelcontextprotocol/inspector climate-csrd-mcp
```

## Beispiele 📊

### Klimarisiko für München (2030)

```python
assess_climate_risk(lat=48.1351, lon=11.5820, location_name="München", year_horizon=2030)
```

**Ergebnis:**
- Hochwasser: 🟢 Risikoklasse 2 (Niedrig)
- Hitze: 🟠 14 Hitzetage/Jahr → Klasse 3
- Dürre: 🟢 Risikoklasse 2
- **Gesamt: 🟠 Risikoscore 3 (Mittel)**

### Portfolio-Risiko

```python
portfolio_risk(
    sites=[
        {"name": "Berlin Plant", "lat": 52.52, "lon": 13.405, "sector": "manufacturing", "revenue_share_pct": 40},
        {"name": "Hamburg Office", "lat": 53.55, "lon": 9.993, "sector": "real_estate", "revenue_share_pct": 25},
        {"name": "Munich Lab", "lat": 48.135, "lon": 11.582, "sector": "technology", "revenue_share_pct": 35},
    ],
    total_portfolio_revenue_eur_m=500.0,
    year_horizon=2030
)
```

### NGFS Szenarien-Vergleich

```python
ngfs_scenarios(lat=48.1351, lon=11.5820, location_name="München", year_horizon=2050)
```

### CSRD-Bericht für Produktionsstandort

```python
csrd_report(
    lat=51.05, lon=13.74,
    sector="manufacturing",
    company_name="Example GmbH",
    site_name="Dresden Plant",
    employees=1200,
    revenue=250.0,
    entity_type="large"
)
```

## ESRS-Konformität ✅

Der Server ist abgestimmt auf:

- **CSRD**: Directive (EU) 2022/2464
- **ESRS 1**: Allgemeine Anforderungen
- **ESRS 2**: Allgemeine Angaben (Strategie, Governance, Wesentlichkeitsanalyse)
- **ESRS E1-E5**: Umwelt (Klima, Verschmutzung, Wasser, Biodiversität, Kreislaufwirtschaft)
- **ESRS S1-S4**: Soziales
- **ESRS G1**: Unternehmensführung

## Projektstruktur 📁

```
climate-csrd-mcp/
├── pyproject.toml          # Package-Konfiguration
├── README.md
├── LICENSE
├── .env.example
├── .gitignore
└── src/
    └── climate_csrd_mcp/
        ├── __init__.py
        ├── server.py           # MCP-Server (FastMCP) + 16 Tools
        ├── cache.py            # SQLite-Cache-Layer
        ├── utils.py            # Risiko-Scoring, ESRS-Mapping
        └── data_sources/
            ├── __init__.py
            ├── copernicus.py   # Hochwasser, Dürre
            ├── dwd.py          # Hitzetage, Klimareferenz
            ├── eu_ets.py       # Emissionsbenchmarks
            ├── uba.py          # Luftqualität, Grundwasser
            ├── eurlex.py       # ESRS-Regularien
            ├── kfw.py          # Förderprogramme
            └── crrem.py        # CRREM-Pfade
```

## Cache 🗄️

- **SQLite-basiert**: Automatisch, erster Start erzeugt `climate_cache.db`
- **TTL**: Klimadaten 30 Tage, Emissionen 7 Tage, CSRD 30 Tage, NGFS 30 Tage
- **Standort-Basiert**: Gleicher Standort → gleicher Cache-Key → kein API-Call

## Disclaimer ⚠️

> *Stand: HEUTE. Dies ist keine geprüfte Berichterstattung. Die finale Verantwortung für Richtigkeit und Vollständigkeit liegt beim Unternehmen. Förderprogramme können sich ändern.*

## Lizenz 📄

MIT License — siehe [LICENSE](LICENSE).

---

**Entwickelt für den Einsatz mit Hermes Agent, Claude Code und anderen MCP-kompatiblen Clients.**
