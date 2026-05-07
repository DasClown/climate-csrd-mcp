# Climate CSRD MCP Server 🌍📊

**MCP-Server für Klimarisiko-Analyse und CSRD/ESRS-Berichterstattung.**

Kombiniert Copernicus-Satellitendaten, DWD-Wetterdaten, EU-ETS-Emissionsbenchmarks, UBA-Umweltdaten und EU-ESRS-Regularien zu einer standortbezogenen Risikobewertung — bereitgestellt als **Model Context Protocol (MCP)** Server.

## Features 🚀

| Tool | Beschreibung |
|---|---|
| `assess_climate_risk` | Physisches Klimarisiko (Hochwasser, Hitze, Dürre) → Gesamt-Score 1-5 |
| `get_emission_benchmarks` | Branchen-Emissionsvergleiche aus EU ETS + EEA |
| `get_csrd_requirements` | ESRS-Offenlegungspflichten nach CSRD-Kriterien |
| `csrd_report` | Kompletter CSRD-Berichtsbaustein für einen Standort |
| `get_kfw_funding` | Fördermittel-Finder (KfW, BAFA) |

## Datenquellen 📡

| Quelle | Daten | Zugang |
|---|---|---|
| [Copernicus CDS](https://cds.climate.copernicus.eu/) | Hochwasser, Dürre, Landnutzung | API-Key (optional) |
| [DWD OpenData](https://opendata.dwd.de/) | Hitzetage, Klimareferenz | Kostenlos |
| [EU ETS](https://ec.europa.eu/clima/eu-ets/) | Emissionsbenchmarks | Öffentlich |
| [UBA](https://www.umweltbundesamt.de/) | Luftqualität, Grundwasser | Kostenlos |
| [EUR-Lex](https://eur-lex.europa.eu/) | ESRS-Standards | Öffentlich |
| [KfW](https://www.kfw.de/) | Förderprogramme | Öffentlich |

## Installation 🛠️

```bash
# 1. Repository klonen
git clone https://github.com/YOUR_USER/climate-csrd-mcp.git
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
        ├── server.py           # MCP-Server (FastMCP) + 5 Tools
        ├── cache.py            # SQLite-Cache-Layer
        ├── utils.py            # Risiko-Scoring, ESRS-Mapping
        └── data_sources/
            ├── __init__.py
            ├── copernicus.py   # Hochwasser, Dürre
            ├── dwd.py          # Hitzetage, Klimareferenz
            ├── eu_ets.py       # Emissionsbenchmarks
            ├── uba.py          # Luftqualität, Grundwasser
            ├── eurlex.py       # ESRS-Regularien
            └── kfw.py          # Förderprogramme
```

## Cache 🗄️

- **SQLite-basiert**: Automatisch, erster Start erzeugt `climate_cache.db`
- **TTL**: Klimadaten 30 Tage, Emissionen 7 Tage, CSRD 30 Tage
- **Standort-Basiert**: Gleicher Standort → gleicher Cache-Key → kein API-Call

## Disclaimer ⚠️

> *Stand: HEUTE. Dies ist keine geprüfte Berichterstattung. Die finale Verantwortung für Richtigkeit und Vollständigkeit liegt beim Unternehmen. Förderprogramme können sich ändern.*

## Lizenz 📄

MIT License — siehe [LICENSE](LICENSE).

---

**Entwickelt für den Einsatz mit Hermes Agent, Claude Code und anderen MCP-kompatiblen Clients.**
