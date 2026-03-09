# interconnector-flow-model

Gridflow is a Python project for ingesting, cleaning, and analysing European electricity market data, with a focus on UK interconnectors and cross-border power flows.

The current codebase supports:

- UK / Elexon
  - generation by fuel type
  - actual total demand
  - market index prices
- Europe / ENTSO-E
  - actual total load
  - generation per type
  - energy prices

Data is stored locally in a simple medallion-style layout:

- data/bronze/ for raw and lightly parsed source data
- data/silver/ for cleaned, standardised parquet data
- later data/gold/ for modelling-ready conformed datasets

---

## Project goals

The longer-term goal of the project is to model and forecast interconnector flows between Great Britain and neighbouring European markets.

That means building up:

- historic generation, demand, and price data
- interconnector-linked market context
- cleaned and standardised time series
- forecasting and analysis tooling
- visual validation and research notebooks

---

## Current status

The project currently has:

- a working bronze ingestion pipeline for Elexon
- a working silver cleaning pipeline for Elexon
- a working bronze ingestion pipeline for ENTSO-E
- a working silver cleaning pipeline for ENTSO-E
- a CLI for running ingestion and cleaning jobs
- notebook-based inspection and validation support

---

## Long-Term Repository structure

.
├── config/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/
├── outputs/
├── src/
│   └── gridflow/
│       ├── cli/
│       ├── common/
│       ├── config/
│       ├── etl/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       ├── evaluation/
│       ├── features/
│       └── models/
└── tests/

---

## Requirements

- Python 3.11
- macOS / Linux shell recommended
- ENTSO-E API token for European data

---

## Setup

### 1. Clone the repository

git clone <your-repo-url>
cd interconnector-flow-model

### 2. Create a virtual environment

python3.11 -m venv .venv
source .venv/bin/activate

### 3. Install the project

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"

### 4. Add your ENTSO-E token

Create a .env file in the project root:

ENTSOE_SECURITY_TOKEN=your_token_here

The code uses this token for ENTSO-E API calls.

---

## ENTSO-E API access

To use the ENTSO-E API, you need a Transparency Platform account and REST API access.

The process is:

1. Register on the ENTSO-E Transparency Platform
2. Email transparency@entsoe.eu with subject:
   RESTful API access
3. Put your registered email address in the email body
4. Once access is granted, generate a token in your account page
5. Put the token in .env

---

## Quick start

### Elexon smoke test

python -m gridflow.cli.main test-fuelhh --date 2026-02-28

### Build UK bronze data

python -m gridflow.cli.main bronze-elexon-core --date-from 2026-01-01 --date-to 2026-02-01

### Build UK silver data

python -m gridflow.cli.main silver-elexon-core --date-from 2026-01-01 --date-to 2026-02-01

### Build ENTSO-E bronze for one zone

python -m gridflow.cli.main bronze-entsoe-core-zone --zone FR --date-from 2026-01-01 --date-to 2026-02-01

### Build ENTSO-E silver for one zone

python -m gridflow.cli.main silver-entsoe-core-zone --zone FR --date-from 2026-01-01 --date-to 2026-02-01

### Build ENTSO-E bronze for all configured zones

python -m gridflow.cli.main bronze-entsoe-core-all --date-from 2026-01-01 --date-to 2026-02-01

### Build ENTSO-E silver for all configured zones

python -m gridflow.cli.main silver-entsoe-core-all --date-from 2026-01-01 --date-to 2026-02-01

---

## Supported CLI commands

### Elexon

Build bronze UK core datasets:

python -m gridflow.cli.main bronze-elexon-core --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build one bronze UK dataset:

python -m gridflow.cli.main bronze-elexon-history --dataset fuelhh --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main bronze-elexon-history --dataset demand_actual_total --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main bronze-elexon-history --dataset mid --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build silver UK core datasets:

python -m gridflow.cli.main silver-elexon-core --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build one silver UK dataset:

python -m gridflow.cli.main silver-elexon-history --dataset fuelhh --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main silver-elexon-history --dataset demand_actual_total --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main silver-elexon-history --dataset mid --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Run UK bronze then silver in sequence:

python -m gridflow.cli.main elexon --date-from YYYY-MM-DD --date-to YYYY-MM-DD

### ENTSO-E

Build bronze ENTSO-E core datasets for one zone:

python -m gridflow.cli.main bronze-entsoe-core-zone --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build bronze ENTSO-E core datasets for all configured zones:

python -m gridflow.cli.main bronze-entsoe-core-all --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build one bronze ENTSO-E dataset for one zone:

python -m gridflow.cli.main bronze-entsoe-history --dataset actual_total_load --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main bronze-entsoe-history --dataset generation_per_type --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main bronze-entsoe-history --dataset energy_prices --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build silver ENTSO-E core datasets for one zone:

python -m gridflow.cli.main silver-entsoe-core-zone --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build silver ENTSO-E core datasets for all configured zones:

python -m gridflow.cli.main silver-entsoe-core-all --date-from YYYY-MM-DD --date-to YYYY-MM-DD

Build one silver ENTSO-E dataset for one zone:

python -m gridflow.cli.main silver-entsoe-history --dataset actual_total_load --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main silver-entsoe-history --dataset generation_per_type --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD
python -m gridflow.cli.main silver-entsoe-history --dataset energy_prices --zone FR --date-from YYYY-MM-DD --date-to YYYY-MM-DD

---

## Current configured ENTSO-E zones

The project is currently set up around a zone-based ENTSO-E approach.

Examples include:

- FR
- BE
- NL
- IE_SEM

Additional zones can be added in:

src/gridflow/config/sources.py

---

## Data layout

### Bronze

Bronze stores:

- raw source artifacts
- parsed flat parquet
- ingest manifests

Examples:

data/bronze/elexon/fuelhh/raw/...
data/bronze/elexon/fuelhh/flat/...
data/bronze/entsoe/actual_total_load__fr/raw/...
data/bronze/entsoe/actual_total_load__fr/flat/...

### Silver

Silver stores cleaned, standardised parquet files.

Examples:

data/silver/elexon/fuelhh/...
data/silver/elexon/demand_actual_total/...
data/silver/elexon/mid/...

data/silver/entsoe/fr/actual_total_load/...
data/silver/entsoe/fr/generation_per_type/...
data/silver/entsoe/fr/energy_prices/...

---

## Notebooks

The project includes notebooks for:

- smoke testing
- data inspection
- bronze/silver validation
- modelling experiments

If you want to use the project venv in Jupyter:

pip install ipykernel
python -m ipykernel install --user --name interconnector-flow-model --display-name "Python (interconnector-flow-model)"

Then select that kernel in VS Code.

---

## Cleaning generated data

Useful Makefile commands:

make clean-bronze
make clean-silver
make clean

These remove generated parquet / raw data but do not affect source code.

---

## Development notes

### Rate limiting

The project includes conservative HTTP throttling and retry logic for both:

- Elexon
- ENTSO-E

This is important for large historical backfills.

### Philosophy

The codebase follows a medallion-style structure:

- bronze = source-native raw / flat data
- silver = source-cleaned and standardised data
- gold = future conformed, modelling-ready datasets

### Scope discipline

The project intentionally avoids building the entire world energy system.
The current focus is:

- Great Britain
- directly connected European markets
- selected second-order neighbouring zones where useful

---

## Onboarding a new developer

After cloning the repo, a new developer should:

1. create and activate the venv
2. install the project with pip install -e ".[dev]"
3. create a .env file with ENTSOE_SECURITY_TOKEN
4. run a small Elexon smoke test
5. run a small ENTSO-E bronze test
6. run the corresponding silver build

Suggested first commands:

python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
echo 'ENTSOE_SECURITY_TOKEN=your_token_here' > .env

python -m gridflow.cli.main test-fuelhh --date 2026-02-28
python -m gridflow.cli.main bronze-entsoe-history --dataset actual_total_load --zone FR --date-from 2026-02-01 --date-to 2026-02-02
python -m gridflow.cli.main silver-entsoe-history --dataset actual_total_load --zone FR --date-from 2026-02-01 --date-to 2026-02-02

---