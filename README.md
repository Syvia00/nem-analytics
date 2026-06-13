# NEM Analytics Pipeline

A Python data pipeline for ingesting, transforming, and visualising Australian National Electricity Market (NEM) data.

## Project Structure

```
nem-analytics-pipeline/
├── dags/           # Airflow DAGs for orchestrating pipeline runs
├── src/            # Core Python source code (ingestion, transforms, models)
├── sql/            # SQL queries and schema definitions
├── notebooks/      # Jupyter notebooks for exploration and analysis
├── dashboard/      # Dashboard code and configuration (e.g. Streamlit / Grafana)
└── README.md
```

## Getting Started

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies (once requirements.txt is added)
pip install -r requirements.txt
```

## Data Source

Data is sourced from the [AEMO MMS Data Model](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-management-system-mms-data-model).

## License

MIT
