# Satellite Mission Operation Monitoring System

A cloud-based satellite telemetry monitoring pipeline built using **Azure Databricks**, **Delta Lake**, **PySpark**, **Skyfield**, and **Power BI**.

This project was developed as part of the **Digital Egypt Pioneers Initiative (DEPI) – Round 4 (2025–2026)**.

---

# Project Overview

The Satellite Mission Operation Monitoring System simulates telemetry from an Earth-orbiting satellite and processes it through a complete modern data engineering pipeline.

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw telemetry into clean analytical datasets that can be visualized in Power BI.

Telemetry is generated dynamically using the **Skyfield** astronomy library, processed through multiple Databricks notebooks, stored as Delta tables, and finally visualized through an interactive dashboard.

---

# Architecture

```
                    Skyfield Telemetry Simulation
                               │
                               ▼
                     Bronze Layer (Raw Delta)
                               │
                               ▼
                 Silver Layer (Validated & Clean)
                               │
                               ▼
               Gold Layer (Analytics & KPIs)
                               │
                               ▼
                     Power BI Dashboard
```

---

# Project Workflow

```
nb_04_pipeline_runner
        │
        ▼
nb_01_simulate_to_bronze
        │
        ▼
nb_02_bronze_to_silver
        │
        ▼
nb_03_silver_to_gold
        │
        ▼
Power BI Dashboard
```

---

# Medallion Architecture

## Bronze Layer

Raw telemetry generated using Skyfield.

Contains:

- timestamp
- elevation
- azimuth
- range
- signal quality
- packet loss
- data rate
- pass information

Table:

- bronze_telemetry

---

## Silver Layer

Performs data validation and cleaning.

Includes:

- schema validation
- datatype validation
- physics validation
- duplicate removal
- metadata generation

Table:

- silver_telemetry

---

## Gold Layer

Creates analytical datasets optimized for reporting.

Tables:

- gold_signal_kpi
- gold_pass_analytics
- gold_timeseries
- gold_alerts

---

# Alert Detection

The system automatically detects operational issues.

Implemented alerts include:

- High Packet Loss
- Weak Signal Quality
- In-Pass Communication Outage

Alert severity:

- NOMINAL
- WARNING
- CRITICAL

---

# Dashboard

The Power BI dashboard consists of three pages.

### Mission Overview

- Overall KPIs
- Signal trend
- Packet loss trend
- Data rate monitoring

### Pass Analytics

- Individual satellite passes
- Signal statistics
- Pass duration
- Maximum elevation
- Communication quality

### Alerts

- Critical alerts
- Warning alerts
- Alert history
- Severity distribution

---

# Technologies Used

- Python
- PySpark
- Azure Databricks
- Delta Lake
- Skyfield
- Pandas
- NumPy
- Power BI Desktop

---

# Project Structure

```
Satellite_Monitoring/

│
├── notebooks/
│   ├── nb_01_simulate_to_bronze.ipynb
│   ├── nb_02_bronze_to_silver.ipynb
│   ├── nb_03_silver_to_gold.ipynb
│   └── nb_04_pipeline_runner.ipynb
│
├── dashboard/
│   └── Satellite_Monitoring.pbix
│
├── documentation/
│
├── screenshots/
│
└── README.md
```

---

# Pipeline Automation

The project supports automated execution through Databricks Jobs.

Pipeline order:

1. Generate telemetry
2. Load into Bronze
3. Clean into Silver
4. Produce Gold analytics
5. Refresh dashboard dataset

---

# Data Processing Features

- Dynamic satellite orbit simulation
- Automated telemetry generation
- Physics-based validation
- Data cleaning
- Deduplication
- KPI calculation
- Alert generation
- Pass analytics
- Time-series aggregation

---

# Team Members

| Name | Role |
|------|------|
| Jana Habachy | Project Lead, System Architecture, Skyfield Simulation, Documentation |
| Hanin Galal | Data Engineering, Processing Pipeline, Power BI Dashboard |
| Malak Assal | Data Engineering, Automation & Scheduling |

Instructor:

**Mohamed Hamed**

DEPI Round 4 (2025–2026)

---

# Running the Project

Execute the notebooks in the following order:

1. `nb_01_simulate_to_bronze`
2. `nb_02_bronze_to_silver`
3. `nb_03_silver_to_gold`

or simply execute

```
nb_04_pipeline_runner
```

which orchestrates the complete pipeline automatically.

---

# Power BI

Due to limitations of the Azure Databricks Free Edition, Power BI could not maintain a live connection to the workspace.

Instead, the generated Gold Delta tables were exported as CSV files and imported into Power BI Desktop for dashboard creation.

---

# Future Improvements

- Kafka real-time streaming
- Multi-satellite constellation simulation
- Predictive anomaly detection
- REST API ingestion
- Live Power BI connection
- Microsoft Fabric integration
- Automated CI/CD deployment
- Docker containerization

---

# Repository

This repository contains:

- Databricks notebooks
- Power BI dashboard
- Project documentation
- Screenshots
- Pipeline implementation

---

## License

This project was developed for educational purposes as part of the **Digital Egypt Pioneers Initiative (DEPI)**.
