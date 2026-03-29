# AI Data Analyst — Snowflake + Cortex + Streamlit

An end-to-end data platform that combines **data engineering, analytics, and AI** to enable natural language querying of business data.

Users can ask questions in plain English, and the system automatically generates SQL, queries Snowflake, and returns results with clear insights.

---

## Overview

This project implements a modern data stack using:

* Snowflake (Data Warehouse)
* Medallion Architecture (Bronze → Silver → Gold)
* Power BI (Dashboards)
* Snowflake Cortex (AI layer)
* Streamlit (User interface)

The result is an interactive AI-powered analytics system.

---

## Architecture

Raw Data → Snowflake Stage → Bronze → Silver → Gold → AI Agent → Streamlit App → User

---

## Features

* Natural language → SQL generation using Snowflake Cortex
* Real-time querying of Snowflake warehouse
* Automated data pipeline using Streams and Tasks
* Star schema for analytics (Facts + Dimensions)
* Interactive dashboards in Power BI
* AI-generated insights (one-line summaries)
* Auto visualizations (charts from query results)
* Chat-style interface with history

---

## Data Pipeline

### Bronze Layer

* Raw ingestion from CSV files
* No transformations

### Silver Layer

* Data cleaning and standardization
* Joins and filtering
* Built using stored procedures

### Gold Layer

* Star schema design
* Fact and dimension tables
* Analytics-ready views

---

## Tech Stack

* Snowflake (SQL, Streams, Tasks, Cortex)
* Python
* Streamlit
* Pandas
* Power BI

---

## Example Queries

* "Top 5 products by revenue"
* "Monthly sales trend"
* "Top customers by total spending"
* "Give me a random 2-star review"

---

## Live App

https://ai-data-project.streamlit.app/
---

## How It Works

1. User enters a question
2. Cortex generates SQL
3. Query runs on Snowflake
4. Results are displayed
5. AI generates a one-line insight

---

## Setup (Local)

```bash
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```

---

## Secrets Configuration

Set the following in Streamlit secrets:

```
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
```

---

## What This Project Demonstrates

* Data engineering fundamentals
* Analytical data modeling
* AI integration with data systems
* Building user-facing data applications

---

## Author

Kavya Talwar
Aspiring Data Analyst | AI + Data Engineering Enthusiast
