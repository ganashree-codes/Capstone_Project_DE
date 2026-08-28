# FinShield — Real-Time Fraud Detection & Governance Pipeline

## 🚧 Status
In progress — Day 1 of 6. Building out ingestion layer.

## Overview
A real-time financial transaction processing pipeline that streams credit card
transactions, enriches them with merchant risk data, detects fraud patterns,
masks PII, and stores results in a Medallion Architecture (Bronze → Silver →
Gold) on Snowflake, visualized in Power BI.

## Architecture
*(diagram goes here once built — Excalidraw/draw.io export)*

```
Kaggle CSV → Kafka → PySpark Structured Streaming → Snowflake (Bronze/Silver)
Faker merchant risk data → NiFi → Kafka → (joined into Spark stream)
Snowflake Silver → Airflow + dbt → Snowflake Gold → Power BI
```

## Tech Stack
| Tool | Role |
|---|---|
| Python | Data streaming producer, EDA |
| Apache Kafka | Event streaming backbone |
| Apache NiFi | Secondary source ingestion (merchant risk feed) |
| PySpark (Structured Streaming) | Real-time enrichment, PII masking, fraud detection |
| Snowflake | Medallion-architecture data warehouse |
| dbt | Silver → Gold transformations, testing |
| Apache Airflow | Orchestration |
| Power BI | Fraud analytics dashboard |

## Dataset
- **Transactions**: [Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection) (Kaggle, simulated but realistic, includes ground-truth fraud labels)
- **Merchant risk scores**: synthetically generated via Faker, streamed through NiFi

*(See `data_provenance.md` for full details on what's real vs. simulated.)*

## Key Findings
*(fill in after EDA / SQL analysis — e.g. class imbalance %, top fraud categories, fraud rate by state)*

## How to Run
*(fill in once pipeline components are working — setup steps, env vars needed, how to start each service)*

## Design Decisions
*(e.g. why stream-static join over stream-stream, why hashing not encryption, why NiFi for the second source)*

## Demo
*(link to demo video/GIF once recorded)*
