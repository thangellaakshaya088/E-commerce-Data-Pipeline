# Architecture

## Overview

ShopStream is a fully local batch + streaming data pipeline built with open-source tools running in Docker Compose.

```
[Python Faker]
      |
      v
[Apache Kafka] ──────────────────────────────────────────────────────┐
                                                                      |
[PostgreSQL OLTP] ────────────────────────────────────────────────────┤
                                                                      |
                                              ┌───────────────────────┘
                                              v
                                    [PySpark Structured Streaming]
                                    [PySpark Batch Extract]
                                              |
                                              v
                                    [Bronze Layer - MinIO / Delta Lake]
                                    (raw, append-only, partitioned by date)
                                              |
                                    [PySpark Batch Jobs]
                                              |
                                              v
                                    [Silver Layer - MinIO / Delta Lake]
                                    (cleaned, typed, deduped)
                                              |
                                    [PySpark Batch Jobs]
                                              |
                                              v
                                    [Gold Layer - MinIO / Delta Lake]
                                    (aggregated business metrics)
                                              |
                              ┌───────────────┴──────────────┐
                              v                              v
                     [dbt Models on PostgreSQL]     [DuckDB on Delta files]
                     (staging → intermediate → mart) (ad-hoc analytics)
                              |
                              v
                     [PostgreSQL - Gold Tables]
                     (for BI tools / reporting)
```

## Layers

**Bronze** — Raw data as ingested. No transformations. Append-only Delta tables, partitioned by date.

**Silver** — Cleaned and typed. Deduplication, null handling, type casting. Upsert-capable via Delta MERGE.

**Gold** — Business-ready aggregations. Daily revenue, conversion funnel, customer metrics.

## Orchestration

Apache Airflow runs 5 DAGs in sequence:

1. `bronze_ingestion` — 02:00 daily
2. `silver_processing` — 03:00 daily
3. `gold_aggregation` — 04:00 daily
4. `dbt_transformations` — 05:00 daily
5. `data_quality` — 06:00 daily

Each DAG waits for the previous via `ExternalTaskSensor`.

## Streaming

Kafka → Bronze uses PySpark Structured Streaming with Delta Lake as the sink. Checkpoints stored in MinIO prevent data loss on restart.

## Idempotency

All Spark jobs are safe to re-run:
- Bronze streaming uses Kafka offsets + checkpoints
- Silver writes use `overwrite` mode with partition pruning
- Gold uses Delta MERGE for upserts
- dbt incremental models use `merge` strategy with `unique_key`
