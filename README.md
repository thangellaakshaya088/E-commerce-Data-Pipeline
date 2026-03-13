# ShopStream

I built this project to learn how modern data pipelines work end-to-end — not just the theory, but actually wiring everything together. ShopStream simulates a real e-commerce company generating clickstream events, orders, user signups, and product updates, then processes all of it through a layered pipeline from raw ingestion to analytics-ready tables.

Everything runs locally on Docker Compose. No AWS, no GCP, no paid services.

The pipeline generates fake but realistic data using Python's Faker library, streams it through Kafka, lands it in MinIO (which is just S3 on your laptop), processes it with PySpark across Bronze/Silver/Gold layers, transforms it with dbt, orchestrates everything with Airflow, and finally makes it queryable through DuckDB. There's also a Great Expectations layer that validates data quality at each stage before it moves forward.

---

## What each tool is doing here

**Python + Faker** generates the synthetic data — clickstream sessions, orders with line items, user profiles, product catalogs. All realistic enough to stress-test the pipeline.

**Apache Kafka** is the message broker. The generators push events to Kafka topics, and Spark reads from those topics in structured streaming mode.

**Apache Spark 3.5** handles both the streaming ingestion (Kafka → Bronze) and all the batch processing jobs (Bronze → Silver → Gold). PySpark throughout.

**Delta Lake** is the storage format sitting on top of MinIO. It gives you ACID transactions, schema evolution, and MERGE upserts — basically makes object storage behave like a proper data lake.

**MinIO** is a self-hosted S3-compatible object store. Spark talks to it using the S3A connector, so you can swap it out for real S3 with zero code changes.

**PostgreSQL** serves two purposes — it's the source OLTP database that gets batch-extracted into the Bronze layer, and it also stores the final Gold tables that Airflow exports at the end of each pipeline run.

**dbt-core** handles the SQL transformation layer on top of PostgreSQL. Staging models clean and cast the raw data, intermediate models join and reshape it, and mart models produce the final business-facing tables.

**Apache Airflow** orchestrates the whole thing with five daily DAGs that run in sequence. Each DAG waits for the previous one to finish using ExternalTaskSensor before it starts.

**DuckDB** is used for ad-hoc analytics directly on the Gold Delta files sitting in MinIO. No need to load data anywhere — it reads Parquet/Delta in place.

**Great Expectations** validates data quality at each layer. If something looks wrong (nulls where they shouldn't be, out-of-range values, unexpected statuses), the pipeline fails loudly before bad data moves downstream.

---

## Forking and getting it running

**What you need installed:**
- Docker and Docker Compose
- Python 3.11
- Git

**Fork it on GitHub**, then clone your fork:

```bash
git clone https://github.com/your-username/shopstream.git
cd shopstream
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the bootstrap script — this starts all the Docker containers, creates the MinIO buckets, seeds PostgreSQL with some initial data, and gets Airflow initialised:

```bash
bash scripts/bootstrap.sh
```

Give it about a minute for everything to come up. Once it's ready you'll see the URLs printed in the terminal.

---

## Accessing the services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8081 | — |
| PostgreSQL | localhost:5432 | shopstream / shopstream123 |

---

## Generating data

Once everything is up, start the data generator to simulate live traffic:

```bash
# Keeps producing events every 2 seconds indefinitely
python -m data_generator.kafka_producer --mode continuous

# Just a one-time batch if you want to test something quickly
python -m data_generator.kafka_producer --mode batch
```

---

## Running the pipeline manually

Airflow handles this automatically on a schedule, but you can trigger each step yourself:

```bash
# Pull from PostgreSQL into Bronze layer
python -m src.ingestion.postgres_to_bronze

# Clean and type the raw data into Silver
python -m src.processing.bronze_to_silver

# Build session-level aggregations from clickstream
python -m src.processing.session_builder

# Aggregate Silver into Gold business metrics
python -m src.processing.silver_to_gold

# Run dbt transformations
cd dbt && dbt run && dbt test

# Run data quality checks across all layers
python -m src.quality.run_validations all

# Query the Gold tables through DuckDB
python -m src.serving.duckdb_queries
```

---

## Running tests

```bash
pytest tests/ -v -m "not integration" --cov=src --cov=data_generator
```

Tests marked `integration` require live Docker services. Everything else runs without any dependencies.

---

## Folder structure

```
shopstream/
│
├── data_generator/
│   ├── clickstream_generator.py    session-aware event simulation
│   ├── orders_generator.py         orders with line items and statuses
│   ├── users_generator.py          user profiles with segments
│   ├── products_generator.py       product catalog with categories
│   ├── kafka_producer.py           publishes all generators to Kafka
│   └── config.py                   topic names, batch sizes, constants
│
├── src/
│   ├── ingestion/
│   │   ├── kafka_to_bronze.py      Spark Structured Streaming → Delta
│   │   ├── postgres_to_bronze.py   JDBC batch extract from PostgreSQL
│   │   └── file_to_bronze.py       CSV/JSON flat file loader
│   │
│   ├── processing/
│   │   ├── bronze_to_silver.py     dedup, null filter, type casting
│   │   ├── silver_to_gold.py       revenue, funnel, customer metrics
│   │   ├── session_builder.py      clickstream sessionization
│   │   └── utils/
│   │       ├── spark_session.py    SparkSession factory with S3/Delta config
│   │       ├── delta_helpers.py    write, upsert, optimize, vacuum helpers
│   │       └── schema_definitions.py  Spark schemas for each topic
│   │
│   ├── quality/
│   │   ├── expectations/           GX suites for bronze, silver, gold
│   │   └── run_validations.py      entry point for running checks
│   │
│   └── serving/
│       ├── duckdb_queries.py       analytical queries on Gold Delta files
│       └── export_to_postgres.py   load Gold tables back to PostgreSQL
│
├── dbt/
│   ├── models/
│   │   ├── staging/                clean and cast source tables
│   │   ├── intermediate/           joins and reshaping (ephemeral)
│   │   └── marts/                  fct_daily_revenue, dim_customers, etc.
│   ├── snapshots/                  SCD Type 2 on products
│   ├── macros/                     reusable SQL helpers
│   ├── seeds/                      country_codes lookup
│   └── schema.yml                  source + model tests and descriptions
│
├── airflow/
│   ├── dags/
│   │   ├── bronze_ingestion_dag.py
│   │   ├── silver_processing_dag.py
│   │   ├── gold_aggregation_dag.py
│   │   ├── dbt_run_dag.py
│   │   └── data_quality_dag.py
│   └── plugins/
│       └── slack_webhook.py        optional failure alerts
│
├── tests/
│   ├── test_generators.py
│   ├── test_bronze_to_silver.py
│   ├── test_silver_to_gold.py
│   └── test_dbt_models.py
│
├── scripts/
│   ├── bootstrap.sh                one-command stack startup
│   ├── seed_postgres.py            populates OLTP tables with fake data
│   └── backfill.py                 re-run pipeline for a date range
│
├── notebooks/
│   └── exploration.ipynb           DuckDB queries for quick exploration
│
├── docs/
│   ├── architecture.md             pipeline diagram and layer descriptions
│   └── data_dictionary.md          column-level docs for every table
│
├── docker-compose.yml
├── requirements.txt
└── .env                            all config — edit this before running
```

---

## Airflow DAG schedule

The five DAGs run back-to-back each night. Each one waits for the previous to complete before starting.

| DAG | Runs at | What it does |
|---|---|---|
| bronze_ingestion | 02:00 | Extracts PostgreSQL tables into Bronze Delta |
| silver_processing | 03:00 | Cleans and deduplicates Bronze into Silver |
| gold_aggregation | 04:00 | Aggregates Silver into Gold business metrics |
| dbt_transformations | 05:00 | Runs dbt models and tests on PostgreSQL |
| data_quality | 06:00 | Validates all three layers with Great Expectations |

---

## Backfilling historical data

```bash
python scripts/backfill.py --start 2024-01-01 --end 2024-03-31
```

---

## Further reading

- [Architecture overview](docs/architecture.md)
- [Data dictionary](docs/data_dictionary.md)
