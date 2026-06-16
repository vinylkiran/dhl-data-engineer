# Technical Decision Records — DHL Data Engineering Platform
## DE Portfolio — Project 06 Reference Artifact

---

## Overview

This document records every major technology and design choice made during the build of the DHL Data Engineering platform. For each decision, the context, options considered, the choice made, and the trade-offs accepted are documented. This is the document a technical interviewer or new team member would read to understand *why* the platform is built the way it is.

---

## TDR-01: Database Engine — DuckDB over PostgreSQL or Cloud Warehouse

### Context
The platform needed a database engine capable of handling analytical workloads (GROUP BY aggregations over millions of rows, window functions, complex JOINs) without requiring any cloud infrastructure or server setup.

### Options Considered

**PostgreSQL (local)**
- Well-understood OLTP/OLAP hybrid; extensive ecosystem
- Requires a running server process, user management, connection configuration
- Row-oriented storage means column-scan aggregations are slower than columnar alternatives
- Overkill for a single-developer portfolio environment

**Snowflake / BigQuery / Redshift (cloud)**
- True production-grade cloud analytical warehouses with autoscaling, concurrency, and storage separation
- Require cloud accounts, billing configuration, credentials management
- Latency for small datasets is higher than local due to network round-trips
- Not appropriate as a portfolio-demonstration environment (cost, complexity, requires cloud access to evaluate)

**DuckDB**
- Embedded columnar analytical database; runs in-process with zero infrastructure
- Single `.duckdb` file is fully portable — the entire 26-table warehouse ships as one 193MB file
- Native support for SQL window functions, CTEs, INTERVAL arithmetic, COPY FROM CSV
- Python API (via `pip install duckdb`) integrates seamlessly with pandas DataFrames
- Vectorised execution engine: GROUP BY on 219,000 rows completes in under 100ms

### Decision: DuckDB

### Trade-offs Accepted
- **Concurrency**: DuckDB supports one writer at a time. In production with multiple pipelines running concurrently, this would require coordination (a queue, or separate warehouse files per pipeline). At portfolio scale this is not a problem.
- **Scale ceiling**: DuckDB is designed for single-machine analytical workloads up to hundreds of GB. Beyond that, a proper cloud warehouse is needed.
- **Production realities**: A real DHL data platform runs on Snowflake, Databricks, or Redshift. DuckDB is the correct choice for demonstrating data engineering skills without infrastructure overhead.

---

## TDR-02: ETL Framework — Python Scripts over Airflow or dbt

### Context
All five pipelines needed ETL (extract, transform, load) logic that could be version-controlled, tested, debugged, and demonstrated without external orchestration infrastructure.

### Options Considered

**Apache Airflow**
- Industry-standard DAG-based workflow orchestrator with retry logic, scheduling, web UI, and operator ecosystem
- Requires a running Airflow server (Docker or a managed service), Celery or Kubernetes executor for parallelism, separate metadata DB
- Would add significant setup overhead for a portfolio project; evaluators would spend time understanding Airflow configuration rather than the data engineering logic itself

**dbt (data build tool)**
- Excellent for SQL-centric transformation logic, lineage tracking, documentation, and testing within a warehouse
- Not designed for the extraction or loading phases (it is a T in ELT, not ETL)
- Works best when raw data is already in a warehouse; would require a separate extraction layer regardless
- Less flexible for custom Python transformations (SCD2 logic, SHA-256 hashing, statistical calculations like lift scores)

**Custom Python scripts**
- Full control over every aspect of the pipeline
- Directly demonstrates Python data engineering skills: pandas, DuckDB Python API, logging, error handling, watermark patterns, incremental loading
- Zero dependencies beyond Python standard library + pandas + duckdb
- Each pipeline is independently runnable with `python pipeline.py`

### Decision: Custom Python scripts

### Trade-offs Accepted
- **No scheduling**: Pipelines must be triggered manually or via a simple cron job. A production environment would use Airflow, Prefect, or Dagster for scheduling, retry logic, and alerting.
- **No automatic lineage tracking**: dbt would provide auto-generated lineage graphs. This platform documents lineage manually in `lineage/master_data_lineage.md`.
- **No dependency management between pipelines**: Pipeline 05 depends on Pipeline 04 having run first. This dependency is documented but not enforced by code. An orchestrator would enforce this.

### What This Demonstrates
The custom Python approach deliberately exposes the underlying data engineering logic — watermark management, SCD2 pattern implementation, incremental load patterns, quality framework design — that would be abstracted away by dbt or Airflow. This is the right trade-off for a portfolio designed to demonstrate engineering depth.

---

## TDR-03: Data Modelling — Star Schema over Flat Denormalised Tables

### Context
Raw source CSV files are essentially flat: `wms_tasks.csv` has warehouse_id, sku_id, operator_id, task_type all in a single row. The question was whether to load this flat structure directly or normalise into a dimensional model.

### Options Considered

**Flat denormalised tables**
- Single wide table per domain (e.g., one massive `wms_fact` with all SKU attributes, warehouse attributes, operator attributes embedded)
- Simple to load; no JOIN logic required
- Query performance degrades with scale as more columns are scanned
- No ability to update dimension attributes without rewriting every fact row (e.g., if a warehouse changes its region, all 219,000 task rows need updating)
- Dimension history (SCD2) is impossible without structural redesign

**Normalised (3NF)**
- Eliminates all redundancy; every fact references only keys
- Correct for OLTP systems where writes are frequent
- Terrible for analytical queries — requires many JOINs to reconstruct any meaningful report
- DuckDB's vectorised JOIN execution handles star schema JOINs efficiently; 3NF would be worse for read performance without the write benefits

**Kimball Star Schema**
- Fact tables contain only measurable events (task executions, orders, demand) with foreign keys to dimension tables
- Dimension tables contain descriptive attributes (warehouse name, SKU category, operator cohort)
- Supports SCD2 for dimension history
- Query patterns are simple: `fact JOIN dim WHERE dim.attribute = value GROUP BY dim.another_attribute`
- Standard pattern in data warehousing; immediately recognisable to any analytics professional

### Decision: Kimball Star Schema

### Trade-offs Accepted
- **Join cost**: Every analytical query requires at least one JOIN. At 219,000 fact rows this is negligible; at 1 billion rows in production this would require proper indexing and partitioning strategy.
- **Modelling overhead**: Creating separate dimension tables requires more upfront design than flat loading. This is intentional — it forces explicit thinking about grain, cardinality, and refresh patterns.

---

## TDR-04: Operator Anonymisation — SHA-256 Hash Truncation

### Context
Project 04 ingests WMS operator IDs (`OP-0001` through `OP-0060`) which represent real warehouse workers. In a production DHL environment these would be PII. Anonymisation was required before storage.

### Options Considered
- **Tokenisation**: Replace with sequential integers (Operator 1, 2, 3). Simple but reversible if the mapping is stored.
- **Pseudonymisation**: Store a mapping table of raw ID → pseudonym separately. Reversible with the mapping.
- **One-way hash (SHA-256)**: Apply SHA-256 to the raw ID, truncate to 8 hex chars, prefix with "OP-". Irreversible without brute force.

### Decision: SHA-256 hash truncation

`hashlib.sha256("OP-0001".encode()).hexdigest()[:8].upper()` → `OP-3C5A7D2B`

Deterministic (same raw ID always produces the same hash), non-reversible, and consistent across ETL runs. The 8-char truncation reduces collision risk to 1/4 billion, which is more than sufficient for a 60-operator dataset.

---

## TDR-05: What Would Change at Production DHL Scale

The following architectural choices would be different at the scale and operational requirements of a real DHL data platform:

### Orchestration
Replace custom Python scripts called manually with **Apache Airflow** or **Prefect**. Pipeline dependencies (P04 before P05) would be enforced as DAG dependencies. Scheduling, retry logic, SLA alerting, and run history would be managed by the orchestrator.

### Storage and Compute
Replace DuckDB with **Snowflake** or **Databricks** (Delta Lake). Reasons: multi-user concurrency, compute/storage separation for independent scaling, built-in Time Travel for SCD queries, native integration with BI tools (Tableau, Power BI) via JDBC/ODBC, production-grade access control and audit logging.

### Real-Time Streaming for WMS Events
WMS task events (Picks, Putaways) in a live DHL warehouse occur in real time — the current batch ETL runs on a daily schedule. At production scale this would be replaced with an **Apache Kafka** event stream → **Apache Flink** or **Spark Structured Streaming** → Delta Lake landing zone → micro-batch fact table updates. This reduces the latency from hours (batch) to seconds (streaming).

### CI/CD for Pipeline Deployment
Replace manual `git push` + `python pipeline.py` with a **CI/CD pipeline** (GitHub Actions or Jenkins) that: runs unit tests on every pull request, runs integration tests against a staging warehouse, deploys to production on merge to main, and rolls back on failure. Schema migrations would be managed with a tool like **Flyway** or **Liquibase**.

### Data Catalogue and Governance
The `lineage/` and `standards/` documents in this project exist as markdown files. At production scale these would be managed in a **data catalogue** (Alation, Collibra, or DataHub) with automated lineage tracking, column-level data classification (PII tagging), access request workflows, and SLA monitoring dashboards.

### Testing
Replace the informal DQ checks with a formal **data testing framework** (Great Expectations or dbt tests) integrated into the CI/CD pipeline. Every table would have a test suite run automatically on each pipeline execution, and failures would block deployment.
