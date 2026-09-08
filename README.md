# Fraud Detection System

A comprehensive fraud detection solution built with **Databricks**, **Kafka**, and **PostgreSQL** for real-time credit card transaction monitoring and fraud watchlist management.

## 📋 Project Overview

This project implements an end-to-end fraud detection platform that processes credit card transactions in real-time, identifies suspicious patterns, and maintains a watchlist of fraudulent activities. The system integrates multiple data sources including PostgreSQL customer databases and Kafka streaming transactions, leveraging:

- **PostgreSQL**: Source system for customer master data and fraud detection logic
- **Kafka**: Real-time streaming of credit card transactions
- **Databricks**: Data processing, transformation, and ML models
- **Delta Lake**: Unified data lakehouse architecture (Bronze, Silver, Gold layers)

## 🏗️ Architecture

### Tech Stack
- **Primary Language**: PLpgSQL (73%) - PostgreSQL stored procedures, functions, and data ingestion logic
- **Data Processing**: Python (20%) - PySpark/Databricks notebooks and ETL jobs
- **Analytics & Testing**: Jupyter Notebooks (7%)

## 📁 Project Structure

```
Fraud_detection/
├── Credentials.ipynb                           # Databricks credential setup and secret management
├── Kafka_streaming_test.ipynb                  # Kafka streaming pipeline testing
├── Autoloader_test.py.ipynb                    # Data autoloader configuration testing
├── query test.ipynb                            # SQL query testing
│
├── Postgres_SQL/                               # PostgreSQL stored procedures and functions
│   └── [Customer data extraction and fraud detection rules]
│
├── kafka_producer/                             # Kafka producer implementations
│   └── [Producer configurations and scripts]
│
├── finguard_streaming/                         # Streaming data pipeline
│   └── [Kafka to Delta Lake streaming jobs]
│
├── finguard_customers_silver_load/             # Customer data transformation (Bronze → Silver)
│   └── [PostgreSQL to Bronze, Bronze to Silver data pipeline]
│
├── Fraud_watchlist_file_generator/             # Watchlist generation
│   └── [Fraud watchlist creation and updates]
│
└── .gitignore                                  # Git ignore rules
```

## 🎯 Key Features

### 1. **PostgreSQL Customer Data Integration**
- Extract customer master data from PostgreSQL source system
- Ingest customer data into Bronze layer (raw data)
- Transform and enrich customer data in Silver layer
- Support for customer profiles, demographics, and behavioral attributes

### 2. **Real-Time Streaming**
- Kafka-based streaming of credit card transactions from Confluent Cloud
- SASL/SSL authentication for secure data transmission
- Configurable topic subscription and partition handling
- Parallel processing of batch and streaming transactions

### 3. **Databricks Integration**
- Multi-layer data lakehouse (Bronze, Silver, Gold)
- Batch and streaming processing capabilities
- Automated data ingestion using Autoloader (cloudFiles)
- Delta Lake tables for ACID transactions and versioning
- Support for both structured and unstructured data

### 4. **Fraud Detection Components**
- Customer data enrichment and deduplication
- Transaction data ingestion and processing
- Fraud watchlist management and real-time matching
- Real-time anomaly detection based on customer behavior
- Historical fraud pattern analysis and scoring

### 5. **Credential Management**
- Secure secret storage using Databricks Secret Scopes
- Support for Kafka credentials, PostgreSQL connection strings, and API keys
- Centralized credential management for all integrations

## 📦 Components Overview

### Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  PostgreSQL (Customer DB)    Kafka Topic (CC_Transactions)      │
│       ↓                                    ↓                     │
│  Customer Master Data              Transaction Stream            │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────────┐
        │         DATABRICKS PROCESSING              │
        ├─────────────────────────────────────────────┤
        │                                              │
        │  BRONZE LAYER (Raw Data)                   │
        │  ├─ customers_bronze                        │
        │  └─ transactions_bronze                     │
        │  └─ fraud_watchlist_bronze                  │
        │                                              │
        │              ↓ (Transform)                  │
        │                                              │
        │  SILVER LAYER (Cleaned & Enriched)         │
        │  ├─ customers_silver                        │
        │  └─ transactions_silver                     │
        │                                              │
        │              ↓ (Aggregate & Model)          │
        │                                              │
        │  GOLD LAYER (Analytics Ready)              │
        │  ├─ fraud_scores                            │
        │  ├─ customer_risk_profiles                  │
        │  └─ watchlist_matches                       │
        │                                              │
        └─────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────────┐
        │     FRAUD DETECTION OUTPUTS                │
        ├─────────────────────────────────────────────┤
        │  PostgreSQL Database                        │
        │  ├─ Fraud alerts                            │
        │  ├─ Risk scores                             │
        │  ├─ Watchlist matches                       │
        │  └─ Audit logs                              │
        └─────────────────────────────────────────────┘
```

### Credentials Management (`Credentials.ipynb`)
- Creates and manages Databricks secret scopes
- Stores Kafka connection details securely
- Stores PostgreSQL connection strings
- Manages API credentials for notifications (Gmail, etc.)
- Retrieves and validates stored secrets

### PostgreSQL Customer Data Loading (`finguard_customers_silver_load/`)
- **Bronze Layer**: Raw ingestion from PostgreSQL source
  - Extracts customer master data via JDBC connector
  - Maintains source data integrity and schema
  - Tracks data lineage and ingestion timestamps

- **Silver Layer**: Cleaned and transformed data
  - Removes duplicates and handles missing values
  - Standardizes customer attributes
  - Enriches with business rules and calculations
  - Creates customer dimension tables for analytical use

### Kafka Streaming (`Kafka_streaming_test.ipynb` & `finguard_streaming/`)
- **Configuration**: Connects to Confluent Cloud Kafka cluster
- **Input Topic**: `CC_Transactions` - Real-time credit card transactions
- **Processing**:
  - Reads messages in batch and streaming modes
  - Parses transaction data from Kafka messages
  - Applies SASL/SSL authentication

- **Output Tables**:
  - `finguard.bronze.transactions_batch_test` - Batch transaction history
  - `finguard.bronze.transactions_streaming_test` - Real-time transactions

### Data Autoloader (`Autoloader_test.py.ipynb` & `Fraud_watchlist_file_generator/`)
- Auto-detects and processes new files from cloud storage
- Supports JSON format for fraud watchlist files
- Infers schema automatically with schema evolution
- Writes to `finguard.bronze.fraud_watchlist_batch_test` table
- Tracks file metadata and ingestion timestamps

### PostgreSQL Backend (`Postgres_SQL/`)
- Stores fraud detection rules and thresholds
- Manages alert configurations
- Maintains fraud history and patterns
- Stores customer risk classifications
- Logs all fraud detection activities

### SQL Utilities (`query test.ipynb`)
- Test and verify SQL queries
- Database connectivity checks
- Data quality validations
- Performance testing scripts

## 🚀 Setup & Configuration

### Prerequisites
- Databricks workspace with compute cluster (Databricks Runtime 12.x+)
- Confluent Cloud Kafka cluster with API credentials
- PostgreSQL database (source system for customer data)
- Azure storage or similar cloud storage for data volumes
- Python 3.8+ with PySpark compatibility

### Initial Setup Steps

#### 1. Credential Configuration
```
Run Credentials.ipynb first:
- Configure Databricks API endpoint and token
- Set up Kafka bootstrap servers and credentials
- Store PostgreSQL connection string
- Add Gmail API key for notifications
```

#### 2. PostgreSQL Connection Setup
```python
# Connection parameters to configure:
POSTGRES_HOST = "your-postgres-host"
POSTGRES_PORT = 5432
POSTGRES_DB = "fraud_detection_db"
POSTGRES_USER = "your_username"
POSTGRES_PASSWORD = "your_password"

# Source tables:
CUSTOMER_TABLE = "customers"
TRANSACTION_TABLE = "transactions"
```

#### 3. Kafka Configuration
```
Confluent Cloud Settings:
- Bootstrap servers: pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092
- Topic name: CC_Transactions
- API Key: [Configure from Confluent]
- API Secret: [Configure from Confluent]
- Security: SASL/SSL enabled
```

#### 4. Databricks Cluster Setup
```
Required Libraries:
- pyspark
- delta-spark
- postgresql-jdbc (for PostgreSQL connectivity)
- confluent-kafka-python
```

#### 5. Data Pipeline Deployment
```
Step 1: Run finguard_customers_silver_load
        └─> Ingest PostgreSQL customer data to Bronze/Silver

Step 2: Run kafka_producer
        └─> Start producing transactions to Kafka

Step 3: Run finguard_streaming
        └─> Consume transactions and write to Delta Lake

Step 4: Run Fraud_watchlist_file_generator
        └─> Generate and update fraud watchlist
```

## 🔄 Data Flow Details

### Customer Data Pipeline (PostgreSQL → Databricks)

```
PostgreSQL Customer Table
           ↓
    [JDBC Extraction]
           ↓
    Bronze Layer: finguard.bronze.customers
    (Raw customer records with metadata)
           ↓
    [Data Quality Checks]
    [Deduplication]
    [Schema Validation]
           ↓
    Silver Layer: finguard.silver.customers
    (Clean, enriched customer data)
           ↓
    Available for joining with transactions
    and fraud scoring operations
```

### Transaction & Fraud Detection Pipeline

```
Kafka: CC_Transactions
           ↓
    [SASL/SSL Authentication]
           ↓
    Bronze: transactions_batch/streaming
           ↓
    [Join with Customer Silver Data]
    [Apply Business Rules]
    [Calculate Risk Scores]
           ↓
    Silver: transactions_enriched
           ↓
    [Anomaly Detection]
    [Fraud Pattern Matching]
    [Watchlist Lookup]
           ↓
    Gold: fraud_scores, alerts
           ↓
    PostgreSQL: Store alerts and logs
```

## 🔒 Security Features

- **Secret Management**: Databricks secret scopes for credentials
- **Encryption in Transit**: SASL/SSL for Kafka and PostgreSQL connections
- **Encryption at Rest**: Delta Lake with Azure encryption
- **Access Control**: Role-based access in Databricks and PostgreSQL
- **Audit Logging**: Track data access, modifications, and fraud alerts
- **Credential Rotation**: Support for regular credential updates

## 📊 Monitoring & Alerting

- Real-time transaction monitoring against customer profiles
- Behavioral anomaly detection using historical patterns
- Fraud watchlist real-time matching
- Customer risk scoring and tiering
- Email alerts via Gmail API for high-risk transactions
- Delta Lake transaction history for full audit trails
- Checkpoint management for streaming job recovery

## 🛠️ Development & Testing

- Jupyter notebooks for exploratory analysis
- Automated test pipelines for data quality validation
- SQL query testing and optimization
- Streaming job testing and failure recovery
- PostgreSQL query performance testing

## 📝 Notebooks Description

| Notebook | Purpose |
|----------|---------|
| `Credentials.ipynb` | Set up and manage Databricks secrets, Kafka, PostgreSQL credentials |
| `Kafka_streaming_test.ipynb` | Test Kafka connections, streaming pipelines, and data parsing |
| `Autoloader_test.py.ipynb` | Test cloud file auto-loading, schema inference, and ingestion |
| `query test.ipynb` | Validate SQL queries, database connectivity, data quality checks |

## 🔧 Configuration Variables

### PostgreSQL Configuration
```python
POSTGRES_HOST = 'your-db-host'
POSTGRES_PORT = 5432
POSTGRES_DATABASE = 'fraud_detection'
POSTGRES_USER = 'your_user'
POSTGRES_PASSWORD = '[Stored in Databricks Secrets]'
```

### Kafka Configuration
```python
BOOTSTRAP_SERVERS = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
API_KEY = '[Stored in Databricks Secrets]'
API_SECRET = '[Stored in Databricks Secrets]'
TOPIC_NAME = 'CC_Transactions'
SECURITY_PROTOCOL = 'SASL_SSL'
SASL_MECHANISM = 'PLAIN'
```

### Databricks Configuration
```python
secret_scope_name = "finguard-scope"
checkpoint_location = "/Volumes/finguard/source/transactions/checkpoint/"
schema_location = "/Volumes/finguard/source/fraud_watchlist/schema/"
customer_bronze_path = "/Volumes/finguard/source/customers/bronze/"
customer_silver_path = "/Volumes/finguard/source/customers/silver/"
```

## 📚 Database Schema

### PostgreSQL Tables
- **customers**: Master customer records with demographics
- **transactions**: Historical transaction data
- **fraud_rules**: Fraud detection rules and thresholds
- **alerts**: Generated fraud alerts and notifications
- **audit_log**: Audit trail of all operations

### Databricks Delta Tables
- **Bronze Layer**: Raw data from PostgreSQL and Kafka
  - `customers_bronze`, `transactions_bronze`, `fraud_watchlist_bronze`
- **Silver Layer**: Cleaned and enriched data
  - `customers_silver`, `transactions_silver`
- **Gold Layer**: Analytics-ready aggregated data
  - `fraud_scores`, `customer_risk_profiles`, `watchlist_matches`

## 🔄 ETL/ELT Process

```
1. EXTRACT
   ├─ PostgreSQL (JDBC): Customer master data
   └─ Kafka: Real-time transactions

2. LOAD
   ├─ Bronze Layer: Raw data ingestion
   └─ Checkpoint: Streaming state management

3. TRANSFORM
   ├─ Data quality checks
   ├─ Deduplication & standardization
   ├─ Customer-transaction joins
   └─ Enrichment with business rules

4. DETECT
   ├─ Pattern matching against fraud rules
   ├─ Anomaly detection
   ├─ Watchlist matching
   └─ Risk scoring

5. OUTPUT
   └─ PostgreSQL: Alerts, scores, and audit logs
```

## 📊 Key Metrics & KPIs

- Transaction processing latency (from Kafka to Bronze)
- Customer data refresh frequency
- Fraud detection accuracy rate
- False positive rate
- Processing throughput (transactions/second)
- Data quality score
- System availability and uptime

## 🚀 Deployment Guide

1. **Clone Repository**
   ```bash
   git clone https://github.com/Ankitbisht01/Fraud_detection.git
   ```

2. **Set Up Databricks Cluster**
   - Create cluster with Databricks Runtime 12.x+
   - Install required libraries

3. **Configure Secrets**
   - Run `Credentials.ipynb` to set up secret scopes
   - Validate all connections

4. **Deploy Data Pipelines**
   - Deploy PostgreSQL customer loader first
   - Then deploy Kafka streaming pipeline
   - Finally, deploy fraud detection jobs

5. **Validation**
   - Run query tests to validate data
   - Check alert generation
   - Monitor pipeline performance

## 📖 Documentation

- Databricks Best Practices: [Link to internal docs]
- Kafka Configuration: See `Kafka_streaming_test.ipynb`
- PostgreSQL Setup: See `finguard_customers_silver_load/`
- Fraud Rules: See `Postgres_SQL/`

## 🔗 Related Technologies

- **Databricks**: https://databricks.com/
- **Delta Lake**: https://delta.io/
- **Apache Kafka**: https://kafka.apache.org/
- **PostgreSQL**: https://www.postgresql.org/
- **PySpark**: https://spark.apache.org/docs/latest/api/python/

## 📞 Support & Contributions

For issues, questions, or contributions, please refer to the repository's issues and pull requests.

## 📄 License

[Add your license information here]

## 👤 Author

- **Ankit Bisht** (@Ankitbisht01)

---

**Last Updated**: September 2026

**Repository**: [Ankitbisht01/Fraud_detection](https://github.com/Ankitbisht01/Fraud_detection)

**Status**: Active Development 🚀
