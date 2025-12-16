#  Dagster E-commerce Analytics Pipeline

A production-ready data pipeline for e-commerce analytics built with Dagster.

##  Features

- **Multi-layer Architecture**: Bronze → Silver → Gold
- **Partitioned Assets**: Daily incremental loads
- **Data Quality**: Automated validation checks
- **Scheduling**: Daily and weekly jobs
- **Sensors**: File upload monitoring
- **Testing**: Full test coverage
- **Monitoring**: Rich metadata and logging

##  Architecture

Bronze Layer (Raw)
↓
Silver Layer (Cleaned & Validated)
↓
Gold Layer (Analytics Ready)

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone <your-repo>
cd dagster-ecommerce-pipeline

python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your settings
```

### 3. Run Dagster
```bash
dagster dev -m dagster_ecommerce
```

Open http://localhost:3000

### 4. Materialize Assets

1. Go to **Assets** tab
2. Select assets by layer (bronze → silver → gold)
3. Click **Materialize selected**

##  Data Flow

### Bronze Layer
- `raw_orders`: Extract orders from API (partitioned)
- `raw_customers`: Extract customer data
- `raw_products`: Extract product catalog

### Silver Layer
- `clean_orders`: Validated and cleaned orders
- `clean_customers`: Validated customers with enrichment

### Gold Layer
- `daily_sales_summary`: Daily sales metrics by category
- `customer_lifetime_value`: CLV and RFM segmentation

##  Schedules

- **Daily ETL**: Runs every day at 2 AM
- **Weekly Full Refresh**: Runs every Sunday at 3 AM

##  Testing
```bash
# Run tests
pytest

# With coverage
pytest --cov=dagster_ecommerce --cov-report=html
```

## 📁 Project Structure