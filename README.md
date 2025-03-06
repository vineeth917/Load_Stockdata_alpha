# Building a Stock Price Prediction Analytics using Snowflake & Airflow

## Overview
This project focuses on developing a stock price prediction analytics system using data from the **yfinance** API. The system retrieves stock prices for selected companies, stores them in **Snowflake**, processes the data using **Airflow DAGs**, and forecasts future stock prices using **Snowflake ML**.

## Project Structure
### 1. **Data Collection (ETL Pipeline)**
- Data is fetched for GOOGL and AAPL using the **yfinance** API for 180 days.
- The extracted data includes:
  - Stock Symbol
  - Date
  - Open, Close, High, Low, and Volume
- Data is stored in **Snowflake** in the `stock_prices` table.
- **Airflow DAG** (`lab_1_dag.py`) automates daily data ingestion.

### 2. **Data Processing & Machine Learning (Forecasting Pipeline)**
- **Snowflake ML Forecasting** is used to predict stock prices for the next **7 days**.
- A view `STOCK_PRICES_v1` is created for preprocessing.
- **Airflow DAG** (`Forecast_stock_price.py`) automates forecasting using Snowflake ML.
- Forecasted data is merged with historical data for analysis.

## Technologies Used
- **Python** (for data extraction and pipeline scripting)
- **Airflow** (workflow orchestration)
- **Snowflake** (data storage, transformation, and ML model execution)
- **yfinance API** (data source for stock market prices)

## Setup Instructions
### 1. Install Dependencies
Ensure you have **Python 3.x** installed and install the required dependencies:
```sh
pip install airflow snowflake-connector-python yfinance pandas requests pendulum
```

### 2. Set Up Airflow
- Initialize Airflow:
```sh
airflow db init
```
- Start the Airflow web server:
```sh
airflow webserver -p 8080
```
- Start the Airflow scheduler:
```sh
airflow scheduler
```
- Configure **Airflow Variables**:
  - `snowflake_default`: Airflow connection for Snowflake.

### 3. Configure Snowflake Database
Run the following SQL commands in Snowflake:
```sql
CREATE DATABASE stock_price;
CREATE SCHEMA raw_data;
```

### 4. Deploy DAGs in Airflow
- Copy `lab_1_dag.py` and `Forecast_stock_price.py` to the **Airflow DAGs folder**.
- Enable and trigger the DAGs from the **Airflow Web UI**.

## Running the Project
1. Start Airflow services.
2. Navigate to the **Airflow Web UI** (`http://localhost:8080`).
3. Enable the `Lab_1_DAG` for data ingestion.
4. Enable the `stock_price_forecasting` DAG for stock price prediction.
5. Monitor DAG execution and Snowflake tables for results.

## Expected Outputs
- **Daily Updated Stock Prices** in the `stock_prices` table.
- **Forecasted Stock Prices** for the next **7 days** stored in Snowflake.
- **Airflow Web UI Screenshots** showcasing the running DAGs.
- **SQL Queries for Forecasting & Feature Analysis** in Snowflake.

## References
- [yfinance API](https://pypi.org/project/yfinance/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Airflow Documentation](https://airflow.apache.org/docs/)

---
### Team:
Shashidhar Babu P V D
Vineeth Rayadurgam 

DATA 226 - Spring 2025
