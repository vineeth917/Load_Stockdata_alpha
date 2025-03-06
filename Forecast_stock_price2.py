from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
# Define Snowflake connection ID
SNOWFLAKE_CONN_ID = 'snowflake_conn'

# SQL Queries
USE_ROLE_SQL = "use role ACCOUNTADMIN;"
USE_WAREHOUSE_SQL = "use warehouse COMPUTE_WH;"
USE_DATABASE_SQL = "use database COUNTRY;"
USE_SCHEMA_SQL = "use schema RAW;"

TRAINING_DATA_SQL = """
SELECT * FROM COUNTRY.RAW.STOCK_PRICES LIMIT 10;
"""

CREATE_VIEW_SQL = """
CREATE OR REPLACE VIEW STOCK_PRICES_v1 AS SELECT
    to_timestamp_ntz(DATE) as DATE_v1,
    CLOSE,
    SYMBOL
FROM STOCK_PRICES;
"""

CREATE_FORECAST_MODEL_SQL = """
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST lab_1_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_PRICES_v1'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE_v1',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);
"""

GENERATE_PREDICTIONS_SQL = """
BEGIN
    CALL lab_1_forecast!FORECAST(
        FORECASTING_PERIODS => 7,
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    LET x := SQLID;
    CREATE TABLE My_forecasts_2025_03_03 AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;
"""

VIEW_PREDICTIONS_SQL = """
SELECT * FROM My_forecasts_2025_03_03;
"""

UNION_PREDICTIONS_WITH_HISTORICAL_SQL = """
SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM STOCK_PRICES
UNION ALL
SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM My_forecasts_2025_03_03;
"""

INSPECT_ACCURACY_SQL = """
CALL lab_1_forecast!SHOW_EVALUATION_METRICS();
"""

FEATURE_IMPORTANCE_SQL = """
CALL lab_1_forecast!EXPLAIN_FEATURE_IMPORTANCE();
"""

# Function to execute SQL queries in Snowflake
def execute_snowflake_query(sql_query):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query)
    cursor.close()

# Define the DAG
with DAG(
    dag_id='stock_price_forecasting2',
    default_args={'owner': 'airflow'},
    schedule='@daily',                                  # DAG will run daily
    start_date=pendulum.today('UTC').add(days=-1),    # Start date is 1 day ago
    catchup=False,
    description='A DAG for stock price forecasting using Snowflake ML',
) as dag:

    @task
    def setup_snowflake_environment():
        # Execute each SQL statement separately
        execute_snowflake_query(USE_ROLE_SQL)
        execute_snowflake_query(USE_WAREHOUSE_SQL)
        execute_snowflake_query(USE_DATABASE_SQL)
        execute_snowflake_query(USE_SCHEMA_SQL)

    @task
    def inspect_training_data():
        execute_snowflake_query(TRAINING_DATA_SQL)

    @task
    def create_stock_prices_view():
        execute_snowflake_query(CREATE_VIEW_SQL)

    @task
    def create_forecast_model():
        execute_snowflake_query(CREATE_FORECAST_MODEL_SQL)

    @task
    def generate_predictions():
        execute_snowflake_query(GENERATE_PREDICTIONS_SQL)

    @task
    def view_predictions():
        execute_snowflake_query(VIEW_PREDICTIONS_SQL)

    @task
    def union_predictions_with_historical_data():
        execute_snowflake_query(UNION_PREDICTIONS_WITH_HISTORICAL_SQL)

    @task
    def inspect_model_accuracy():
        execute_snowflake_query(INSPECT_ACCURACY_SQL)

    @task
    def explain_feature_importance():
        execute_snowflake_query(FEATURE_IMPORTANCE_SQL)

    # Define task dependencies
    setup_task = setup_snowflake_environment()
    inspect_training_data_task = inspect_training_data()
    create_view_task = create_stock_prices_view()
    create_forecast_model_task = create_forecast_model()
    generate_predictions_task = generate_predictions()
    view_predictions_task = view_predictions()
    union_predictions_task = union_predictions_with_historical_data()
    inspect_accuracy_task = inspect_model_accuracy()
    explain_feature_importance_task = explain_feature_importance()

    # Task execution order
    setup_task >> inspect_training_data_task >> create_view_task >> create_forecast_model_task
    create_forecast_model_task >> generate_predictions_task >> view_predictions_task
    view_predictions_task >> union_predictions_task
    generate_predictions_task >> inspect_accuracy_task >> explain_feature_importance_task
