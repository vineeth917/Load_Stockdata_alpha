from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import pendulum

# Default arguments for the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Function to return a Snowflake connection cursor
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Defining the DAG using the context manager
with DAG(
    dag_id='stock_price_full_refresh_delete',  # Unique identifier for the DAG
    schedule='30 * * * *',                        # DAG will run daily
    start_date=datetime(2024,9,25),  # Start date is 1 day ago
    catchup=False,                            # No catchup for missed runs
    default_args=default_args,                # Set retry and delay settings
    description='Full refresh of stock data for the last 90 days using DELETE'
) as dag:

    @task
    def fetch_last_90_days_data():
        symbols = ["IBM"]
        api_key = Variable.get("Alpha_Vantage_API")
        results = []

        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
            response = requests.get(url)
            data = response.json()

            if "Time Series (Daily)" not in data:
                print(f"Error fetching data for {symbol}:", data)
                continue

            time_series = data["Time Series (Daily)"]
            end_date = datetime.today().date()
            start_date = end_date - timedelta(days=90)

            for date, values in time_series.items():
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                if start_date <= date_obj <= end_date:
                    results.append({
                        "symbol": symbol,
                        "date": date,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"])
                    })

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        return df.to_dict()

    @task
    def transform(records):
        df = pd.DataFrame(records)
        return df

    @task
    def load(df, target_table):
        cur = return_snowflake_conn()
        try:
        # Start a transaction
            cur.execute("BEGIN;")

        # Step 1: Create the table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
            symbol STRING,
            date DATE,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume INT,
            PRIMARY KEY (symbol, date)
            );"""

            cur.execute(create_table_query)
            print("Table 'stock_data' created in raw schema.")

            # Step 2: Check if there are records to insert
            if df.empty:
                print("No records to insert.")
                cur.execute("COMMIT;")  # Commit even if no records to insert
                return

            # Step 3: Delete records for the last 90 days (if they exist)
            symbol = df.iloc[0]['symbol']  # Get the symbol from the first row
            delete_query = f"""
            DELETE FROM {target_table}
            WHERE symbol = '{symbol}'
            AND date >= DATEADD(DAY, -90, CURRENT_DATE);
            """
            cur.execute(delete_query)
            print(f"Deleted records for symbol '{symbol}' in the last 90 days.")

              # Step 4: Insert new records
            for index, row in df.iterrows():
                sql = f"""
                INSERT INTO {target_table} (date, open, high, low, close, volume, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, (
                    row['date'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['symbol']
                ))

             # Commit the transaction
            cur.execute("COMMIT;")
            print("Data inserted successfully.")

        except Exception as e:
        # Rollback the transaction if an error occurs
           cur.execute("ROLLBACK;")
           print(f"Error occurred: {e}")
           raise e

        finally:
           cur.close()

    # Define task dependencies
    target_table = "COUNTRY.RAW.Alpha"
    records = fetch_last_90_days_data()
    df = transform(records)
    load(df, target_table)

