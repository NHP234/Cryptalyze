import duckdb
import polars as pl
from data_processor import BulkDataDownloader
from pathlib import Path

# --- Configuration ---
DB_FILE = "data/klines.duckdb"
TABLE_NAME = "klines"
# Set the number of days of historical data you want to download.
# Each file typically represents one day.
NUMBER_OF_DAYS_TO_DOWNLOAD = 30 
# ---------------------

def create_table_if_not_exists(con: duckdb.DuckDBPyConnection):
    """
    Creates the klines table in DuckDB with the correct schema if it doesn't already exist.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        open_time BIGINT PRIMARY KEY,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        close_time BIGINT,
        quote_asset_volume DOUBLE,
        number_of_trades BIGINT,
        taker_buy_base_volume DOUBLE,
        taker_buy_quote_volume DOUBLE,
        ignore VARCHAR,
        open_datetime TIMESTAMP,
        close_datetime TIMESTAMP
    );
    """
    con.execute(create_table_sql)
    print(f"✅ Table '{TABLE_NAME}' is ready in DuckDB.")

def upsert_data(con: duckdb.DuckDBPyConnection, df: pl.DataFrame):
    """
    Efficiently upserts a Polars DataFrame into a DuckDB table.
    """
    if df.is_empty():
        print("⚠️ DataFrame is empty, nothing to upsert.")
        return

    print(f"🚀 Preparing to upsert {df.height} records into DuckDB...")
    
    # Register the Polars DataFrame as a temporary virtual table in DuckDB
    con.register('updates', df)
    
    # Prepare the SQL SET clause for the update part of the upsert
    update_cols = [col for col in df.columns if col != 'open_time']
    set_clause = ", ".join(f'"{col}" = updates."{col}"' for col in update_cols)

    # Construct the full UPSERT (INSERT ... ON CONFLICT DO UPDATE) query
    upsert_sql = f"""
    INSERT INTO {TABLE_NAME}
    SELECT * FROM updates
    ON CONFLICT (open_time) DO UPDATE
    SET {set_clause};
    """
    
    # Execute the query
    con.execute(upsert_sql)
    
    # Clean up the registered virtual table
    con.unregister('updates')
    
    print(f"💾 Successfully upserted {df.height} records into '{TABLE_NAME}'.")

def main():
    """
    Main function to download historical data and load it into DuckDB.
    """
    print("--- Starting Historical Data Load ---")
    
    # Ensure the data directory exists
    Path("data").mkdir(parents=True, exist_ok=True)
    
    # --- Step 1: Download data from Binance ---
    print(f"🌎 Downloading last {NUMBER_OF_DAYS_TO_DOWNLOAD} days of data from Binance...")
    downloader = BulkDataDownloader(symbol="BTCUSDT", interval="1m")
    # We call get_polars_df without the 'upload' flag, as we are handling DB operations here.
    historical_df = downloader.get_polars_df(nfiles=NUMBER_OF_DAYS_TO_DOWNLOAD)

    if historical_df is None or historical_df.is_empty():
        print("❌ No data downloaded. Exiting.")
        return
        
    # --- Step 2: Connect to DB and load data ---
    con = None
    try:
        con = duckdb.connect(database=DB_FILE, read_only=False)
        
        # Ensure the table exists with the correct schema
        create_table_if_not_exists(con)
        
        # Upsert the downloaded data into the table
        upsert_data(con, historical_df)
        
        # Verify the data load
        row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"\n--- Verification ---")
        print(f"Total rows in table '{TABLE_NAME}': {row_count}")
        print(f"----------------------")
        
    except Exception as e:
        print(f"❌ An error occurred during database operations: {e}")
    finally:
        if con:
            con.close()
            print("🦆 DuckDB connection closed.")
            
    print("\n--- Historical Data Load Finished ---")

if __name__ == "__main__":
    main() 