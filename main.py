from data_processor import BulkDataDownloader

def main():
    """
    Main function to download cryptocurrency data and upload it to Supabase.
    """
    # Configuration for the data download
    config = {
        "market": "spot",
        "timeframe": "daily",
        "data_type": "klines",
        "symbol": "BTCUSDT",
        "interval": "1m"
    }

    try:
        # Initialize the data downloader with the specified configuration
        downloader = BulkDataDownloader(**config)

        # Get the data as a Polars DataFrame, upload it to Supabase,
        # and limit to the first 2 files for a quick test.
        print("Starting data download and upload process...")
        df = downloader.get_polars_df(nfiles=2, upload=True, table_name="klines")

        print("\nProcess completed successfully!")
        print(f"Downloaded and uploaded {df.height} records.")
        print("\nSample of the downloaded data:")
        print(df.head())

    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main() 