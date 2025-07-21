import io
from zipfile import ZipFile
import polars as pl
import requests
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.text import Text

class BulkDataDownloader:
    def __init__(self, market = "spot", timeframe = "daily", data_type="klines", symbol="BTCUSDT", interval = "1m"):
        self.BINANCE_DATA_DOWNLOAD_BASE_URL = "https://data.binance.vision"
        self.BINANCE_DATA_S3_BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?prefix="
        self.MAX_DISPLAY_FILES = 5
        self.market = market
        self.timeframe = timeframe
        self.data_type = data_type
        self.symbol = symbol
        self.interval = interval

        self.KLINES_SCHEMA = {
        "open_time": pl.Int64,              # Open time (timestamp in milliseconds)
        "open": pl.Float64,                 # Open price
        "high": pl.Float64,                 # High price
        "low": pl.Float64,                  # Low price
        "close": pl.Float64,                # Close price
        "volume": pl.Float64,               # Volume
        "close_time": pl.Int64,             # Close time (timestamp in milliseconds)
        "quote_asset_volume": pl.Float64,   # Quote asset volume
        "number_of_trades": pl.Int64,       # Number of trades
        "taker_buy_base_volume": pl.Float64, # Taker buy base asset volume
        "taker_buy_quote_volume": pl.Float64, # Taker buy quote asset volume
        "ignore": pl.Utf8                   # Unused field (kept as string)
    }

        # Column names in order
        self.KLINES_COLUMNS = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ]

    def get_file_list_from_s3_bucket(self, prefix):
        filenames = []
        continuation_token = None

        with Live(refresh_per_second=4) as live:
            while True:
                # Use list-objects-v2 API for better pagination
                url = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
                params = {
                    'list-type': '2',
                    'prefix': prefix,
                    'max-keys': '1000'  # Maximum allowed
                }
                
                if continuation_token:
                    params['continuation-token'] = continuation_token

                response = requests.get(url, params=params)
                response.raise_for_status()

                tree = ET.fromstring(response.content)

                # Process current page
                for content in tree.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
                    key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
                    if key.endswith(".zip"):
                        filenames.append(key)

                # Update display
                status_text = Text(f"Getting file list: {prefix}\nTotal files: {len(filenames)}")
                if filenames:
                    # Show ACTUAL latest files (chronologically last)
                    latest_files = sorted(filenames)[-self.MAX_DISPLAY_FILES:]
                    status_text.append("\n\nLatest files (chronologically):")
                    for recent_file in latest_files:
                        status_text.append(f"\n{recent_file}")
                live.update(Panel(status_text, style="blue"))

                # Check for more pages
                is_truncated = tree.find("{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated")
                if is_truncated is not None and is_truncated.text.lower() == "true":
                    next_token = tree.find("{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken")
                    if next_token is not None:
                        continuation_token = next_token.text
                    else:
                        break
                else:
                    break

            # Sort filenames to get actual latest files
            filenames.sort()
            
            status_text.plain = f"File list complete: {prefix}\nTotal files: {len(filenames)}"
            if filenames:
                status_text.append(f"\nDate range: {filenames[0].split('/')[-1]} to {filenames[-1].split('/')[-1]}")
                status_text.append("\n\nLatest files:")
                for recent_file in filenames[-self.MAX_DISPLAY_FILES:]:
                    status_text.append(f"\n{recent_file}")
            live.update(Panel(status_text, style="green"))
            
            return filenames
    
    def get_prefix(self):
        prefix = "data/" + self.market + "/" + self.timeframe +  "/" + self.data_type + "/" + self.symbol + "/" + self.interval + "/"

        return prefix

    def create_download_urls(self, filenames):
        return [f"{self.BINANCE_DATA_DOWNLOAD_BASE_URL}/{filename}" for filename in filenames]

    def download_file(self, url):
        response = requests.get(url)
        response.raise_for_status()

        zip_data = io.BytesIO(response.content)

        csv_content = None

        with ZipFile(zip_data) as z:
            for file_name in z.namelist():
                if(file_name.endswith(".csv")):
                    with z.open(file_name) as f:
                        csv_content = f.read()
                        break
        
        return csv_content
    
    def export_csv(self, df, path, create_dir = False, overwrite = False):
        try:
            file_path = Path(path)

            if df.is_empty():
                raise ValueError("Cannot export empty DataFrame to CSV")
            parent_dir = file_path.parent
            if not parent_dir.exists():
                if create_dir:
                    print(f"Creating directory: {parent_dir}")
                    parent_dir.mkdir(parents=True, exist_ok=True)
                else:
                    raise FileNotFoundError(f"Directory does not exist: {parent_dir}")
            
            if file_path.exists() and not overwrite:
                raise FileExistsError(f"File already exists: {file_path}. Use overwrite=True to replace it.")
            
            df.write_csv(file_path, separator=',')

        except FileNotFoundError as e:
            print(f"Path Error: {e}")
            raise
            
        except FileExistsError as e:
            print(f"File Exists Error: {e}")
            raise
            
        except PermissionError as e:
            print(f"Permission Error: Cannot write to {path}. Check file permissions.")
            raise
            
        except ValueError as e:
            print(f"Data Error: {e}")
            raise
            
        except Exception as e:
            print(f"Unexpected error while exporting CSV: {e}")
            raise
    
    def get_polars_df(self, nfiles = None):
        prefix = self.get_prefix()
        filenames = self.get_file_list_from_s3_bucket(prefix)
        download_urls = self.create_download_urls(filenames)

        # Read first file with schema
        csv_content = self.download_file(download_urls[0])
        df = pl.read_csv(
            io.StringIO(csv_content.decode('utf-8')),
            schema=self.KLINES_SCHEMA,
            new_columns=self.KLINES_COLUMNS,
            has_header=False 
        )

        if nfiles is not None:
            download_urls = download_urls[:nfiles]

        for url in download_urls[1:]:
            csv_content = self.download_file(url)
            temp_df = pl.read_csv(
                io.StringIO(csv_content.decode('utf-8')),
                schema=self.KLINES_SCHEMA,
                new_columns=self.KLINES_COLUMNS,
                has_header=False
            )
            df = pl.concat([df, temp_df])
        
        # Convert timestamps to datetime
        df = df.with_columns([
            pl.col("open_time").map_elements(lambda x: datetime.fromtimestamp(x/1000)).alias("open_datetime"),
            pl.col("close_time").map_elements(lambda x: datetime.fromtimestamp(x/1000)).alias("close_datetime")
        ])

        return df