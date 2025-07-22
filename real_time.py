import asyncio
import json
import websockets 
import polars as pl
from datetime import datetime, timedelta
import requests
from pathlib import Path
from data_processor import BulkDataDownloader

class BinanceRealtimeData:
    def __init__(self, symbol="BTCUSDT", interval="1m"):
        self.symbol = symbol.lower()  # WebSocket uses lowercase
        self.interval = interval
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{interval}"
        self.rest_api_url = "https://api.binance.com/api/v3"
        self.historical_downloader = BulkDataDownloader(
            symbol=symbol.upper(), 
            interval=interval,
            timeframe="daily"
        )
        
    def get_today_start_timestamp(self):
        """Get today's start timestamp in milliseconds"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        return int(today.timestamp() * 1000)
    
    def get_historical_today_data(self):
        """Get today's historical data from Binance REST API"""
        start_time = self.get_today_start_timestamp()
        
        params = {
            'symbol': self.symbol.upper(),
            'interval': self.interval,
            'startTime': start_time,
            'limit': 1000  # Max limit
        }
        
        response = requests.get(f"{self.rest_api_url}/klines", params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Convert to Polars DataFrame
        if data:
            df = pl.DataFrame(data, schema=[
                ("open_time", pl.Int64),
                ("open", pl.Utf8),
                ("high", pl.Utf8), 
                ("low", pl.Utf8),
                ("close", pl.Utf8),
                ("volume", pl.Utf8),
                ("close_time", pl.Int64),
                ("quote_asset_volume", pl.Utf8),
                ("number_of_trades", pl.Int64),
                ("taker_buy_base_volume", pl.Utf8),
                ("taker_buy_quote_volume", pl.Utf8),
                ("ignore", pl.Utf8)
            ])
            
            # Convert string columns to float
            df = df.with_columns([
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("quote_asset_volume").cast(pl.Float64),
                pl.col("taker_buy_base_volume").cast(pl.Float64),
                pl.col("taker_buy_quote_volume").cast(pl.Float64)
            ])
            
            # Fixed: Use consistent datetime precision  
            df = df.with_columns([
                pl.col("open_time").cast(pl.Datetime("ms")).alias("open_datetime"),
                pl.col("close_time").cast(pl.Datetime("ms")).alias("close_datetime")
            ])
            
            print(f"✅ Loaded {df.height} historical candles for today")
            return df
        else:
            return pl.DataFrame()
    
    def get_complete_today_data(self):
        """Get complete data: yesterday's last candle + today's data"""
        # Get today's data
        today_df = self.get_historical_today_data()
        
        # Get yesterday's last few candles for context
        try:
            yesterday_df = self.historical_downloader.get_polars_df(nfiles=1)
            if not yesterday_df.is_empty():
                # Get last 10 candles from yesterday
                yesterday_last = yesterday_df.tail(10)
                complete_df = pl.concat([yesterday_last, today_df])
                print(f"✅ Combined data: {yesterday_last.height} from yesterday + {today_df.height} from today")
                return complete_df
        except Exception as e:
            print(f"⚠️ Could not get yesterday's data: {e}")
            return today_df
        
        return today_df
    
    async def stream_live_data(self, callback=None):
        """Stream live kline data via WebSocket"""
        print(f"🔴 Starting WebSocket stream for {self.symbol.upper()} {self.interval}")
        
        async with websockets.connect(self.ws_url) as websocket:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    kline = data['k']
                    
                    # Extract kline data - keep timestamps as integers
                    candle_data = {
                        'open_time': kline['t'],
                        'open': float(kline['o']),
                        'high': float(kline['h']),
                        'low': float(kline['l']),
                        'close': float(kline['c']),
                        'volume': float(kline['v']),
                        'close_time': kline['T'],
                        'quote_asset_volume': float(kline['q']),
                        'number_of_trades': kline['n'],
                        'taker_buy_base_volume': float(kline['V']),
                        'taker_buy_quote_volume': float(kline['Q']),
                        'is_closed': kline['x'],  # True if this kline is closed
                        # For display purposes only
                        'open_datetime': datetime.fromtimestamp(kline['t'] / 1000),
                        'close_datetime': datetime.fromtimestamp(kline['T'] / 1000)
                    }
                    
                    if callback:
                        await callback(candle_data)
                    else:
                        self.print_candle_update(candle_data)
                        
                except Exception as e:
                    print(f"❌ Error processing WebSocket message: {e}")
    
    def print_candle_update(self, candle_data):
        """Print formatted candle update"""
        status = "🟢 CLOSED" if candle_data['is_closed'] else "🟡 LIVE"
        timestamp = candle_data['open_datetime'].strftime("%H:%M:%S")
        
        print(f"{status} [{timestamp}] {self.symbol.upper()} | "
              f"O: ${candle_data['open']:.2f} | "
              f"H: ${candle_data['high']:.2f} | "
              f"L: ${candle_data['low']:.2f} | "
              f"C: ${candle_data['close']:.2f} | "
              f"V: {candle_data['volume']:.2f}")

class ContinuousDataStreamer:
    def __init__(self, symbol="BTCUSDT", interval="1m", save_interval_minutes=5):
        self.symbol = symbol
        self.interval = interval
        self.save_interval_minutes = save_interval_minutes
        self.realtime = BinanceRealtimeData(symbol, interval)
        self.combined_df = None
        self.live_candles = []
        self.last_save_time = datetime.now()
        
    async def initialize_data(self):
        """Load historical data and prepare for streaming"""
        print("🔄 Initializing combined data...")
        
        # Get complete today's data (includes yesterday's context)
        self.combined_df = self.realtime.get_complete_today_data()
        
        if self.combined_df is not None and not self.combined_df.is_empty():
            print(f"✅ Initialized with {self.combined_df.height} historical candles")
            print(f"📅 Date range: {self.combined_df['open_datetime'].min()} to {self.combined_df['open_datetime'].max()}")
        else:
            print("⚠️ No historical data loaded, starting with empty dataset")
            self.combined_df = pl.DataFrame()
    
    async def handle_live_candle(self, candle_data):
        """Process incoming live candle data"""
        # Add to live candles buffer
        self.live_candles.append(candle_data)
        
        # Print live update
        status = "🟢 CLOSED" if candle_data['is_closed'] else "🟡 LIVE"
        timestamp = candle_data['open_datetime'].strftime("%H:%M:%S")
        price_change = ""
        
        if len(self.live_candles) > 1:
            prev_close = self.live_candles[-2]['close']
            current_close = candle_data['close']
            change = current_close - prev_close
            change_pct = (change / prev_close) * 100
            price_change = f"📈 +{change:.2f} (+{change_pct:.2f}%)" if change > 0 else f"📉 {change:.2f} ({change_pct:.2f}%)"
        
        print(f"{status} [{timestamp}] {self.symbol.upper()} | "
              f"Price: ${candle_data['close']:.2f} | "
              f"Volume: {candle_data['volume']:.2f} {price_change}")
        
        # If candle is closed, integrate it into main dataset
        if candle_data['is_closed']:
            await self.integrate_closed_candle(candle_data)
        
        # Auto-save periodically
        await self.auto_save_check()
    
    async def integrate_closed_candle(self, candle_data):
        """Integrate a closed candle into the main dataset"""
        print(f"💾 Integrating closed candle: {candle_data['close_datetime']}")
        
        # Create DataFrame with integer timestamps (matching existing schema)
        new_row = pl.DataFrame([{
            'open_time': int(candle_data['open_time']),
            'open': candle_data['open'],
            'high': candle_data['high'],
            'low': candle_data['low'],
            'close': candle_data['close'],
            'volume': candle_data['volume'],
            'close_time': int(candle_data['close_time']),
            'quote_asset_volume': candle_data['quote_asset_volume'],
            'number_of_trades': candle_data['number_of_trades'],
            'taker_buy_base_volume': candle_data['taker_buy_base_volume'],
            'taker_buy_quote_volume': candle_data['taker_buy_quote_volume'],
            'ignore': ""
        }])
        
        # Convert timestamps to datetime with millisecond precision (matching existing schema)
        new_row = new_row.with_columns([
            pl.col("open_time").cast(pl.Datetime("ms")).alias("open_datetime"),
            pl.col("close_time").cast(pl.Datetime("ms")).alias("close_datetime")
        ])
        
        # Add to main dataset
        if self.combined_df.is_empty():
            self.combined_df = new_row
        else:
            self.combined_df = pl.concat([self.combined_df, new_row])
        
        print(f"📊 Total candles in dataset: {self.combined_df.height}")

    async def auto_save_check(self):
        """Auto-save data periodically"""
        now = datetime.now()
        if (now - self.last_save_time).total_seconds() >= (self.save_interval_minutes * 60):
            await self.save_data()
            self.last_save_time = now
    
    async def save_data(self, filepath=None):
        """Save combined data to CSV"""
        if filepath is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"data/continuous_{self.symbol}_{self.interval}_{timestamp}.csv"
        
        try:
            # Create directory if needed
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            # Save combined dataset
            if not self.combined_df.is_empty():
                self.combined_df.write_csv(filepath)
                print(f"💾 Saved {self.combined_df.height} candles to {filepath}")
            else:
                print("⚠️ No data to save")
                
        except Exception as e:
            print(f"❌ Error saving data: {e}")
    
    async def run_continuous_stream(self):
        """Run the continuous data streaming"""
        print(f"🚀 Starting continuous data stream for {self.symbol.upper()} {self.interval}")
        
        # Initialize historical data
        await self.initialize_data()
        
        # Start live streaming
        print("🔴 Starting live WebSocket stream...")
        try:
            await self.realtime.stream_live_data(self.handle_live_candle)
        except KeyboardInterrupt:
            print("\n⏹️  Stopping stream...")
            await self.save_data()
            print("✅ Data saved before exit")
        except Exception as e:
            print(f"❌ Stream error: {e}")
            await self.save_data()

class BinanceDataManager:
    def __init__(self, symbol="BTCUSDT", interval="1m"):
        self.realtime = BinanceRealtimeData(symbol, interval)
        self.historical_df = None
        self.live_candles = []
        
    async def initialize(self):
        """Initialize with historical data"""
        print("📊 Loading historical data...")
        self.historical_df = self.realtime.get_complete_today_data()
        print(f"✅ Loaded {self.historical_df.height if self.historical_df else 0} historical candles")
        
    async def handle_live_candle(self, candle_data):
        """Handle incoming live candle data"""
        # Store live candle
        self.live_candles.append(candle_data)
        
        # If candle is closed, you might want to append it to historical data
        if candle_data['is_closed']:
            print(f"💾 Candle closed at {candle_data['close_datetime']}")
            # Here you could append to your DataFrame or save to database
            
        # Print update
        status = "🟢 CLOSED" if candle_data['is_closed'] else "🟡 LIVE"
        timestamp = candle_data['open_datetime'].strftime("%H:%M:%S")
        
        print(f"{status} [{timestamp}] | "
              f"Price: ${candle_data['close']:.2f} | "
              f"Volume: {candle_data['volume']:.2f}")
    
    async def run(self):
        """Run the complete data manager"""
        await self.initialize()
        
        print("🚀 Starting live data stream...")
        await self.realtime.stream_live_data(self.handle_live_candle)
    
    def get_combined_dataframe(self):
        """Get combined historical + live data as DataFrame"""
        if not self.historical_df or self.historical_df.is_empty():
            return pl.DataFrame()
            
        # Convert live candles to DataFrame if any
        if self.live_candles:
            live_df = pl.DataFrame(self.live_candles)
            return pl.concat([self.historical_df, live_df])
        
        return self.historical_df
    
    def export_today_data(self, filepath="btc_today_data.csv"):
        """Export all today's data to CSV"""
        df = self.get_combined_dataframe()
        if not df.is_empty():
            df.write_csv(filepath)
            print(f"💾 Exported {df.height} rows to {filepath}")
        else:
            print("⚠️ No data to export")

# Fixed usage examples
async def main():
    """Continuous streaming example with the correct class"""
    streamer = ContinuousDataStreamer("BTCUSDT", "1m", save_interval_minutes=5)
    await streamer.run_continuous_stream()

async def simple_example():
    """Simple live streaming example"""
    realtime = BinanceRealtimeData("BTCUSDT", "1m")
    
    # Get today's historical data
    today_data = realtime.get_complete_today_data()
    print(f"Historical data shape: {today_data.shape}")
    
    # Start live streaming (this will run forever)
    await realtime.stream_live_data()

async def advanced_example():
    """Complete data management example"""
    manager = BinanceDataManager("BTCUSDT", "1m")
    
    # This will load historical data and start live streaming
    await manager.run()

if __name__ == "__main__":
    # Run continuous stream (recommended)
    asyncio.run(main())
    
    # Or run simple example
    # asyncio.run(simple_example())
    
    # Or run advanced example
    # asyncio.run(advanced_example())