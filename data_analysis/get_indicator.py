import ta
import duckdb
import pandas as pd

class Indicators:
    def __init__(self):
        DUCKDB_CONNECTION = duckdb.connect(database='my_database.duckdb')
        OHCL_DATA = DUCKDB_CONNECTION.sql("SELECT open, high, close, low FROM table LIMIT 5000").df() #edit the "table" and LIMIT when deployed

    def close_duckdb_database(self):
        self.DUCKDB_CONNECTION.close()
        print("DuckDB connection closed")

    def get_average_directional_movement_index(self):
        return ta.trend.ADXIndicator(open=self.OHCL_DATA['open'], high=self.OHCL_DATA['high'], close=self.OHCL_DATA['close'], low = self.OHCL_DATA['low'])
