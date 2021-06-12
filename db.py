import sqlite3
import pandas as pd

# Database Factory Class
class Db():
    
    def __init__(self):
        self.market_average_growth = 0
        self.database = "indian_stock_data.db"
        self.databse_connect()
        self.databse_init()
        
    def databse_connect(self):
        """Connect to the SQLite3 database."""
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()
    
    def databse_init(self):
        """Initialise the SQLite3 database."""
        self.cursor.execute("CREATE TABLE IF NOT EXISTS company (\
                            symbol TEXT UNIQUE NOT NULL, name TEXT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS stock_prices (\
                            symbol TEXT NOT NULL references company(symbol) ON DELETE CASCADE, \
                            date TEXT, \
                            open REAL, high REAL, low REAL, close REAL, adj_close REAL, volume REAL, \
                            CONSTRAINT unq UNIQUE (symbol, date))")
    
    def find_companies_list(self):
        self.cursor.execute("SELECT symbol FROM company")         
        companies = [comp[0] for comp in self.cursor.fetchall()]
        return companies
    
    def insert_company_data(self, symbol, name):
        """Insert the company data to db"""
        try:
            self.cursor.execute("INSERT INTO company (symbol, name) \
                                VALUES (:symbol, :name)", \
                                {"symbol": symbol, "name": name})
            self.connection.commit()
        except sqlite3.IntegrityError as e:
            pass
    
    def insert_stock_price_data(self, symbol, date, open_, high, low, close, adj_close, volume):
        """Insert the stock data to db"""
        try:
            self.cursor.execute("INSERT INTO stock_prices (symbol, date, open, high, low, close, adj_close, volume) \
                                VALUES (:symbol, :date, :open, :high, :low, :close, :adj_close, :volume)", \
                                {"symbol": symbol, "date": date, "open": open_, "high": high, "low": low, "close": close, "adj_close": adj_close, "volume": volume})
            self.connection.commit()
        except sqlite3.IntegrityError as e:
            pass
    
    def get_stock_price_data_frame(self, start_date):
        """ Get the compleate stock data and return it as a data frame"""
        query_string = "SELECT symbol, date, adj_close from stock_prices where date > " + start_date
        print(query_string)
        return pd.read_sql_query(query_string, self.connection)
    
    def close(self):
        self.connection.close()