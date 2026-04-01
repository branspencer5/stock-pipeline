import yfinance as yf
import pandas as pd
import sqlalchemy as sa
import requests
import sys
import os
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv('/Users/spencer/Desktop/python-projects/stock-pipeline/.env')

# -----------------------------------------------
# STEP 1: Allow period to be passed from command line
# Default to 1y if not provided
# -----------------------------------------------
PERIOD = sys.argv[1] if len(sys.argv) > 1 else "1y"

# -----------------------------------------------
# STEP 2: Set up the local SQLite database
# -----------------------------------------------
def setup_database():
    # Connect to Supabase cloud database
    engine = sa.create_engine(os.getenv("DATABASE_URL"))

    with engine.connect() as conn:
        # Table for daily price history
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                fetched_at TEXT
            )
        """))

        # Table for S&P 500 fundamentals
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS stock_fundamentals (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                company_name TEXT,
                sector TEXT,
                all_time_high REAL,
                current_price REAL,
                pct_from_ath REAL,
                week_52_high REAL,
                week_52_low REAL,
                quarterly_earnings TEXT,
                is_profitable INTEGER,
                earnings_trend TEXT,
                debt_to_equity REAL,
                analyst_rating TEXT,
                fetched_at TEXT
            )
        """))

        conn.commit()

    print("✅ Database ready!\n")
    return engine

# -----------------------------------------------
# STEP 3: Fetch all S&P 500 tickers from Wikipedia
# -----------------------------------------------
def get_sp500_tickers():
    print("📋 Fetching S&P 500 tickers from Wikipedia...")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    # Add a browser header so Wikipedia doesn't block us
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    # Fetch the page using requests
    response = requests.get(url, headers=headers)
    
    # Parse the HTML table with pandas
    tables = pd.read_html(response.text)
    df = tables[0]

    # Extract ticker and sector columns
    tickers = df[['Symbol', 'GICS Sector', 'Security']].copy()
    tickers.columns = ['ticker', 'sector', 'company_name']

    # Some tickers have dots which Yahoo Finance uses as dashes
    tickers['ticker'] = tickers['ticker'].str.replace('.', '-', regex=False)

    print(f"✅ Found {len(tickers)} S&P 500 companies\n")
    return tickers

# -----------------------------------------------
# STEP 4: Fetch stock price history
# -----------------------------------------------
def fetch_stock_data(ticker, period="1y"):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)

        if df.empty:
            return None

        # Reset index so Date becomes a column
        df = df.reset_index()
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

        # Strip timezone info and format date
        df['date'] = df['date'].dt.tz_localize(None).dt.strftime('%Y-%m-%d')

        # Round price columns
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].round(2)

        df['volume'] = df['volume'].astype(int)
        df['ticker'] = ticker
        df['fetched_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return df

    except Exception as e:
        print(f"⚠️  Error fetching price data for {ticker}: {e}")
        return None

# -----------------------------------------------
# STEP 5: Fetch fundamentals for a stock
# This includes ATH, earnings, debt, analyst rating
# -----------------------------------------------
def fetch_fundamentals(ticker, sector, company_name):
    try:
        stock = yf.Ticker(ticker)

        # Get max history to find all time high
        history = stock.history(period="max")
        if history.empty:
            return None

        # Calculate all time high and % below it
        all_time_high = round(history['Close'].max(), 2)
        current_price = round(history['Close'].iloc[-1], 2)
        pct_from_ath = round(((current_price - all_time_high) / all_time_high) * 100, 2)

        # Get quarterly earnings
        earnings = stock.quarterly_financials
        quarterly_earnings = []
        is_profitable = 0
        earnings_trend = "N/A"

        if earnings is not None and not earnings.empty:
            # Look for net income row
            if 'Net Income' in earnings.index:
                net_income = earnings.loc['Net Income'].dropna()

                # Get last 4 quarters
                last_4 = net_income.head(4).tolist()
                quarterly_earnings = [round(x / 1e6, 2) for x in last_4]  # Convert to millions

                # Check if most recent quarter is profitable
                is_profitable = 1 if last_4[0] > 0 else 0

                # Determine earnings trend
                if len(last_4) >= 2:
                    if last_4[0] > last_4[1]:
                        earnings_trend = "Growing"
                    elif last_4[0] < last_4[1]:
                        earnings_trend = "Shrinking"
                    else:
                        earnings_trend = "Flat"

        # Get 52 week high and low
        info = stock.info
        week_52_high = info.get('fiftyTwoWeekHigh', None)
        week_52_low = info.get('fiftyTwoWeekLow', None)

        # Get debt to equity ratio
        debt_to_equity = info.get('debtToEquity', None)
        if debt_to_equity:
            debt_to_equity = round(debt_to_equity, 2)

        # Get analyst recommendation
        analyst_rating = info.get('recommendationKey', 'N/A').title()

        return {
            'ticker': ticker,
            'company_name': company_name,
            'sector': sector,
            'all_time_high': all_time_high,
            'current_price': current_price,
            'pct_from_ath': pct_from_ath,
            'quarterly_earnings': str(quarterly_earnings),
            'is_profitable': is_profitable,
            'earnings_trend': earnings_trend,
            'debt_to_equity': debt_to_equity,
            'analyst_rating': analyst_rating,
            'week_52_high': week_52_high,
            'week_52_low': week_52_low,
            'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    except Exception as e:
        print(f"⚠️  Error fetching fundamentals for {ticker}: {e}")
        return None

# -----------------------------------------------
# STEP 6: Store price data in SQLite
# -----------------------------------------------
def store_price_data(engine, ticker, df):
    with engine.connect() as conn:
        conn.execute(sa.text(f"DELETE FROM stock_prices WHERE ticker = '{ticker}'"))
        conn.commit()
    df.to_sql('stock_prices', engine, if_exists='append', index=False)

# -----------------------------------------------
# STEP 7: Store fundamentals in SQLite
# -----------------------------------------------
def store_fundamentals(engine, data):
    with engine.connect() as conn:
        conn.execute(sa.text(f"DELETE FROM stock_fundamentals WHERE ticker = '{data['ticker']}'"))
        conn.commit()
    pd.DataFrame([data]).to_sql('stock_fundamentals', engine, if_exists='append', index=False)

# -----------------------------------------------
# MAIN: Run the full pipeline
# -----------------------------------------------
def main():
    print("🚀 Starting S&P 500 pipeline...\n")

    # Set up the database
    engine = setup_database()

    # Get all S&P 500 tickers
    sp500 = get_sp500_tickers()

        # -----------------------------------------------
    # Process each stock using multithreading
    # This runs multiple stocks at the same time
    # instead of one at a time — much faster!
    # -----------------------------------------------
    def process_stock(row):
        import time
        time.sleep(0.5)  # Small delay to avoid rate limits
        
        ticker = row['ticker']
        sector = row['sector']
        company_name = row['company_name']

        try:
            # Fetch and store price history
            price_df = fetch_stock_data(ticker, period=PERIOD)
            if price_df is not None:
                store_price_data(engine, ticker, price_df)

            # Fetch and store fundamentals
            fundamentals = fetch_fundamentals(ticker, sector, company_name)
            if fundamentals is not None:
                store_fundamentals(engine, fundamentals)

            return f"✅ Done: {ticker}"
        except Exception as e:
            return f"⚠️ Error: {ticker} — {e}"

    # Run up to 10 stocks at the same time
    total = len(sp500)
    completed = 0

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all stocks to the thread pool
        futures = {executor.submit(process_stock, row): row['ticker'] for _, row in sp500.iterrows()}

        # Print results as each stock finishes
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            print(f"[{completed}/{total}] {result}")

    print("\n🎉 S&P 500 pipeline complete!")
# Run the pipeline
if __name__ == "__main__":
    main()