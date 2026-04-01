import yfinance as yf
import pandas as pd
import sqlalchemy as sa
import requests
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

# -----------------------------------------------
# STEP 1: Database path
# -----------------------------------------------

# Load environment variables
load_dotenv('/Users/spencer/Desktop/python-projects/stock-pipeline/.env')

DB_PATH = os.getenv("DATABASE_URL")

# -----------------------------------------------
# STEP 2: Set up the short signals table
# -----------------------------------------------
def setup_database():
    engine = sa.create_engine(DB_PATH)

    with engine.connect() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS short_signals (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                company_name TEXT,
                sector TEXT,
                current_price REAL,
                rsi REAL,
                pe_ratio REAL,
                sector_pe REAL,
                pe_vs_sector REAL,
                free_cash_flow REAL,
                earnings_growth REAL,
                short_interest REAL,
                days_to_cover REAL,
                insider_selling INTEGER,
                analyst_rating TEXT,
                debt_to_equity REAL,
                risk_score REAL,
                fetched_at TEXT
            )
        """))
        conn.commit()

    print("✅ Short signals table ready!\n")
    return engine

# -----------------------------------------------
# STEP 3: Fetch all S&P 500 tickers from Wikipedia
# -----------------------------------------------
def get_sp500_tickers():
    print("📋 Fetching S&P 500 tickers from Wikipedia...")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers)
    tables = pd.read_html(response.text)
    df = tables[0]

    tickers = df[['Symbol', 'GICS Sector', 'Security']].copy()
    tickers.columns = ['ticker', 'sector', 'company_name']
    tickers['ticker'] = tickers['ticker'].str.replace('.', '-', regex=False)

    print(f"✅ Found {len(tickers)} S&P 500 companies\n")
    return tickers

# -----------------------------------------------
# STEP 4: Calculate RSI from price history
# RSI measures if a stock is overbought (>70)
# or oversold (<30)
# -----------------------------------------------
def calculate_rsi(prices, period=14):
    # Calculate price changes
    delta = prices.diff()

    # Separate gains and losses
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)

    # Calculate average gains and losses
    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()

    # Calculate RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # Return the most recent RSI value
    return round(rsi.iloc[-1], 2)

# -----------------------------------------------
# STEP 5: Calculate risk score (0-100)
# Higher score = stronger short opportunity
# -----------------------------------------------
def calculate_risk_score(rsi, pe_vs_sector, free_cash_flow,
                          earnings_growth, short_interest,
                          days_to_cover, insider_selling,
                          analyst_rating, debt_to_equity):
    score = 0

    # --- Bearish signals (add points) ---

    # RSI overbought above 70
    if rsi and rsi > 70:
        score += 10

    # High P/E vs sector
    if pe_vs_sector and pe_vs_sector > 20:
        score += 10

    # Negative free cash flow
    if free_cash_flow and free_cash_flow < 0:
        score += 10

    # Slowing or negative earnings growth
    if earnings_growth and earnings_growth < 0:
        score += 10

    # Insider selling detected
    if insider_selling:
        score += 15

    # High debt relative to equity
    if debt_to_equity and debt_to_equity > 100:
        score += 10

    # --- Squeeze risk (subtract points) ---

    # High short interest — crowded trade
    if short_interest and short_interest > 20:
        score -= 20

    # High days to cover — squeeze danger
    if days_to_cover and days_to_cover > 5:
        score -= 15

    # Analyst buy rating fights the short thesis
    if analyst_rating and analyst_rating.lower() in ['buy', 'strong buy']:
        score -= 10

    # Low debt — company less vulnerable
    if debt_to_equity and debt_to_equity < 30:
        score -= 5

    # Keep score between 0 and 100
    score = max(0, min(100, score))

    return score

# -----------------------------------------------
# STEP 6: Fetch short signals for a single stock
# -----------------------------------------------
def fetch_short_signals(ticker, sector, company_name):
    try:
        time.sleep(0.5)  # Avoid rate limiting

        stock = yf.Ticker(ticker)
        info = stock.info

        # Get current price
        current_price = info.get('currentPrice', None)

        # Get price history for RSI calculation
        history = stock.history(period="3mo")
        rsi = None
        if not history.empty:
            rsi = calculate_rsi(history['Close'])

        # Get P/E ratio
        pe_ratio = info.get('trailingPE', None)

        # Get sector average P/E (approximated)
        sector_pe_map = {
            'Information Technology': 28,
            'Health Care': 22,
            'Financials': 14,
            'Consumer Discretionary': 25,
            'Consumer Staples': 20,
            'Industrials': 20,
            'Energy': 12,
            'Materials': 16,
            'Real Estate': 30,
            'Utilities': 18,
            'Communication Services': 20
        }
        sector_pe = sector_pe_map.get(sector, 20)

        # Calculate how much higher P/E is vs sector average
        pe_vs_sector = None
        if pe_ratio and sector_pe:
            pe_vs_sector = round(pe_ratio - sector_pe, 2)

        # Get free cash flow
        free_cash_flow = info.get('freeCashflow', None)
        if free_cash_flow:
            free_cash_flow = round(free_cash_flow / 1e6, 2)  # Convert to millions

        # Get earnings growth
        earnings_growth = info.get('earningsGrowth', None)
        if earnings_growth:
            earnings_growth = round(earnings_growth * 100, 2)  # Convert to percentage

        # Get short interest as % of float
        short_interest = info.get('shortPercentOfFloat', None)
        if short_interest:
            short_interest = round(short_interest * 100, 2)

        # Get days to cover
        days_to_cover = info.get('shortRatio', None)

        # Check for insider selling using insider purchases data
        insider_selling = 0
        try:
            insider_purchases = stock.insider_purchases
            if insider_purchases is not None and not insider_purchases.empty:
                # Yahoo Finance returns a summary table with Purchases and Sales rows
                if 'Purchases' in insider_purchases.index and 'Sales' in insider_purchases.index:
                    purchases = insider_purchases.loc['Purchases', 'Shares']
                    sales = insider_purchases.loc['Sales', 'Shares']
                    if sales > purchases:
                        insider_selling = 1
        except:
            # Fall back to checking insider transactions
            try:
                transactions = stock.insider_transactions
                if transactions is not None and not transactions.empty:
                    recent = transactions.head(20)
                    sells = recent[recent.get('Text', pd.Series()).str.contains('Sale|Sold', na=False)]
                    buys = recent[recent.get('Text', pd.Series()).str.contains('Purchase|Bought', na=False)]
                    if len(sells) > len(buys):
                        insider_selling = 1
            except:
                insider_selling = 0

        # Get analyst rating
        analyst_rating = info.get('recommendationKey', 'N/A').title()

        # Get debt to equity
        debt_to_equity = info.get('debtToEquity', None)

        # Calculate risk score
        risk_score = calculate_risk_score(
            rsi, pe_vs_sector, free_cash_flow,
            earnings_growth, short_interest,
            days_to_cover, insider_selling,
            analyst_rating, debt_to_equity
        )

        return {
            'ticker': ticker,
            'company_name': company_name,
            'sector': sector,
            'current_price': current_price,
            'rsi': rsi,
            'pe_ratio': pe_ratio,
            'sector_pe': sector_pe,
            'pe_vs_sector': pe_vs_sector,
            'free_cash_flow': free_cash_flow,
            'earnings_growth': earnings_growth,
            'short_interest': short_interest,
            'days_to_cover': days_to_cover,
            'insider_selling': insider_selling,
            'analyst_rating': analyst_rating,
            'debt_to_equity': debt_to_equity,
            'risk_score': risk_score,
            'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    except Exception as e:
        print(f"⚠️ Error fetching signals for {ticker}: {e}")
        return None

# -----------------------------------------------
# STEP 7: Store short signals in SQLite
# -----------------------------------------------
def store_signals(engine, data):
    with engine.connect() as conn:
        conn.execute(sa.text(f"DELETE FROM short_signals WHERE ticker = '{data['ticker']}'"))
        conn.commit()
    pd.DataFrame([data]).to_sql('short_signals', engine, if_exists='append', index=False)

# -----------------------------------------------
# MAIN: Run the short signals pipeline
# -----------------------------------------------
def main():
    print("🚀 Starting Short Scanner Pipeline...\n")

    # Set up the database
    engine = setup_database()

    # Get all S&P 500 tickers
    sp500 = get_sp500_tickers()

    total = len(sp500)
    completed = 0

    # Process stocks using multithreading
    def process_stock(row):
        signals = fetch_short_signals(row['ticker'], row['sector'], row['company_name'])
        if signals:
            store_signals(engine, signals)
            return f"✅ Done: {row['ticker']}"
        return f"⚠️ Skipped: {row['ticker']}"

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_stock, row): row['ticker'] for _, row in sp500.iterrows()}
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            print(f"[{completed}/{total}] {result}")

    print("\n🎉 Short scanner pipeline complete!")

# Run the pipeline
if __name__ == "__main__":
    main()