import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sqlalchemy as sa
from newsapi import NewsApiClient
import os
from dotenv import load_dotenv

# -----------------------------------------------
# STEP 1: Load API keys and database connection
# -----------------------------------------------
load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
# Load environment variables
load_dotenv('/Users/spencer/Desktop/python-projects/stock-pipeline/.env')

DB_PATH = os.getenv("DATABASE_URL")

def get_engine():
    return sa.create_engine(DB_PATH)

# -----------------------------------------------
# STEP 2: Load short signals from database
# -----------------------------------------------
def load_short_signals():
    engine = get_engine()
    query = """
        SELECT *
        FROM short_signals
        ORDER BY risk_score DESC
    """
    df = pd.read_sql(query, engine)
    return df

# -----------------------------------------------
# STEP 3: Load price history for a ticker
# -----------------------------------------------
def load_price_data(ticker):
    engine = get_engine()
    query = f"""
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = '{ticker}'
        ORDER BY date ASC
    """
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

# -----------------------------------------------
# STEP 4: Fetch latest news for a ticker
# -----------------------------------------------
def fetch_news(ticker, company_name):
    try:
        newsapi = NewsApiClient(api_key=NEWS_API_KEY)

        # Search by company name only for better results
        response = newsapi.get_everything(
            q=company_name,
            language='en',
            sort_by='publishedAt',
            page_size=5
        )

        articles = response.get('articles', [])

        # If no results try with ticker symbol
        if not articles:
            response = newsapi.get_everything(
                q=ticker,
                language='en',
                sort_by='publishedAt',
                page_size=5
            )
            articles = response.get('articles', [])

        return articles
    except Exception as e:
        print(f"News error: {e}")
        return []

# -----------------------------------------------
# STEP 5: Build candlestick price chart
# -----------------------------------------------
def build_price_chart(df, ticker):
    fig = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        increasing_line_color='#4ade80',
        decreasing_line_color='#f87171',
        name=ticker
    )])
    fig.update_layout(
        title=f"{ticker} Price History",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        plot_bgcolor='#111111',
        paper_bgcolor='#111111',
        font=dict(color='#f0f0f0'),
        xaxis=dict(gridcolor='#222222'),
        yaxis=dict(gridcolor='#222222'),
        height=400
    )
    return fig

# -----------------------------------------------
# STEP 6: Build RSI gauge chart
# -----------------------------------------------
def build_rsi_gauge(rsi, ticker):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=rsi,
        title={'text': f"{ticker} RSI", 'font': {'color': '#f0f0f0'}},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': '#f0f0f0'},
            'bar': {'color': '#e8ff6b'},
            'steps': [
                {'range': [0, 30], 'color': '#4ade80'},   # Oversold - green
                {'range': [30, 70], 'color': '#222222'},   # Neutral - dark
                {'range': [70, 100], 'color': '#f87171'}   # Overbought - red
            ],
            'threshold': {
                'line': {'color': '#e8ff6b', 'width': 4},
                'thickness': 0.75,
                'value': rsi
            }
        }
    ))
    fig.update_layout(
        paper_bgcolor='#111111',
        font=dict(color='#f0f0f0'),
        height=250
    )
    return fig

# -----------------------------------------------
# STEP 7: Build risk score distribution chart
# -----------------------------------------------
def build_score_distribution(df):
    fig = px.histogram(
        df,
        x='risk_score',
        nbins=20,
        title='Risk Score Distribution',
        color_discrete_sequence=['#e8ff6b']
    )
    fig.update_layout(
        plot_bgcolor='#111111',
        paper_bgcolor='#111111',
        font=dict(color='#f0f0f0'),
        xaxis=dict(gridcolor='#222222', title='Risk Score'),
        yaxis=dict(gridcolor='#222222', title='Number of Stocks'),
        height=300
    )
    return fig

# -----------------------------------------------
# PAGE CONFIG
# -----------------------------------------------
st.set_page_config(
    page_title="Short Scanner",
    page_icon="📉",
    layout="wide"
)

# Custom dark styling
st.markdown("""
    <style>
        .stApp { background-color: #0a0a0a; color: #f0f0f0; }
        [data-testid="metric-container"] {
            background: #111111;
            border: 1px solid rgba(255,255,255,0.07);
            padding: 1rem;
            border-radius: 4px;
        }
        [data-testid="stSidebar"] { background: #111111; }
        .stSelectbox label, .stSlider label { color: #f0f0f0; }
        .news-card {
            background: #111111;
            border: 1px solid rgba(255,255,255,0.07);
            padding: 1rem;
            margin-bottom: 0.75rem;
            border-radius: 4px;
        }
        .news-title { font-size: 0.85rem; color: #f0f0f0; font-weight: 500; }
        .news-meta { font-size: 0.7rem; color: rgba(240,240,240,0.4); margin-top: 0.25rem; }
        .score-legend {
            background: #111111;
            border: 1px solid rgba(255,255,255,0.07);
            padding: 1rem 1.5rem;
            border-radius: 4px;
            margin-bottom: 1.5rem;
        }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------
# HEADER
# -----------------------------------------------
st.title("🎯 S&P 500 Short Scanner")
st.markdown("*Identifies potential short opportunities based on technical, fundamental, and sentiment signals*")
st.markdown("---")

# -----------------------------------------------
# SCORE LEGEND
# -----------------------------------------------
st.markdown("""
<div class="score-legend">
    <strong>📊 Risk Score Guide</strong><br><br>
    🟢 <strong>70–100</strong> — Strong short opportunity (multiple bearish signals confirmed)<br>
    🟡 <strong>40–69</strong> — Moderate opportunity (some signals present, proceed with caution)<br>
    🔴 <strong>0–39</strong> — Weak / risky short (high squeeze risk or insufficient signals)
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------
# LOAD DATA
# -----------------------------------------------
df = load_short_signals()

# -----------------------------------------------
# SIDEBAR FILTERS
# -----------------------------------------------
with st.sidebar:
    st.header("🔍 Filters")

    # Score range filter
    min_score = st.slider(
        "Minimum Risk Score",
        min_value=0,
        max_value=100,
        value=0,
        step=5
    )

    # Sector filter
    sectors = ["All Sectors"] + sorted(df['sector'].dropna().unique().tolist())
    selected_sector = st.selectbox("Sector", sectors)

    # RSI filter
    rsi_filter = st.selectbox(
        "RSI",
        ["All", "Overbought (>70)", "Neutral (30-70)", "Oversold (<30)"]
    )

   # Short interest filter
    short_interest_filter = st.selectbox(
        "Short Interest",
        ["All", "Low (<5%)", "Moderate (5-15%)", "High (>15%)"]
    )

    # Days to cover filter
    days_to_cover_filter = st.selectbox(
        "Days to Cover",
        ["All", "Low (<3)", "Moderate (3-5)", "High (>5)"]
    )

    # Free cash flow filter
    fcf_filter = st.selectbox(
        "Free Cash Flow",
        ["All", "Negative FCF Only"]
    )

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data"):
        with st.spinner("Running short pipeline..."):
            import subprocess
            subprocess.run(["python3", "short_pipeline.py"])
        st.success("Data updated!")

# -----------------------------------------------
# APPLY FILTERS
# -----------------------------------------------
filtered = df[df['risk_score'] >= min_score].copy()

if selected_sector != "All Sectors":
    filtered = filtered[filtered['sector'] == selected_sector]

if rsi_filter == "Overbought (>70)":
    filtered = filtered[filtered['rsi'] > 70]
elif rsi_filter == "Neutral (30-70)":
    filtered = filtered[(filtered['rsi'] >= 30) & (filtered['rsi'] <= 70)]
elif rsi_filter == "Oversold (<30)":
    filtered = filtered[filtered['rsi'] < 30]

# Short interest filter
if short_interest_filter == "Low (<5%)":
    filtered = filtered[filtered['short_interest'] < 5]
elif short_interest_filter == "Moderate (5-15%)":
    filtered = filtered[(filtered['short_interest'] >= 5) & (filtered['short_interest'] <= 15)]
elif short_interest_filter == "High (>15%)":
    filtered = filtered[filtered['short_interest'] > 15]

# Days to cover filter
if days_to_cover_filter == "Low (<3)":
    filtered = filtered[filtered['days_to_cover'] < 3]
elif days_to_cover_filter == "Moderate (3-5)":
    filtered = filtered[(filtered['days_to_cover'] >= 3) & (filtered['days_to_cover'] <= 5)]
elif days_to_cover_filter == "High (>5)":
    filtered = filtered[filtered['days_to_cover'] > 5]

if fcf_filter == "Negative FCF Only":
    filtered = filtered[filtered['free_cash_flow'] < 0]

# -----------------------------------------------
# SUMMARY STATS
# -----------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Stocks Found", len(filtered))
with col2:
    overbought = len(filtered[filtered['rsi'] > 70]) if 'rsi' in filtered.columns else 0
    st.metric("Overbought RSI", overbought)
with col3:
    negative_fcf = len(filtered[filtered['free_cash_flow'] < 0]) if 'free_cash_flow' in filtered.columns else 0
    st.metric("Negative FCF", negative_fcf)
with col4:
    insider_selling = len(filtered[filtered['insider_selling'] == 1])
    st.metric("Insider Selling", insider_selling)

st.markdown("---")

# -----------------------------------------------
# SCORE DISTRIBUTION CHART
# -----------------------------------------------
st.plotly_chart(build_score_distribution(filtered), use_container_width=True)
st.markdown("---")

# -----------------------------------------------
# MAIN RESULTS TABLE
# -----------------------------------------------
st.subheader(f"📋 {len(filtered)} Short Opportunities")

if len(filtered) > 0:
    display_df = filtered[[
        'ticker', 'company_name', 'sector',
        'current_price', 'risk_score',
        'rsi', 'pe_ratio', 'pe_vs_sector',
        'free_cash_flow', 'earnings_growth',
        'short_interest', 'days_to_cover',
        'insider_selling', 'analyst_rating'
    ]].copy()

    display_df.columns = [
        'Ticker', 'Company', 'Sector',
        'Price', 'Risk Score',
        'RSI', 'P/E', 'P/E vs Sector',
        'FCF ($M)', 'Earnings Growth %',
        'Short Interest %', 'Days to Cover',
        'Insider Selling', 'Analyst Rating'
    ]

    # Format columns
    display_df['Price'] = display_df['Price'].apply(lambda x: f"${x:.2f}" if x else "N/A")
    display_df['Risk Score'] = display_df['Risk Score'].apply(lambda x: f"{x:.0f}")
    display_df['RSI'] = display_df['RSI'].apply(lambda x: f"{x:.1f}" if x else "N/A")
    display_df['Insider Selling'] = display_df['Insider Selling'].apply(lambda x: "⚠️ Yes" if x == 1 else "No")

    st.dataframe(display_df, use_container_width=True, height=400)
else:
    st.info("No stocks match your current filters. Try lowering the minimum risk score.")

st.markdown("---")

# -----------------------------------------------
# INDIVIDUAL STOCK DEEP DIVE
# -----------------------------------------------
st.subheader("🔎 Stock Deep Dive")

if len(filtered) > 0:
    selected_stock = st.selectbox(
        "Select a stock to analyze",
        filtered['ticker'].tolist(),
        format_func=lambda x: f"{x} — {filtered[filtered['ticker']==x]['company_name'].values[0]} (Score: {filtered[filtered['ticker']==x]['risk_score'].values[0]:.0f})"
    )

    stock_data = filtered[filtered['ticker'] == selected_stock].iloc[0]

    # Key metrics row
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Risk Score", f"{stock_data['risk_score']:.0f}/100")
    with c2:
        st.metric("RSI", f"{stock_data['rsi']:.1f}" if stock_data['rsi'] else "N/A")
    with c3:
        st.metric("P/E vs Sector", f"+{stock_data['pe_vs_sector']:.1f}" if stock_data['pe_vs_sector'] and stock_data['pe_vs_sector'] > 0 else str(stock_data['pe_vs_sector']))
    with c4:
        st.metric("Free Cash Flow", f"${stock_data['free_cash_flow']:.0f}M" if stock_data['free_cash_flow'] else "N/A")
    with c5:
        st.metric("Short Interest", f"{stock_data['short_interest']:.1f}%" if stock_data['short_interest'] else "N/A")
    with c6:
        st.metric("Days to Cover", f"{stock_data['days_to_cover']:.1f}" if stock_data['days_to_cover'] else "N/A")

    # Charts row
    col_left, col_right = st.columns([2, 1])

    with col_left:
        price_df = load_price_data(selected_stock)
        if not price_df.empty:
            st.plotly_chart(build_price_chart(price_df, selected_stock), use_container_width=True)

    with col_right:
        if stock_data['rsi']:
            st.plotly_chart(build_rsi_gauge(stock_data['rsi'], selected_stock), use_container_width=True)

    # -----------------------------------------------
    # NEWS SECTION
    # -----------------------------------------------
    st.markdown("---")
    st.subheader(f"📰 Latest News — {selected_stock}")

    articles = fetch_news(selected_stock, stock_data['company_name'])

    if articles:
        for article in articles:
            title = article.get('title', 'No title')
            source = article.get('source', {}).get('name', 'Unknown')
            published = article.get('publishedAt', '')[:10]
            url = article.get('url', '#')
            description = article.get('description', '')

            st.markdown(f"""
            <div class="news-card">
                <div class="news-title"><a href="{url}" target="_blank" style="color:#f0f0f0; text-decoration:none;">{title}</a></div>
                <div class="news-meta">{source} · {published}</div>
                <div style="font-size:0.78rem; color:rgba(240,240,240,0.5); margin-top:0.5rem;">{description}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No recent news found for this stock.")