import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sqlalchemy as sa
import ast

# -----------------------------------------------
# STEP 1: Connect to the SQLite database
# -----------------------------------------------
DB_PATH = "sqlite:////Users/spencer/Desktop/python-projects/stock-pipeline/stocks.db"

def get_engine():
    return sa.create_engine(DB_PATH)

# -----------------------------------------------
# STEP 2: Load fundamentals data
# -----------------------------------------------
def load_fundamentals():
    engine = get_engine()
    query = """
        SELECT 
            ticker,
            company_name,
            sector,
            all_time_high,
            current_price,
            pct_from_ath,
            week_52_high,
            week_52_low,
            quarterly_earnings,
            is_profitable,
            earnings_trend,
            debt_to_equity,
            analyst_rating
        FROM stock_fundamentals
        ORDER BY pct_from_ath ASC
    """
    df = pd.read_sql(query, engine)
    return df

# -----------------------------------------------
# STEP 3: Load price history for a specific ticker
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
# STEP 4: Build candlestick price chart
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
# STEP 5: Build quarterly earnings chart
# -----------------------------------------------
def build_earnings_chart(quarterly_earnings_str, ticker):
    try:
        # Parse the earnings list from string
        earnings = ast.literal_eval(quarterly_earnings_str)
        
        if not earnings:
            return None

        # Create quarter labels
        quarters = [f"Q-{i+1}" for i in range(len(earnings))]
        
        # Color bars green if profitable, red if not
        colors = ['#4ade80' if e > 0 else '#f87171' for e in earnings]

        fig = go.Figure(data=[go.Bar(
            x=quarters,
            y=earnings,
            marker_color=colors,
            name='Net Income (Millions)'
        )])

        fig.update_layout(
            title=f"{ticker} Quarterly Net Income (Last 4 Quarters)",
            xaxis_title="Quarter",
            yaxis_title="Net Income ($M)",
            plot_bgcolor='#111111',
            paper_bgcolor='#111111',
            font=dict(color='#f0f0f0'),
            xaxis=dict(gridcolor='#222222'),
            yaxis=dict(gridcolor='#222222'),
            height=300
        )
        return fig
    except:
        return None

# -----------------------------------------------
# STEP 6: Build sector breakdown chart
# -----------------------------------------------
def build_sector_chart(df):
    sector_counts = df['sector'].value_counts().reset_index()
    sector_counts.columns = ['sector', 'count']

    fig = px.bar(
        sector_counts,
        x='count',
        y='sector',
        orientation='h',
        color='count',
        color_continuous_scale=['#222222', '#e8ff6b'],
        title="Stocks Down 10%+ by Sector"
    )
    fig.update_layout(
        plot_bgcolor='#111111',
        paper_bgcolor='#111111',
        font=dict(color='#f0f0f0'),
        xaxis=dict(gridcolor='#222222'),
        yaxis=dict(gridcolor='#222222'),
        height=400,
        showlegend=False,
        coloraxis_showscale=False
    )
    return fig

# -----------------------------------------------
# PAGE CONFIG
# -----------------------------------------------
st.set_page_config(
    page_title="S&P 500 Opportunity Scanner",
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
        div[data-testid="stDataFrame"] { background: #111111; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------
# HEADER
# -----------------------------------------------
st.title("📉 S&P 500 Opportunity Scanner")
st.markdown("*Stocks trading significantly below their all time highs — with profitability and financial health data*")
st.markdown("---")

# -----------------------------------------------
# LOAD DATA
# -----------------------------------------------
df = load_fundamentals()

# -----------------------------------------------
# SIDEBAR FILTERS
# -----------------------------------------------
with st.sidebar:
    st.header("🔍 Filters")

    # Dropdown threshold for % below ATH
    ath_threshold = st.slider(
        "Min % Below All Time High",
        min_value=-80,
        max_value=-10,
        value=-10,
        step=5,
        format="%d%%"
    )

    # Sector filter
    sectors = ["All Sectors"] + sorted(df['sector'].dropna().unique().tolist())
    selected_sector = st.selectbox("Sector", sectors)

    # Profitability filter
    profitability = st.selectbox(
        "Profitability",
        ["All", "Profitable Only", "Unprofitable Only"]
    )

    # Earnings trend filter
    trend = st.selectbox(
        "Earnings Trend",
        ["All", "Growing", "Shrinking", "Flat"]
    )

    # Analyst rating filter
    ratings = ["All"] + sorted(df['analyst_rating'].dropna().unique().tolist())
    selected_rating = st.selectbox("Analyst Rating", ratings)

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data"):
        with st.spinner("Running pipeline..."):
            import subprocess
            subprocess.run(["python3", "pipeline.py"])
        st.success("Data updated!")

# -----------------------------------------------
# APPLY FILTERS
# -----------------------------------------------
filtered = df[df['pct_from_ath'] <= ath_threshold].copy()

if selected_sector != "All Sectors":
    filtered = filtered[filtered['sector'] == selected_sector]

if profitability == "Profitable Only":
    filtered = filtered[filtered['is_profitable'] == 1]
elif profitability == "Unprofitable Only":
    filtered = filtered[filtered['is_profitable'] == 0]

if trend != "All":
    filtered = filtered[filtered['earnings_trend'] == trend]

if selected_rating != "All":
    filtered = filtered[filtered['analyst_rating'] == selected_rating]

# -----------------------------------------------
# SUMMARY STATS
# -----------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Stocks Found", len(filtered))
with col2:
    profitable_count = filtered['is_profitable'].sum()
    st.metric("Profitable", f"{profitable_count} ({int(profitable_count/len(filtered)*100) if len(filtered) > 0 else 0}%)")
with col3:
    avg_drop = filtered['pct_from_ath'].mean()
    st.metric("Avg Drop from ATH", f"{avg_drop:.1f}%" if not pd.isna(avg_drop) else "N/A")
with col4:
    growing = len(filtered[filtered['earnings_trend'] == 'Growing'])
    st.metric("Growing Earnings", growing)

st.markdown("---")

# -----------------------------------------------
# SECTOR BREAKDOWN CHART
# -----------------------------------------------
if len(filtered) > 0:
    st.plotly_chart(build_sector_chart(filtered), use_container_width=True)
    st.markdown("---")

# -----------------------------------------------
# MAIN RESULTS TABLE
# -----------------------------------------------
st.subheader(f"📋 {len(filtered)} Stocks Matching Filters")

if len(filtered) > 0:
    # Format the display table
    display_df = filtered[[
        'ticker', 'company_name', 'sector',
        'current_price', 'all_time_high', 'pct_from_ath',
        'week_52_high', 'week_52_low',
        'is_profitable', 'earnings_trend',
        'debt_to_equity', 'analyst_rating'
    ]].copy()

    # Rename columns for display
    display_df.columns = [
        'Ticker', 'Company', 'Sector',
        'Current Price', 'All Time High', '% From ATH',
        '52W High', '52W Low',
        'Profitable', 'Earnings Trend',
        'Debt/Equity', 'Analyst Rating'
    ]

    # Format columns
    display_df['Current Price'] = display_df['Current Price'].apply(lambda x: f"${x:.2f}")
    display_df['All Time High'] = display_df['All Time High'].apply(lambda x: f"${x:.2f}")
    display_df['% From ATH'] = display_df['% From ATH'].apply(lambda x: f"{x:.1f}%")
    display_df['52W High'] = display_df['52W High'].apply(lambda x: f"${x:.2f}" if x else "N/A")
    display_df['52W Low'] = display_df['52W Low'].apply(lambda x: f"${x:.2f}" if x else "N/A")
    display_df['Profitable'] = display_df['Profitable'].apply(lambda x: "✅" if x == 1 else "❌")

    st.dataframe(display_df, use_container_width=True, height=400)
else:
    st.info("No stocks match your current filters. Try adjusting the filters.")

st.markdown("---")

# -----------------------------------------------
# INDIVIDUAL STOCK DEEP DIVE
# -----------------------------------------------
st.subheader("🔎 Stock Deep Dive")

if len(filtered) > 0:
    selected_stock = st.selectbox(
        "Select a stock to analyze",
        filtered['ticker'].tolist(),
        format_func=lambda x: f"{x} — {filtered[filtered['ticker']==x]['company_name'].values[0]}"
    )

    stock_data = filtered[filtered['ticker'] == selected_stock].iloc[0]

    # Stock stats row
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    with c1:
        st.metric("Current Price", f"${stock_data['current_price']:.2f}")
    with c2:
        st.metric("All Time High", f"${stock_data['all_time_high']:.2f}")
    with c3:
        st.metric("% From ATH", f"{stock_data['pct_from_ath']:.1f}%")
    with c4:
        st.metric("52 Week High", f"${stock_data['week_52_high']:.2f}" if stock_data['week_52_high'] else "N/A")
    with c5:
        st.metric("52 Week Low", f"${stock_data['week_52_low']:.2f}" if stock_data['week_52_low'] else "N/A")
    with c6:
        st.metric("Debt/Equity", f"{stock_data['debt_to_equity']:.2f}" if stock_data['debt_to_equity'] else "N/A")
    with c7:
        st.metric("Analyst Rating", stock_data['analyst_rating'] or "N/A")

    # Price and earnings charts side by side
    col_left, col_right = st.columns(2)

    with col_left:
        price_df = load_price_data(selected_stock)
        if not price_df.empty:
            st.plotly_chart(build_price_chart(price_df, selected_stock), use_container_width=True)

    with col_right:
        earnings_fig = build_earnings_chart(stock_data['quarterly_earnings'], selected_stock)
        if earnings_fig:
            st.plotly_chart(earnings_fig, use_container_width=True)
        else:
            st.info("No earnings data available for this stock.")
else:
    st.info("No stocks match your current filters.")