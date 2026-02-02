import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date

# ---------------- Page Config ----------------
st.set_page_config(page_title="Market Overview", layout="wide")

# ---------------- Main Title ----------------
st.title("📊 Cross-Market Overview")
st.caption("Crypto • Oil • Stock Market | SQL-powered analytics")

# ---------------- DB Connection ----------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="Cross_Market_Analysis"
    )

# ---------------- Date Filters ----------------
col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("Start Date", date(2024, 1, 1))

with col2:
    end_date = st.date_input("End Date", date(2026, 1, 1))

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

# ---------------- KPI Metrics Query ----------------
avg_query = """
SELECT
    (SELECT AVG(price_usd)
     FROM crypto_prices
     WHERE coin_id = 'bitcoin' 
       AND date BETWEEN %s AND %s) AS btc_avg,

    (SELECT AVG(price_usd)
     FROM oil_prices
     WHERE date BETWEEN %s AND %s) AS oil_avg,

    (SELECT AVG(close)
     FROM stock_prices
     WHERE ticker = '^GSPC'
       AND date BETWEEN %s AND %s) AS sp500_avg,

    (SELECT AVG(close)
     FROM stock_prices
     WHERE ticker = '^NSEI'
       AND date BETWEEN %s AND %s) AS nifty_avg
"""

conn = get_connection()
avg_df = pd.read_sql(
    avg_query,
    conn,
    params=(
        start_date, end_date,
        start_date, end_date,
        start_date, end_date,
        start_date, end_date
    )
).fillna(0)

conn.close()

# ---------------- KPI Display ----------------
k1, k2, k3, k4 = st.columns(4)

k1.metric("₿ Bitcoin Avg ($)", f"{avg_df['btc_avg'][0]:,.2f}")
k2.metric("🛢 Oil Avg ($)", f"{avg_df['oil_avg'][0]:,.2f}")
k3.metric("📈 S&P 500 Avg", f"{avg_df['sp500_avg'][0]:,.2f}")
k4.metric("🇮🇳 NIFTY Avg", f"{avg_df['nifty_avg'][0]:,.2f}")

st.divider()

# ---------------- Daily Market Snapshot ----------------
snapshot_query = """
SELECT
    c.date,
    c.price_usd AS bitcoin_price,
    o.price_usd AS oil_price,
    s1.close AS sp500,
    s2.close AS nifty
FROM crypto_prices c
JOIN oil_prices o 
    ON c.date = o.date
JOIN stock_prices s1 
    ON c.date = s1.date AND s1.ticker = '^GSPC'
JOIN stock_prices s2 
    ON c.date = s2.date AND s2.ticker = '^NSEI'
WHERE c.coin_id = 'bitcoin'
  AND c.date BETWEEN %s AND %s
ORDER BY c.date DESC;
"""

conn = get_connection()
snapshot_df = pd.read_sql(
    snapshot_query,
    conn,
    params=(start_date, end_date)
)
conn.close()

st.subheader("📋 Daily Market Snapshot")

if snapshot_df.empty:
    st.warning("No data available for the selected date range.")
else:
    st.dataframe(snapshot_df, use_container_width=True)