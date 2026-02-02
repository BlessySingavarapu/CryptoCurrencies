import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date

# ---------------- Page Config ----------------
st.set_page_config(page_title="Top 5 Crypto Analysis", layout="wide")

st.title("🪙 Top 5 Crypto Analysis")
st.caption("Daily price analysis for top cryptocurrencies")


# ---------------- DB Connection ----------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="Cross_Market_Analysis"
    )

# ---------------- Crypto Selector ----------------

conn = get_connection()

crypto_query = """
SELECT name, id
FROM cryptocurrencies
ORDER BY market_cap DESC
LIMIT 5;
"""

crypto_df = pd.read_sql(crypto_query, conn)
conn.close()

crypto_map = dict(zip(crypto_df["id"], crypto_df["name"]))

#crypto_map = {
 #   "Bitcoin": "bitcoin",
  #  "Ethereum": "ethereum",
  #  "Tether": "tether"
#}

selected_crypto = st.selectbox(
    "Select a Cryptocurrency",
    list(crypto_map.keys())
)

ticker = crypto_map[selected_crypto]

# ---------------- Date Filters ----------------
col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("Start Date", date(2024, 1, 1))

with col2:
    end_date = st.date_input("End Date", date.today())

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

# ---------------- SQL Query ----------------
price_query = """
SELECT date, price_usd
FROM crypto_prices
WHERE coin_id = %s
  AND date BETWEEN %s AND %s
ORDER BY date;
"""

conn = get_connection()
df = pd.read_sql(
    price_query,
    conn,
    params=(ticker, start_date, end_date)
)
conn.close()

# ---------------- Line Chart ----------------
st.subheader(f"📈 {selected_crypto.upper()} Price Trend")

if df.empty:
    st.warning("No data available for the selected period.")
else:
    st.line_chart(df.set_index("date")["price_usd"])

# ---------------- Data Table ----------------
st.subheader("📋 Daily Price Data")
st.dataframe(df, use_container_width=True)