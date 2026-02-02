import streamlit as st
import pandas as pd
import mysql.connector

# ---------------- Page Config ----------------
st.set_page_config(page_title="SQL Query Runner", layout="wide")

st.title("🔎 SQL Query Runner")
st.caption("Predefined analytical SQL queries")


# ---------------- DB Connection ----------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="Cross_Market_Analysis"
    )

# ---------------- Predefined Queries ----------------
queries = {
    "Top 3 Cryptocurrencies by Market Cap": """
        SELECT name, market_cap
        FROM cryptocurrencies
        ORDER BY market_cap DESC
        LIMIT 3;
    """,
    "List all coins where circulating supply exceeds 90% of total supply.": """
        SELECT name, circulating_supply, total_supply
        FROM cryptocurrencies
        WHERE circulating_supply > 0.9 * total_supply;
    """,
    "Get coins that are within 10% of their all-time-high (ATH).": """ 
        SELECT name, current_price, ath
        FROM cryptocurrencies
        WHERE current_price >= ath * 0.9;
    """,
    "Find the average market cap rank of coins with volume above $1B.": """
        SELECT AVG(market_cap_rank) AS avg_market_cap_rank
        FROM cryptocurrencies
        WHERE total_volume > 1000000000;
    """,
    "Get the most recently updated coin.": """
        SELECT name, date
        FROM cryptocurrencies
        ORDER BY date DESC
        LIMIT 1;
    """,

    "Find the highest daily price of Bitcoin in the last 365 days.":"""
        SELECT MAX(price_usd) AS highest_price
        FROM crypto_prices
        WHERE coin_id = 'bitcoin'
        AND date >= CURDATE() - INTERVAL 365 DAY;
    """,
    "Calculate the average daily price of Ethereum in the past 1 year.":"""
        SELECT AVG(price_usd) AS avg_price
        FROM crypto_prices
        WHERE coin_id = 'ethereum'
        AND date >= CURDATE() - INTERVAL 1 YEAR;
    """,
    "Show the daily price trend of Bitcoin in January 2025.":"""
        SELECT date, price_usd
        FROM crypto_prices
        WHERE coin_id = 'bitcoin'
        AND date BETWEEN '2025-01-01' AND '2025-01-31'
        ORDER BY date;
    """,
    "Find the coin with the highest average price over 1 year.":"""
        SELECT coin_id, AVG(price_usd) AS avg_price
        FROM crypto_prices
        WHERE date >= CURDATE() - INTERVAL 1 YEAR
        GROUP BY coin_id
        ORDER BY avg_price DESC
        LIMIT 1;
    """,
    "Get the % change in Bitcoin’s price between Sep 2024 and Sep 2025.":"""
        SELECT
        (
            (MAX(CASE WHEN date = '2025-09-01' THEN price_usd END) -
            MAX(CASE WHEN date = '2024-09-01' THEN price_usd END))
            /
            MAX(CASE WHEN date = '2024-09-01' THEN price_usd END)
        ) * 100 AS price_change_percentage
        FROM crypto_prices
        WHERE coin_id = 'bitcoin'
        AND date IN ('2024-09-01', '2025-09-01');
    """,

    "Find the highest oil price in the last 5 years.": """ 
        SELECT MAX(price_usd) AS highest_price_last_5_years
        FROM oil_prices
        WHERE date >= CURDATE() - INTERVAL 5 YEAR;
    """,
    "Get the average oil price per year.": """
        SELECT YEAR(date) AS year, AVG(price_usd) AS avg_oil_price
        FROM oil_prices
        GROUP BY YEAR(date)
        ORDER BY year;
    """,
    "Show oil prices during COVID crash (March–April 2020).":"""
        SELECT date, price_usd
        FROM oil_prices
        WHERE date BETWEEN '2020-03-01' AND '2020-04-30'
        ORDER BY date;
    """,
    "Find the lowest price of oil in the last 10 years.":"""
        SELECT MIN(price_usd) AS lowest_price_last_10_years
        FROM oil_prices
        WHERE date >= CURDATE() - INTERVAL 10 YEAR;
    """,
    "Calculate the volatility of oil prices (max-min difference per year).":"""
        SELECT YEAR(date) AS year, MAX(price_usd) - MIN(price_usd) AS yearly_volatility
        FROM oil_prices
        GROUP BY YEAR(date)
        ORDER BY year;
    """,

    "Get all stock prices for a given ticker":"""
        SELECT *
        FROM stock_prices
        WHERE ticker = '^GSPC'
        ORDER BY date;
    """,
    "Find the highest closing price for NASDAQ (^IXIC)": """
        SELECT MAX(close) AS highest_close_nasdaq
        FROM stock_prices
        WHERE ticker = '^IXIC';
    """,
    "List top 5 days with highest price difference (high - low) for S&P 500 (^GSPC)":"""
        SELECT date, high, low, (high - low) AS price_difference
        FROM stock_prices
        WHERE ticker = '^GSPC'
        ORDER BY price_difference DESC
        LIMIT 5;
    """,
    "Get monthly average closing price for each ticker":"""
        SELECT ticker, YEAR(date) AS year, MONTH(date) AS month, AVG(close) AS avg_monthly_close
        FROM stock_prices
        GROUP BY ticker,YEAR(date),MONTH(date)
        ORDER BY ticker, year, month;
    """,
    "Get average trading volume of NSEI in 2024":"""
        SELECT AVG(volume) AS avg_volume_nsei_2024
        FROM stock_prices
        WHERE ticker = '^NSEI'
        AND YEAR(date) = 2024;
    """,

    "Compare Bitcoin vs Oil average price in 2025.":"""
    SELECT
    (SELECT AVG(price_usd)
     FROM crypto_prices
     WHERE coin_id = 'bitcoin'
       AND YEAR(date) = 2025) AS bitcoin_avg_2025,

    (SELECT AVG(price_usd)
     FROM oil_prices
     WHERE YEAR(date) = 2025) AS oil_avg_2025;
    """,
    "Check if Bitcoin moves with S&P 500 (correlation idea).":"""
    SELECT
    c.date,
    c.price_usd AS bitcoin_price,
    s.close AS sp500_price
FROM crypto_prices c
JOIN stock_prices s
    ON c.date = s.date
WHERE c.coin_id = 'bitcoin'
  AND s.ticker = '^GSPC'
ORDER BY c.date;
    """,
    "Compare Ethereum and NASDAQ daily prices for 2025.":""" 
    SELECT
    c.date,
    c.price_usd AS ethereum_price,
    s.close AS nasdaq_price
FROM crypto_prices c
JOIN stock_prices s
    ON c.date = s.date
WHERE c.coin_id = 'ethereum'
  AND s.ticker = '^IXIC'
  AND YEAR(c.date) = 2025
ORDER BY c.date;
    """,
    "Find days when oil price spiked and compare with Bitcoin price change.":"""
    SELECT
    o.date,
    o.price_usd AS oil_price,
    c.price_usd AS bitcoin_price
FROM oil_prices o
JOIN crypto_prices c
    ON o.date = c.date
WHERE c.coin_id = 'bitcoin'
  AND o.price_usd >
      (SELECT AVG(price_usd) * 1.05 FROM oil_prices)
ORDER BY o.date;
    """,
    "Compare top 3 coins daily price trend vs Nifty (^NSEI).":"""
   SELECT
    c.date,
    c.coin_id,
    c.price_usd AS crypto_price,
    s.close AS nifty_price
FROM crypto_prices c
JOIN (
    SELECT id
    FROM cryptocurrencies
    ORDER BY market_cap DESC
    LIMIT 3
) top_crypto
    ON c.coin_id = top_crypto.id
JOIN stock_prices s
    ON c.date = s.date
WHERE s.ticker = '^NSEI'
  AND YEAR(c.date) = 2025
ORDER BY c.date;
    """,
    "Compare stock prices (^GSPC) with crude oil prices on the same dates":"""
    SELECT
    s.date,
    s.close AS sp500_price,
    o.price_usd AS oil_price
FROM stock_prices s
JOIN oil_prices o
    ON s.date = o.date
WHERE s.ticker = '^GSPC'
ORDER BY s.date;
    """,
    "Correlate Bitcoin closing price with crude oil closing price (same date)":"""
    SELECT
    c.date,
    c.price_usd AS bitcoin_price,
    o.price_usd AS oil_price
FROM crypto_prices c
JOIN oil_prices o
    ON c.date = o.date
WHERE c.coin_id = 'bitcoin'
ORDER BY c.date;
    """,
    "Compare NASDAQ (^IXIC) with Ethereum price trends":"""
    SELECT
    s.date,
    s.close AS nasdaq_price,
    c.price_usd AS ethereum_price
FROM stock_prices s
JOIN crypto_prices c
    ON s.date = c.date
WHERE s.ticker = '^IXIC'
  AND c.coin_id = 'ethereum'
ORDER BY s.date;
    """,
    "Join top 3 crypto coins with stock indices for 2025":"""
    SELECT
    c.date,
    c.coin_id,
    c.price_usd AS crypto_price,
    s.ticker,
    s.close AS stock_price
FROM crypto_prices c
JOIN (
    SELECT id
    FROM cryptocurrencies
    ORDER BY market_cap DESC
    LIMIT 3
) top_crypto
    ON c.coin_id = top_crypto.id
JOIN stock_prices s
    ON c.date = s.date
WHERE YEAR(c.date) = 2025
ORDER BY c.date;
    """,
    "Multi-join: stock prices, oil prices, and Bitcoin prices for daily comparison":"""
    SELECT
    c.date,
    c.price_usd AS bitcoin_price,
    o.price_usd AS oil_price,
    s.close AS sp500_price
FROM crypto_prices c
JOIN oil_prices o
    ON c.date = o.date
JOIN stock_prices s
    ON c.date = s.date
WHERE c.coin_id = 'bitcoin'
  AND s.ticker = '^GSPC'
ORDER BY c.date;
    """ 

}

# ---------------- Query Selector ----------------
selected_query = st.selectbox(
    "📌 Select a Query",
    list(queries.keys())
)

# ---------------- Run Query Button ----------------
if st.button("▶ Run Query"):
    try:
        conn = get_connection()
        df = pd.read_sql(queries[selected_query], conn)
        conn.close()

        st.success("Query executed successfully")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error("Error executing query")
        st.code(str(e))

# ---------------- Info Message ----------------
st.info("💡 These queries are executed directly on the SQL database")