import sqlite3
import pandas as pd

from config import DB_FILE

def load_prices_from_db(tickers, start_date=None, end_date=None, db_file=DB_FILE):
    """
    Gets selected stock data from the database.
    Dates (start_date, end_date) can be ISO 'YYYY-MM-DD' strings.
    """
    if not tickers:
        raise ValueError("At least one ticker must be provided.")
    conn = sqlite3.connect(db_file)

    placeholders = ",".join("?" * len(tickers))
    query = f"""
        SELECT Ticker, Tarih, KapanisTL
        FROM prices_adjusted
        WHERE Ticker IN ({placeholders})
    """

    params = list(tickers)

    if start_date is not None:
        query += " AND Tarih >= ?"
        params.append(start_date)

    if end_date is not None:
        query += " AND Tarih <= ?"
        params.append(end_date)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        print("Warning: No data found for the selected range.")
        return df

    # Convert 'Tarih' to datetime
    df["Tarih"] = pd.to_datetime(df["Tarih"], errors="coerce").dt.date
    return df
