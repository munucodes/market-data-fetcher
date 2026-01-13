"""
STOCK HISTORICAL PRICES (ADJUSTED)
------------------------------------------------
Each run:
- Fetches adjusted closing prices (in TL) for all stocks from 31.12.2001 to today.
- Deletes the old table in the database and creates a new one.
- Uses SQLite (single file, no installation required).
- Ideal for running daily or weekly (cron / Task Scheduler).

Author: Deniz Kertmen
"""


import sqlite3
import pandas as pd

from config import START_DATE, END_DATE, DB_FILE, TEMPLATE_XLSX, OUTPUT_XLSX
from market_data_api import get_all_symbols, fetch_adjusted
from excel_daily import fill_excel_from_db


def rebuild_database():
    symbols = get_all_symbols()
    all_df = []

    for i, sym in enumerate(symbols, 1):
        df = fetch_adjusted(sym)
        if not df.empty:
            all_df.append(df)
        if i % 20 == 0:
            print(f"{i}/{len(symbols)} stocks done…")

    if not all_df:
        print("No data fetched.")
        return

    df_final = pd.concat(all_df, ignore_index=True)
    df_final.sort_values(["Ticker","Tarih"], inplace=True)

    # Reset and write SQLite database
    conn = sqlite3.connect(DB_FILE)
    df_final.to_sql("prices_adjusted", conn, if_exists="replace", index=False)
    conn.close()

    print(f"\nDatabase rebuilt: {DB_FILE}")
    print(f"Total records: {len(df_final):,}")






if __name__ == "__main__":
    print(f"\nProcess started: {START_DATE} → {END_DATE}")
    rebuild_database()
    print("Completed ✅")

    print("\nExcel fill process started...")    
    fill_excel_from_db(TEMPLATE_XLSX, OUTPUT_XLSX, db_file=DB_FILE)
    print("Excel fill process completed ✅")