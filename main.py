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
<<<<<<< HEAD
=======
import requests
from bs4 import BeautifulSoup
import numpy as np

>>>>>>> main

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

<<<<<<< HEAD
    print(f"\nDatabase rebuilt: {DB_FILE}")
    print(f"Total records: {len(df_final):,}")



=======
    print(f"\nVeritabani yenilendi: {DB_FILE}")
    print(f"Toplam kayit: {len(df_final):,}")

def load_prices_from_db(tickers, start_date=None, end_date=None, db_file=DB_FILE):
    """
    Veritabanindan secili hisselerin verisini ceker.
    Tarihler (start_date, end_date) ISO 'YYYY-MM-DD' string olabilir.
    """
    if not tickers:
        raise ValueError("En az bir ticker vermelisin.")

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
        print("Uyari: secili aralik icin hic veri bulunamadi.")
        return df

    # Tarih'i datetime'e cevir
    df["Tarih"] = pd.to_datetime(df["Tarih"], errors="coerce").dt.date
    return df

def fill_excel_from_db(template_path, output_path, db_file=DB_FILE):
    """
    Excel sablonu:
      - 1. satir (row 0): B1'den itibaren tarihler (28/11/15, 29/11/15, ...)
      - A sutunu (col 0): A2'den itibaren ticker'lar (AEFES, AGESA, ...)
      - B2 ve sonrasindaki hucreler fiyatlarla doldurulacak.
    """

    # 1) Sablonu oku (hiçbir satiri header olarak kullanma)
    template_df = pd.read_excel(template_path, header=None)

    # --- Ticker listesi: A sutunu, 2. satirdan asagiya ---
    ticker_series = template_df.iloc[1:, 0]          # col 0, rows 1+
    tickers = ticker_series.dropna().astype(str).tolist()
    if not tickers:
        raise ValueError("Sablon Excel'de A2'den asagiya en az bir ticker olmali.")

    # --- Tarih listesi: 1. satir, B sutunundan saga ---
    date_series = template_df.iloc[0, 1:]            # row 0, cols 1+
    date_strings = date_series.dropna().astype(str)
    if date_strings.empty:
        raise ValueError("Sablon Excel'de B1'den itibaren en az bir tarih olmali.")

    # dd/mm/yy veya dd/mm/yyyy, gun once: dayfirst=True US karisikliklarini engeller
    dates = pd.to_datetime(date_strings, dayfirst=True, errors="coerce").dt.date
    if dates.isnull().any():
        raise ValueError("1. satirdaki tarihlerden bazilari parse edilemedi (tarih formati dd/mm/yy ya da dd/mm/yyyy olmali).")

    dates_list = list(dates)
    start_iso = min(dates_list).isoformat()
    end_iso   = max(dates_list).isoformat()

    # --- Ozet log ---
    print("\n=== Excel Fill Summary ===")
    print(f"Start date in template: {start_iso}")
    print(f"End date in template:   {end_iso}")
    print(f"Total dates:            {len(dates_list)}")
    print(f"Total tickers:          {len(tickers)}")
    print("============================\n")

    # 2) Veritabanindan fiyatlari cek
    prices_df = load_prices_from_db(
        tickers=tickers,
        start_date=start_iso,
        end_date=end_iso,
        db_file=db_file,
    )

    if prices_df.empty:
        print("Uyari: Veritabanindan hic fiyat gelmedi, sablon oldugu gibi kaydediliyor.")
        template_df.to_excel(output_path, header=False, index=False)
        return

    # 3) Pivot: satirlar Tarih, sutunlar Ticker
    price_matrix = (
        prices_df
        .pivot(index="Tarih", columns="Ticker", values="KapanisTL")
        .sort_index()
    )

    # Sablondaki tarih ve ticker listesine gore hizala ve weekend/holiday icin ffill
    price_matrix = price_matrix.reindex(index=dates_list, columns=tickers)
    
    # last real date per ticker
    last_real = (
        prices_df.groupby("Ticker")["Tarih"]
        .max()
        .rename("LAST_REAL_DATE")
    )

    pm = price_matrix.copy()
    pm["Tarih"] = pm.index

    # normal ffill
    pm = pm.ffill()


    # remove future fill
    for t in tickers:
        lr = last_real.get(t)
        if lr is not None:
            pm.loc[pm["Tarih"] > lr, t] = np.nan

    pm.set_index("Tarih", inplace=True)
    price_matrix = pm

    # Fiyatlari 2 ondalık basamaga yuvarla (sadece Excel icin, DB'yi etkilemez)
    price_matrix = price_matrix.round(2)

    # Excel icin: satirlar ticker, sutunlar tarih olacak sekilde cevir
    matrix_for_excel = price_matrix.T   # shape: (len(tickers), len(dates_list))

    # 4) Sablonun kopyasini al ve B2'den itibaren doldur
    filled = template_df.copy()
    n_tickers = len(tickers)
    n_dates   = len(dates_list)

    # rows 1..1+n_tickers-1, cols 1..1+n_dates-1
    filled.iloc[1:1 + n_tickers, 1:1 + n_dates] = matrix_for_excel.values

    # 5) Disari yaz (ExcelWriter kullanarak daha temiz cikti)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        filled.to_excel(writer, header=False, index=False)

        # --- OUTPUT EXCEL DATE FORMATTING ---
    wb = writer.book
    ws = wb.active  # tek sheet

    # Tarihler 2. satirda (index 1), B sutunundan (index 1) itibaren basliyor
    for col in range(1, 1 + len(dates_list)):
        cell = ws.cell(row=2, column=col+1)  # Excel rows/cols are 1-based
        cell.number_format = "dd/mm/yyyy"

    print(f"Excel successfully generated for range {start_iso} → {end_iso}")
>>>>>>> main



if __name__ == "__main__":
    print(f"\nProcess started: {START_DATE} → {END_DATE}")
    rebuild_database()
    print("Completed ✅")

    print("\nExcel fill process started...")    
    fill_excel_from_db(TEMPLATE_XLSX, OUTPUT_XLSX, db_file=DB_FILE)
<<<<<<< HEAD
    print("Excel fill process completed ✅")
=======
    print("Excel doldurma islemi tamamlandi ✅")
>>>>>>> main
