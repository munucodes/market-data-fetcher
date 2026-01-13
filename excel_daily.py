import pandas as pd
import numpy as np

from config import DB_FILE
from db import load_prices_from_db

def fill_excel_from_db(template_path, output_path, db_file=DB_FILE):
    """
    Excel template:
      - 1. row (row 0): dates starting from B1 (28/11/15, 29/11/15, ...)
      - A column (col 0): tickers starting from A2 (AEFES, AGESA, ...)
      - Cells from B2 onwards will be filled with prices.
    """

    # 1) Read template (no row used as header)
    template_df = pd.read_excel(template_path, header=None)

    # --- Ticker list: A column, from 2nd row down ---
    ticker_series = template_df.iloc[1:, 0]          # col 0, rows 1+
    tickers = ticker_series.dropna().astype(str).tolist()
    if not tickers:
        raise ValueError("Template Excel must have at least one ticker from A2 downwards.")

    # --- Date list: 1st row, from B column to the right ---
    date_series = template_df.iloc[0, 1:]            # row 0, cols 1+
    date_strings = date_series.dropna().astype(str)
    if date_strings.empty:
        raise ValueError("Template Excel must have at least one date from B1 onwards.")

    # dd/mm/yy or dd/mm/yyyy, day first to avoid US format confusion
    dates = pd.to_datetime(date_strings, dayfirst=True, errors="coerce").dt.date
    if dates.isnull().any():
        raise ValueError("Some dates in the 1st row could not be parsed (date format must be dd/mm/yy or dd/mm/yyyy).")

    dates_list = list(dates)
    start_iso = min(dates_list).isoformat()
    end_iso   = max(dates_list).isoformat()

    # --- Summary log ---
    print("\n=== Excel Fill Summary ===")
    print(f"Start date in template: {start_iso}")
    print(f"End date in template:   {end_iso}")
    print(f"Total dates:            {len(dates_list)}")
    print(f"Total tickers:          {len(tickers)}")
    print("============================\n")

    # 2) Load prices from database
    prices_df = load_prices_from_db(
        tickers=tickers,
        start_date=start_iso,
        end_date=end_iso,
        db_file=db_file,
    )

    if prices_df.empty:
        print("Warning: No prices were loaded from the database, saving template as is.")
        template_df.to_excel(output_path, header=False, index=False)
        return

    # 3) Pivot: rows are dates, columns are tickers
    price_matrix = (
        prices_df
        .pivot(index="Tarih", columns="Ticker", values="KapanisTL")
        .sort_index()
    )

    # Align with template's date and ticker list, forward fill for weekends/holidays
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

    # Round prices to 2 decimal places (only for Excel, does not affect DB)
    price_matrix = price_matrix.round(2)

    # For Excel: transpose so rows are tickers, columns are dates
    matrix_for_excel = price_matrix.T   # shape: (len(tickers), len(dates_list))

    # 4) Copy template and fill from B2 onwards
    filled = template_df.copy()
    n_tickers = len(tickers)
    n_dates   = len(dates_list)

    # rows 1..1+n_tickers-1, cols 1..1+n_dates-1
    filled.iloc[1:1 + n_tickers, 1:1 + n_dates] = matrix_for_excel.values

    # 5) Write out (using ExcelWriter for cleaner output)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        filled.to_excel(writer, header=False, index=False)

        # --- OUTPUT EXCEL DATE FORMATTING ---
    wb = writer.book
    ws = wb.active  # single sheet

    # Dates start at 2nd row (index 1), from B column (index 1)
    for col in range(1, 1 + len(dates_list)):
        cell = ws.cell(row=2, column=col+1)  # Excel rows/cols are 1-based
        cell.number_format = "dd/mm/yyyy"

    print(f"Excel successfully generated for range {start_iso} → {end_iso}")