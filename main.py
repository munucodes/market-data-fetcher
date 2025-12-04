"""
HISSE TARIHSEL FIYATLAR (DUZELTILMIS)
------------------------------------------------
Her calistirmada:
- 31.12.2001 → bugune kadar tum hisselerin duzeltilmis kapanislarini (TL) ceker.
- Veritabanindaki eski tabloyu siler, yenisini olusturur.
- SQLite kullanir (tek dosya, kurulumsuz).
- Gunluk veya haftalik calistirmak icin idealdir (cron / Task Scheduler).

Author: Deniz Kertmen
"""

import time, re, json, sqlite3
from datetime import date
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np


# ------------------------------------------------
# AYARLAR
# ------------------------------------------------
BASE = "https://www.isyatirim.com.tr"
PAGE = f"{BASE}/tr-tr/analiz/hisse/Sayfalar/Tarihsel-Fiyat-Bilgileri.aspx"
API  = f"{BASE}/_layouts/15/Isyatirim.Website/Common/Data.aspx/HisseTekil"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

START_DATE = "31-12-2001"
END_DATE   = date.today().strftime("%d-%m-%Y")
DB_FILE    = "adjusted_prices.db"
SLEEP_BETWEEN = 0.5  # saniye – yavas, nazik, istikrarli

TEMPLATE_XLSX = "templates/portfolio_trading.xlsx"      # gelen sablon
OUTPUT_XLSX   = "output/portfolio_trading_filled.xlsx"  # cikti


# ------------------------------------------------
# YARDIMCI FONKSIYONLAR
# ------------------------------------------------

def get_all_symbols(max_retries=3, delay_seconds=5):
    """Sol ustteki combobox’taki tum sembolleri döndürür."""

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Sembol sayfası isteniyor... (deneme {attempt}/{max_retries})")
            r = requests.get(PAGE, headers=HEADERS, timeout=30)
            r.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"get_all_symbols hata: {e}")
            if attempt == max_retries:
                # Bütün denemeler bitti, hâlâ olmuyorsa yukarı fırlat
                raise
            time.sleep(delay_seconds)

    soup = BeautifulSoup(r.text, "html.parser")
    symbols = set()

    for opt in soup.find_all("option"):
        txt = (opt.get_text() or "").strip()
        val = (opt.get("value") or "").strip()

        m = re.match(r"^([A-Z0-9]{3,6})(?:\s*-.*)?$", txt)
        if m:
            symbols.add(m.group(1))

        if re.fullmatch(r"[A-Z0-9]{3,6}", val):
            symbols.add(val)

    syms = sorted(symbols)
    if not syms:
        raise RuntimeError("Sembol listesi bulunamadı — sayfa yapısı değişmiş olabilir.")
    print(f"{len(syms)} sembol bulundu.")
    return syms


def fetch_adjusted(symbol, start=START_DATE, end=END_DATE, pause=SLEEP_BETWEEN):
    """Bir hisse icin duzeltilmis fiyat serisini (type=1) ceker."""
    params = {
    "hisse": symbol,
    "startdate": start,
    "enddate": end,
    }
    try:
        r = requests.get(API, params=params, headers=HEADERS, timeout=60)
        r.raise_for_status()
        data = r.json() if "json" in r.headers.get("content-type","") else json.loads(r.text)
        rows = data.get("value") or data
        if not rows:
            return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])
        df = pd.DataFrame(rows)

        # kolon isimleri farkli olabiliyor, en yayginlarini ara
        date_col  = next((c for c in df.columns if c.lower() in ["tarih","hg_tarih","hgdg_tarih","date"]), None)
        close_col = next((c for c in df.columns if c.lower() in ["kapanis","kapanistl","hgdg_kapanis","close","closeprice"]), None)
        if not date_col or not close_col:
            return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])

        out = df[[date_col, close_col]].copy()
        out.columns = ["Tarih","KapanisTL"]
        out["Ticker"] = symbol
        out["Tarih"] = pd.to_datetime(out["Tarih"], dayfirst=True, errors="coerce").dt.date
        out["KapanisTL"] = pd.to_numeric(out["KapanisTL"], errors="coerce")
        out = out.dropna(subset=["Tarih","KapanisTL"])
        time.sleep(pause)
        return out
    except Exception as e:
        print(f"{symbol}: hata ({e})")
        time.sleep(pause)
        return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])


# ------------------------------------------------
# ANA AKIS
# ------------------------------------------------
def rebuild_database():
    symbols = get_all_symbols()
    all_df = []

    for i, sym in enumerate(symbols, 1):
        df = fetch_adjusted(sym)
        if not df.empty:
            all_df.append(df)
        if i % 20 == 0:
            print(f"{i}/{len(symbols)} tamam…")

    if not all_df:
        print("Hic veri cekilemedi.")
        return

    df_final = pd.concat(all_df, ignore_index=True)
    df_final.sort_values(["Ticker","Tarih"], inplace=True)

    # SQLite veritabanini sifirla ve yaz
    conn = sqlite3.connect(DB_FILE)
    df_final.to_sql("prices_adjusted", conn, if_exists="replace", index=False)
    conn.close()

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



if __name__ == "__main__":
    #print(f"\nIslem basladi: {START_DATE} → {END_DATE}")
    #rebuild_database()
    #print("Tamamlandi ✅")

    print("\nExcel doldurma islemi basladi...")    
    fill_excel_from_db(TEMPLATE_XLSX, OUTPUT_XLSX, db_file=DB_FILE)
    print("Excel doldurma islemi tamamlandi ✅")