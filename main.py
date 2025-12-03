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
DB_FILE    = "isyatirim_duzeltilmis.db"
SLEEP_BETWEEN = 0.5  # saniye – yavas, nazik, istikrarli

TEMPLATE_XLSX = "templates/portfolio_trading.xlsx"      # gelen sablon
OUTPUT_XLSX   = "output/portfolio_trading_filled.xlsx"  # cikti


# ------------------------------------------------
# YARDIMCI FONKSIYONLAR
# ------------------------------------------------
def get_all_symbols():
    """Sol ustteki combobox’taki tum sembolleri dsndurur."""
    r = requests.get(PAGE, timeout=30)
    r.raise_for_status()
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
        raise RuntimeError("Sembol listesi bulunamadi — sayfa yapisi degismis olabilir.")
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

def fill_excel_from_db(template_path, output_path, db_file=DB_FILE, date_col_name=None):
    """
    Bos (sablon) Excel dosyasini SQLite veritabanindan cektigimiz fiyatlarla doldurur.

    Varsayim:
      - Ilk sutun tarih sutunu (veya date_col_name ile verilen sutun)
      - Diger sutun adlari ticker sembolleri
    """
    # 1) Sablon Excel'i oku
    template_df = pd.read_excel(template_path)

    # Tarih sutununu belirle
    if date_col_name is not None and date_col_name in template_df.columns:
        date_col = date_col_name
    else:
        # yoksa ilk sutunu tarih varsay
        date_col = template_df.columns[0]

    # Ticker sutunlari (tarih sutunundan baska hepsi)
    ticker_cols = [c for c in template_df.columns if c != date_col]
    if not ticker_cols:
        raise ValueError("Sablon Excel'de ticker sutunu yok gibi gorunuyor. En az bir ticker sutunu olmali.")

    # Tarihleri normalize et
    dates = pd.to_datetime(template_df[date_col], format="%d/%m/%Y", errors="coerce").dt.date

    # 2) Veritabanindan ilgili tarih araligini ve ticker'lari cek
    start_iso = min(dates).isoformat()
    end_iso   = max(dates).isoformat()

    prices_df = load_prices_from_db(
        tickers=ticker_cols,
        start_date=start_iso,
        end_date=end_iso,
        db_file=db_file,
    )

    if prices_df.empty:
        print("Uyari: Veritabanindan hic fiyat donmedi, Excel bos kalacak.")
        filled = template_df.copy()
        filled.to_excel(output_path, index=False)
        print(f"Bos Excel sablonu {output_path} dosyasina yazildi.")
        return

    # 3) Pivot: satirlarda Tarih, sutunlarda Ticker olacak sekilde
    price_matrix = (
        prices_df
        .pivot(index="Tarih", columns="Ticker", values="KapanisTL")
        .sort_index()
    )

    # Excel'deki tarih ve ticker setine gore yeniden hizala
    price_matrix = price_matrix.reindex(index=dates, columns=ticker_cols)
    price_matrix = price_matrix.ffill()


    # 4) Sablon uzerine yazarak doldur
    filled = template_df.copy()
    for t in ticker_cols:
        if t in price_matrix.columns:
            filled[t] = price_matrix[t].values
        else:
            # Verisi olmayan ticker'lar NaN kalir
            print(f"Uyari: {t} icin veritabanda hic kayit yok, sutun bos kalacak.")

    # 5) Disari yaz
    filled.to_excel(output_path, index=False)
    print(f"Excel olusturuldu: {output_path}")
    # --- Summary information (beginning) ---
    print("\n=== Excel Fill Summary ===")
    print(f"Start date in template: {start_iso}")
    print(f"End date in template:   {end_iso}")
    print(f"Total dates:            {len(dates)}")
    print(f"Total tickers:          {len(ticker_cols)}")
    print("============================\n")

if __name__ == "__main__":
    #print(f"\nIslem basladi: {START_DATE} → {END_DATE}")
    #rebuild_database()
    #print("Tamamlandi ✅")

    print("\nExcel doldurma islemi basladi...")    
    fill_excel_from_db(TEMPLATE_XLSX, OUTPUT_XLSX, db_file=DB_FILE, date_col_name=None)
    print("Excel doldurma islemi tamamlandi ✅")

