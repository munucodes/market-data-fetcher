"""
İŞ YATIRIM HİSSE TARİHSEL FİYATLAR (DÜZELTİLMİŞ)
------------------------------------------------
Her çalıştırmada:
- 31.12.2001 → bugüne kadar tüm hisselerin düzeltilmiş kapanışlarını (TL) çeker.
- Veritabanındaki eski tabloyu siler, yenisini oluşturur.
- SQLite kullanır (tek dosya, kurulumsuz).
- Günlük veya haftalık çalıştırmak için idealdir (cron / Task Scheduler).

Yazan: ChatGPT (GPT-5)
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
API  = f"{BASE}/_layouts/15/Isyatirim.Website/Common/Data.aspx/GetStockData"

START_DATE = "31-12-2001"
END_DATE   = date.today().strftime("%d-%m-%Y")
DB_FILE    = "isyatirim_duzeltilmis.db"
SLEEP_BETWEEN = 0.5  # saniye – yavaş, nazik, istikrarlı


# ------------------------------------------------
# YARDIMCI FONKSİYONLAR
# ------------------------------------------------
def get_all_symbols():
    """Sol üstteki combobox’taki tüm sembolleri döndürür."""
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
        raise RuntimeError("Sembol listesi bulunamadı — sayfa yapısı değişmiş olabilir.")
    print(f"{len(syms)} sembol bulundu.")
    return syms


def fetch_adjusted(symbol, start=START_DATE, end=END_DATE, pause=SLEEP_BETWEEN):
    """Bir hisse için düzeltilmiş fiyat serisini (type=1) çeker."""
    params = {"symbol": symbol, "startdate": start, "enddate": end, "type": "1"}
    try:
        r = requests.get(API, params=params, timeout=60)
        r.raise_for_status()
        data = r.json() if "json" in r.headers.get("content-type","") else json.loads(r.text)
        rows = data.get("value") or data
        if not rows:
            return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])
        df = pd.DataFrame(rows)

        # kolon isimleri farklı olabiliyor, en yaygınlarını ara
        date_col  = next((c for c in df.columns if c.lower() in ["tarih","hg_tarih","hgdg_tarih","date"]), None)
        close_col = next((c for c in df.columns if c.lower() in ["kapanis","kapanistl","hgdg_kapanis","close","closeprice"]), None)
        if not date_col or not close_col:
            return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])

        out = df[[date_col, close_col]].copy()
        out.columns = ["Tarih","KapanisTL"]
        out["Ticker"] = symbol
        out["Tarih"] = pd.to_datetime(out["Tarih"], errors="coerce").dt.date
        out["KapanisTL"] = pd.to_numeric(out["KapanisTL"], errors="coerce")
        out = out.dropna(subset=["Tarih","KapanisTL"])
        time.sleep(pause)
        return out
    except Exception as e:
        print(f"{symbol}: hata ({e})")
        time.sleep(pause)
        return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])


# ------------------------------------------------
# ANA AKIŞ
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
        print("Hiç veri çekilemedi.")
        return

    df_final = pd.concat(all_df, ignore_index=True)
    df_final.sort_values(["Ticker","Tarih"], inplace=True)

    # SQLite veritabanını sıfırla ve yaz
    conn = sqlite3.connect(DB_FILE)
    df_final.to_sql("prices_adjusted", conn, if_exists="replace", index=False)
    conn.close()

    print(f"\nVeritabanı yenilendi: {DB_FILE}")
    print(f"Toplam kayıt: {len(df_final):,}")


if __name__ == "__main__":
    print(f"\nİşlem başladı: {START_DATE} → {END_DATE}")
    rebuild_database()
    print("Tamamlandı ✅")
