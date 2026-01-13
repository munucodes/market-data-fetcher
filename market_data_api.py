import time
import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import PAGE, API, HEADERS, START_DATE, END_DATE, SLEEP_BETWEEN

def get_all_symbols(max_retries=3, delay_seconds=5):
    """Get all ticker symbols from the main page on the top left combobox."""

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"TICKER page requested... (attempt {attempt}/{max_retries})")
            r = requests.get(PAGE, headers=HEADERS, timeout=30)
            r.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"get_all_symbols error: {e}")
            if attempt == max_retries:
                # if all trials failed, re-raise the last error
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
        raise RuntimeError("Symbol list not found — page structure may have changed.")
    print(f"{len(syms)} TICKERS found.")
    return syms

def fetch_adjusted(symbol, start=START_DATE, end=END_DATE, pause=SLEEP_BETWEEN):
    """Get adjusted price series (type=1) for a single stock."""
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

        # column names may vary, look for the most common ones
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
        print(f"{symbol}: error ({e})")
        time.sleep(pause)
        return pd.DataFrame(columns=["Ticker","Tarih","KapanisTL"])
