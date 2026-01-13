from datetime import date
# ------------------------------------------------
# CONFIGURATION
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
SLEEP_BETWEEN = 0.5  # seconds – slow, polite, consistent

TEMPLATE_XLSX = "templates/portfolio_trading.xlsx"      # incoming template
OUTPUT_XLSX   = "output/portfolio_trading_filled.xlsx"  # output