"""
Fetch monthly benchmark returns (S&P 500, etc.) via yfinance and store in SQLite.
"""
import os, sqlite3, logging, math
from datetime import date

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cache.db"),
)

# Supported benchmarks: ticker → display name
BENCHMARKS = {
    "^GSPC":    "S&P 500",
    "^STOXX50E": "Euro Stoxx 50",
}


def _ensure_table(db_path=DB_PATH):
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_returns (
            ticker     TEXT NOT NULL,
            year_month TEXT NOT NULL,
            return_pct REAL,
            PRIMARY KEY (ticker, year_month)
        )
    """)
    con.commit()
    con.close()


def fetch_benchmark(ticker="^GSPC", db_path=DB_PATH):
    """Download monthly returns for ticker and upsert into DB. Returns number of rows stored."""
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        logger.error("yfinance / pandas not available — install them to use external benchmarks")
        return 0

    _ensure_table(db_path)

    today = date.today()
    data = yf.download(
        ticker,
        start="2016-01-01",
        end=today.strftime("%Y-%m-%d"),
        interval="1mo",
        auto_adjust=True,
        progress=False,
    )

    if data is None or data.empty:
        logger.warning(f"fetch_benchmark: no data for {ticker}")
        return 0

    # yfinance ≥0.2 may return MultiIndex columns — flatten
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    close = data["Close"]
    returns = close.pct_change() * 100  # monthly % return

    current_ym = today.strftime("%Y-%m")
    rows = []
    for idx, ret in returns.items():
        if ret is None or (isinstance(ret, float) and math.isnan(ret)):
            continue
        ym = idx.strftime("%Y-%m")
        if ym >= current_ym:  # skip current incomplete month
            continue
        rows.append((ticker, ym, float(ret)))

    con = sqlite3.connect(db_path)
    con.execute("DELETE FROM benchmark_returns WHERE ticker=? AND year_month>=?",
                (ticker, current_ym))
    con.commit()
    con.close()

    if not rows:
        return 0

    con = sqlite3.connect(db_path)
    con.executemany(
        "INSERT OR REPLACE INTO benchmark_returns (ticker, year_month, return_pct) VALUES (?, ?, ?)",
        rows,
    )
    con.commit()
    con.close()
    logger.info(f"fetch_benchmark: stored {len(rows)} months for {ticker}")
    return len(rows)


def fetch_all(db_path=DB_PATH):
    total = 0
    for ticker in BENCHMARKS:
        try:
            total += fetch_benchmark(ticker, db_path)
        except Exception as e:
            logger.error(f"fetch_benchmark error for {ticker}: {e}")
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    print(f"Stored {fetch_all()} benchmark rows.")


def needs_update(ticker="^GSPC", db_path=DB_PATH):
    """True if the ticker's data is missing or doesn't include the last complete month."""
    _ensure_table(db_path)
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT MAX(year_month) FROM benchmark_returns WHERE ticker=?", (ticker,)
        ).fetchone()
        con.close()
        latest = row[0] if row else None
        today = date.today()
        y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
        return latest is None or latest < f"{y}-{m:02d}"
    except Exception:
        return True
