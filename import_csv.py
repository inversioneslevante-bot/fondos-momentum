"""
Parse the Mediolanum/Morningstar CSV and populate SQLite.
Usage: python3 import_csv.py rentabilidades.csv
"""
import os
import sys
import sqlite3
import csv
import re
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")

# Column indices (0-based) for each period's return
# Derived by mapping the header rows in the CSV
PERIOD_COLS = {
    "1m":  17,
    "3m":  18,
    "6m":  19,
    "ytd": 21,
    "1a":  23,
    "3a":  25,
    "5a":  27,
    "10a": 29,
}
ANNUAL_COLS = {
    2013: 31, 2014: 33, 2015: 35, 2016: 37,
    2017: 39, 2018: 41, 2019: 43, 2020: 45,
    2021: 47, 2022: 49, 2023: 51, 2024: 53,
    2025: 55,
}


def _clean(val: str):
    """Return float or None from a cell string."""
    v = val.strip() if val else ""
    if v in ("", "n.d.", "n.a.", "N/A", "-"):
        return None
    # Remove thousands separator that can appear as comma inside quoted fields
    v = v.replace(" ", "").replace("€", "").replace("$", "")
    try:
        return float(v)
    except ValueError:
        return None


def _get(row, idx):
    return row[idx] if idx < len(row) else ""


def _valid_isin(s: str) -> bool:
    s = s.strip()
    return bool(s) and len(s) >= 10 and s[0].isalpha() and s[1].isalpha()


def init_schema(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS funds (
            isin TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category_morningstar TEXT,
            category_mediolanum TEXT,
            manager TEXT,
            family TEXT,
            imported_at TEXT
        );
        CREATE TABLE IF NOT EXISTS period_returns (
            isin TEXT PRIMARY KEY,
            return_1m  REAL,
            return_3m  REAL,
            return_6m  REAL,
            return_ytd REAL,
            return_1a  REAL,
            return_3a  REAL,
            return_5a  REAL,
            return_10a REAL,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS annual_returns (
            isin    TEXT    NOT NULL,
            year    INTEGER NOT NULL,
            return_pct    REAL,
            volatility_pct REAL,
            PRIMARY KEY (isin, year)
        );
        CREATE TABLE IF NOT EXISTS top5_year_cache (
            period     TEXT PRIMARY KEY,
            data       TEXT NOT NULL,
            computed_at TEXT
        );
    """)
    con.commit()


def wipe_data(con):
    con.executescript("""
        DELETE FROM funds;
        DELETE FROM period_returns;
        DELETE FROM annual_returns;
        DELETE FROM top5_year_cache;
    """)
    con.commit()


def import_csv(csv_path: str) -> int:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    init_schema(con)
    wipe_data(con)

    now = datetime.utcnow().isoformat()
    imported = 0

    with open(csv_path, encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for line_no, row in enumerate(reader):
            # Skip the 4 header rows (0-based: rows 0-3)
            if line_no < 4:
                continue

            if not row or len(row) < 5:
                continue

            name = row[0].strip()
            isin = row[1].strip()

            # Stop at footer / disclaimer section
            if "SOLO PARA USO INTERNO" in name or "Fondos NO" in name or name == "Fondo en SOFT-CLOSE":
                break
            if not name or not _valid_isin(isin):
                continue

            cat_ms  = row[2].strip()  if len(row) > 2  else ""
            cat_med = row[3].strip()  if len(row) > 3  else ""
            manager = row[4].strip()  if len(row) > 4  else ""
            family  = row[5].strip()  if len(row) > 5  else ""

            # ── Period returns ──────────────────────────────────────────────
            r1m  = _clean(_get(row, PERIOD_COLS["1m"]))
            r3m  = _clean(_get(row, PERIOD_COLS["3m"]))
            r6m  = _clean(_get(row, PERIOD_COLS["6m"]))
            rytd = _clean(_get(row, PERIOD_COLS["ytd"]))
            r1a  = _clean(_get(row, PERIOD_COLS["1a"]))
            r3a  = _clean(_get(row, PERIOD_COLS["3a"]))
            r5a  = _clean(_get(row, PERIOD_COLS["5a"]))
            r10a = _clean(_get(row, PERIOD_COLS["10a"]))

            con.execute("""
                INSERT OR REPLACE INTO funds
                (isin,name,category_morningstar,category_mediolanum,manager,family,imported_at)
                VALUES (?,?,?,?,?,?,?)
            """, (isin, name, cat_ms, cat_med, manager, family, now))

            con.execute("""
                INSERT OR REPLACE INTO period_returns
                (isin,return_1m,return_3m,return_6m,return_ytd,return_1a,return_3a,return_5a,return_10a,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (isin, r1m, r3m, r6m, rytd, r1a, r3a, r5a, r10a, now))

            # ── Annual returns ──────────────────────────────────────────────
            for year, col in ANNUAL_COLS.items():
                ret = _clean(_get(row, col))
                vol = _clean(_get(row, col + 1)) if col + 1 < len(row) else None
                if ret is not None:
                    con.execute("""
                        INSERT OR REPLACE INTO annual_returns (isin, year, return_pct, volatility_pct)
                        VALUES (?,?,?,?)
                    """, (isin, year, ret, vol))

            imported += 1

    con.commit()
    con.close()
    return imported


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "rentabilidades.csv"
    if not os.path.exists(path):
        print(f"ERROR: file not found: {path}")
        sys.exit(1)
    n = import_csv(path)
    print(f"OK — {n} fondos importados en {DB_PATH}")
