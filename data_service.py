from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")
_db_lock = threading.Lock()


# ── helpers ──────────────────────────────────────────────────────────────────

def _query(sql: str, params: tuple = ()) -> List[Dict]:
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
        finally:
            con.close()


def _execute(sql: str, params: tuple = ()):
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        try:
            con.execute(sql, params)
            con.commit()
        finally:
            con.close()


def _db_ready() -> bool:
    try:
        rows = _query("SELECT COUNT(*) AS n FROM funds")
        return rows[0]["n"] > 0
    except Exception:
        return False


# ── public API ────────────────────────────────────────────────────────────────

def get_top_funds_current_period() -> dict:
    """Top 5 fondos por rentabilidad del último mes (1m del CSV)."""
    if not _db_ready():
        return {"error": "Base de datos vacía. Importa el CSV primero."}

    rows = _query("""
        SELECT f.name, f.isin,
               f.category_mediolanum  AS category,
               f.category_morningstar AS category_ms,
               f.manager,
               p.return_1m, p.return_3m, p.return_ytd, p.return_1a,
               p.updated_at
        FROM funds f
        JOIN period_returns p ON f.isin = p.isin
        WHERE p.return_1m IS NOT NULL
        ORDER BY p.return_1m DESC
        LIMIT 5
    """)

    if not rows:
        return {"error": "Sin datos de rentabilidad mensual."}

    total = _query("SELECT COUNT(*) AS n FROM period_returns WHERE return_1m IS NOT NULL")
    total_n = total[0]["n"] if total else 0
    updated = rows[0]["updated_at"]

    return {
        "period": "1m",
        "period_label": "Último mes · Marzo 2026",
        "is_current": True,
        "data_source": "Morningstar",
        "updated_at": updated,
        "top5": _build_top5(rows, total_n, mode="period"),
    }


def get_top_funds_for_year(year: int) -> dict:
    """Top 5 fondos por rentabilidad anual para el año dado."""
    if not _db_ready():
        return {"error": "Base de datos vacía. Importa el CSV primero."}

    available = get_available_years()
    if year not in available:
        return {"error": f"Año {year} no disponible. Años con datos: {available}"}

    # check cache
    cached = _get_cache(str(year))
    if cached:
        return cached

    rows = _query("""
        SELECT f.name, f.isin,
               f.category_mediolanum  AS category,
               f.category_morningstar AS category_ms,
               f.manager,
               a.return_pct AS return_1m,
               a.volatility_pct
        FROM annual_returns a
        JOIN funds f ON f.isin = a.isin
        WHERE a.year = ? AND a.return_pct IS NOT NULL
        ORDER BY a.return_pct DESC
        LIMIT 5
    """, (year,))

    if not rows:
        return {"error": f"Sin datos para {year}"}

    total = _query(
        "SELECT COUNT(*) AS n FROM annual_returns WHERE year=? AND return_pct IS NOT NULL",
        (year,)
    )
    total_n = total[0]["n"] if total else 0

    result = {
        "period": str(year),
        "period_label": f"Año {year}",
        "is_current": False,
        "data_source": "Morningstar",
        "top5": _build_top5(rows, total_n, mode="annual"),
    }
    _save_cache(str(year), result)
    return result


def get_available_years() -> List[int]:
    rows = _query(
        "SELECT DISTINCT year FROM annual_returns WHERE return_pct IS NOT NULL ORDER BY year DESC"
    )
    return [r["year"] for r in rows]


def get_import_status() -> dict:
    try:
        fund_count = _query("SELECT COUNT(*) AS n FROM funds")[0]["n"]
        years = get_available_years()
        last_row = _query("SELECT MAX(updated_at) AS t FROM period_returns")
        last_update = last_row[0]["t"] if last_row else None
        return {
            "status": "ok" if fund_count > 0 else "empty",
            "fund_count": fund_count,
            "available_years": years,
            "last_update": last_update,
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "fund_count": 0}


# ── internal helpers ─────────────────────────────────────────────────────────

def _build_top5(rows: List[Dict], total_n: int, mode: str) -> List[Dict]:
    result = []
    for rank, row in enumerate(rows, 1):
        entry: Dict = {
            "rank":               rank,
            "isin":               row["isin"],
            "name":               row["name"],
            "category":           row.get("category") or row.get("category_ms") or "—",
            "category_ms":        row.get("category_ms", ""),
            "manager":            row.get("manager", ""),
            "return_pct":         row["return_1m"],
            "total_funds_analyzed": total_n,
        }
        if mode == "period":
            entry["return_3m"]  = row.get("return_3m")
            entry["return_1a"]  = row.get("return_1a")
        else:
            entry["volatility"] = row.get("volatility_pct")
        result.append(entry)
    return result


def _get_cache(period: str) -> Optional[dict]:
    rows = _query("SELECT data FROM top5_year_cache WHERE period=?", (period,))
    return json.loads(rows[0]["data"]) if rows else None


def _save_cache(period: str, data: dict):
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        try:
            con.execute(
                "INSERT OR REPLACE INTO top5_year_cache (period,data,computed_at) VALUES (?,?,?)",
                (period, json.dumps(data), datetime.utcnow().isoformat()),
            )
            con.commit()
        finally:
            con.close()
