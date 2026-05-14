import json, os, sqlite3
from datetime import date, datetime
from typing import Dict, List

DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cache.db"),
)


def _q(sql: str, params: tuple = ()) -> List[Dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def _save(sql: str, params: tuple = ()):
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(sql, params)
        con.commit()
    finally:
        con.close()


# ── Top-5 ranking ─────────────────────────────────────────────────────────────

def _build_top5(rows: List[Dict], total: int) -> List[Dict]:
    return [{
        "rank":                  i + 1,
        "isin":                  r["isin"],
        "name":                  r["name"],
        "category":              r.get("category") or r.get("category_ms") or "—",
        "category_ms":           r.get("category_ms", ""),
        "manager":               r.get("manager", ""),
        "return_pct":            r["return_pct"],
        "return_3m":             r.get("return_3m"),
        "return_1a":             r.get("return_1a"),
        "volatility":            r.get("volatility_pct"),
        "total_funds_analyzed":  total,
    } for i, r in enumerate(rows)]


def get_top_funds_current_period() -> dict:
    rows = _q("""
        SELECT f.isin, f.name, f.category_mediolanum AS category,
               f.category_morningstar AS category_ms, f.manager,
               p.return_1m AS return_pct, p.return_3m, p.return_1a
        FROM funds f JOIN period_returns p USING (isin)
        WHERE p.return_1m IS NOT NULL
        ORDER BY p.return_1m DESC LIMIT 5
    """)
    total_row = _q("SELECT COUNT(*) AS n FROM period_returns WHERE return_1m IS NOT NULL")
    total = total_row[0]["n"] if total_row else 0

    if not rows:
        return {"error": "Sin datos. Importa el CSV primero."}

    return {
        "period": "1m",
        "period_label": "Último mes · Marzo 2026",
        "is_current": True,
        "data_source": "Morningstar",
        "top5": _build_top5(rows, total),
    }


def get_top_funds_for_year(year: int) -> dict:
    today = date.today()
    if year < today.year - 10 or year > today.year:
        return {"error": f"Año fuera de rango"}

    # Use cache
    cached = _q("SELECT data FROM top5_year_cache WHERE period=?", (str(year),))
    if cached:
        return json.loads(cached[0]["data"])

    rows = _q("""
        SELECT f.isin, f.name, f.category_mediolanum AS category,
               f.category_morningstar AS category_ms, f.manager,
               a.return_pct, a.volatility_pct
        FROM annual_returns a JOIN funds f USING (isin)
        WHERE a.year = ? AND a.return_pct IS NOT NULL
        ORDER BY a.return_pct DESC LIMIT 5
    """, (year,))

    if not rows:
        return {"error": f"Sin datos para {year}"}

    total_row = _q(
        "SELECT COUNT(*) AS n FROM annual_returns WHERE year=? AND return_pct IS NOT NULL",
        (year,)
    )
    total = total_row[0]["n"] if total_row else 0

    result = {
        "period": str(year),
        "period_label": f"Año {year}",
        "is_current": (year == today.year),
        "data_source": "Morningstar",
        "top5": _build_top5(rows, total),
    }
    _save(
        "INSERT OR REPLACE INTO top5_year_cache (period, data, computed_at) VALUES (?,?,?)",
        (str(year), json.dumps(result), datetime.utcnow().isoformat()),
    )
    return result


def get_available_years() -> List[int]:
    rows = _q("SELECT DISTINCT year FROM annual_returns WHERE return_pct IS NOT NULL ORDER BY year DESC")
    return [r["year"] for r in rows]


def get_import_status() -> dict:
    try:
        fc   = _q("SELECT COUNT(*) AS n FROM funds")[0]["n"]
        yrs  = get_available_years()
        last = _q("SELECT MAX(updated_at) AS t FROM period_returns")[0]["t"]
        nav  = _q("SELECT COUNT(DISTINCT isin) AS n FROM monthly_nav WHERE return_pct IS NOT NULL")[0]["n"]
        return {
            "status":           "ok" if fc > 0 else "empty",
            "fund_count":       fc,
            "available_years":  yrs,
            "last_update":      last,
            "funds_with_nav":   nav,
        }
    except Exception as e:
        return {"status": "error", "fund_count": 0, "message": str(e)}
