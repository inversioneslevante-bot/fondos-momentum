import json, os, sqlite3
from datetime import date, datetime
from typing import Dict, List

MONTH_ES = {
    "01":"enero","02":"febrero","03":"marzo","04":"abril",
    "05":"mayo","06":"junio","07":"julio","08":"agosto",
    "09":"septiembre","10":"octubre","11":"noviembre","12":"diciembre",
}

def _month_label(ym: str) -> str:
    """'2026-05' → 'Mayo 2026'"""
    try:
        y, m = ym.split("-")
        return f"{MONTH_ES[m].capitalize()} {y}"
    except Exception:
        return ym

def _month_date_range(ym: str) -> str:
    """'2026-03' → '1 marzo 2026 – 31 marzo 2026'"""
    try:
        import calendar
        y, m = int(ym.split("-")[0]), int(ym.split("-")[1])
        last_day = calendar.monthrange(y, m)[1]
        mes = MONTH_ES[f"{m:02d}"]
        return f"1 {mes} {y} – {last_day} {mes} {y}"
    except Exception:
        return ym

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


def get_last_complete_month() -> str:
    """
    Return the last year_month that has near-complete fund coverage.
    If the most recent month has < 95% of funds compared to the previous month,
    it is considered a partial/incomplete month and we fall back to the previous one.
    """
    rows = _q("""
        SELECT year_month, COUNT(*) as n
        FROM monthly_nav WHERE return_pct IS NOT NULL
        GROUP BY year_month ORDER BY year_month DESC LIMIT 3
    """)
    if not rows:
        return None
    latest = rows[0]
    if len(rows) >= 2:
        prev = rows[1]
        if prev["n"] > 0 and latest["n"] < prev["n"] * 0.95:
            return prev["year_month"]  # latest is partial, use previous
    return latest["year_month"]


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

    # Use last COMPLETE month (not partial current month)
    complete_ym = get_last_complete_month()
    latest_ym   = _q("SELECT MAX(year_month) AS m FROM monthly_nav WHERE return_pct IS NOT NULL")
    latest_ym   = latest_ym[0]["m"] if latest_ym and latest_ym[0]["m"] else None

    is_partial = (latest_ym and complete_ym and latest_ym != complete_ym)
    ref_ym = complete_ym or latest_ym

    label = f"Último mes completo · {_month_label(ref_ym)}" if ref_ym else "Período actual"
    date_range = _month_date_range(ref_ym) if ref_ym else ""

    return {
        "period":           "1m",
        "period_label":     label,
        "date_range":       date_range,
        "latest_month":     ref_ym,
        "partial_month":    latest_ym if is_partial else None,
        "is_current":       True,
        "data_source":      "Morningstar",
        "morningstar_note": (
            "Rentabilidades de mes completo (del 1 al último día del mes). "
            "Para comparar en Morningstar, busca el historial de rentabilidades "
            "mensuales del fondo, NO la rentabilidad rolling '1 mes'."
        ),
        "top5": _build_top5(rows, total),
    }


def get_top_funds_for_year(year: int) -> dict:
    today = date.today()
    if year < today.year - 10 or year > today.year:
        return {"error": f"Año fuera de rango"}

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
        "period":      str(year),
        "period_label": f"Año {year}",
        "date_range":  f"1 enero {year} – 31 diciembre {year}",
        "is_current":  (year == today.year),
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
        fc      = _q("SELECT COUNT(*) AS n FROM funds")[0]["n"]
        yrs     = get_available_years()
        last    = _q("SELECT MAX(updated_at) AS t FROM period_returns")[0]["t"]
        nav     = _q("SELECT COUNT(DISTINCT isin) AS n FROM monthly_nav WHERE return_pct IS NOT NULL")[0]["n"]
        latest  = _q("SELECT MAX(year_month) AS m FROM monthly_nav WHERE return_pct IS NOT NULL")[0]["m"]
        complete = get_last_complete_month()
        return {
            "status":           "ok" if fc > 0 else "empty",
            "fund_count":       fc,
            "available_years":  yrs,
            "last_update":      last,
            "funds_with_nav":   nav,
            "latest_month":     latest,
            "complete_month":   complete,
        }
    except Exception as e:
        return {"status": "error", "fund_count": 0, "message": str(e)}


def get_dashboard_chart_data() -> dict:
    """
    Returns monthly return history (last 13 months) for the current top-5
    selection and the benchmark average.
    """
    top5 = _q("""
        SELECT f.isin, f.name, p.return_1m AS signal_ret
        FROM funds f JOIN period_returns p USING (isin)
        WHERE p.return_1m IS NOT NULL
        ORDER BY p.return_1m DESC LIMIT 5
    """)
    if not top5:
        return {"error": "Sin datos"}

    months_rows = _q(
        "SELECT DISTINCT year_month FROM monthly_nav "
        "WHERE return_pct IS NOT NULL ORDER BY year_month DESC LIMIT 13"
    )
    months = sorted(r["year_month"] for r in months_rows)

    if not months:
        return {"error": "Sin datos NAV"}

    top5_isins   = [f["isin"] for f in top5]
    placeholders = ",".join("?" * len(top5_isins))

    nav_rows = _q(
        f"SELECT isin, year_month, return_pct FROM monthly_nav "
        f"WHERE isin IN ({placeholders}) AND year_month >= ? AND return_pct IS NOT NULL",
        tuple(top5_isins) + (months[0],),
    )
    fund_hist: Dict[str, Dict[str, float]] = {}
    for r in nav_rows:
        fund_hist.setdefault(r["isin"], {})[r["year_month"]] = r["return_pct"]

    bench_rows = _q(
        "SELECT year_month, AVG(return_pct) AS avg_ret FROM monthly_nav "
        "WHERE year_month >= ? AND return_pct IS NOT NULL GROUP BY year_month",
        (months[0],),
    )
    bench_map = {r["year_month"]: round(r["avg_ret"], 3) for r in bench_rows}

    funds_out = []
    for f in top5:
        hist = fund_hist.get(f["isin"], {})
        funds_out.append({
            "isin":          f["isin"],
            "name":          f["name"],
            "signal_return": round(f["signal_ret"], 2),
            "history":       [{"month": m, "return": hist.get(m)} for m in months],
        })

    complete_ym = get_last_complete_month()
    latest_ym   = months[-1]
    sel_month   = months[-2] if len(months) >= 2 else months[-1]

    return {
        "latest_month":    latest_ym,
        "complete_month":  complete_ym,
        "latest_label":    _month_label(complete_ym or latest_ym),
        "selection_month": sel_month,
        "selection_label": _month_label(sel_month),
        "months":          months,
        "funds":           funds_out,
        "benchmark":       [{"month": m, "return": bench_map.get(m)} for m in months],
    }
