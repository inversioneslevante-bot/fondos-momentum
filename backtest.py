"""
Unified momentum backtest — all 3 strategies in one optimized pass.

  A  Annual basic:    contributions earn ~half the year's return.
  B  Annual compound: sell all + contribution → invest total each year.
  C  Monthly compound: same as B with real monthly Morningstar NAV data.

Entry point: run_all(monthly_contribution) → {strategy_a, strategy_b, strategy_c, monthly}
"""
import os, sqlite3
from typing import Dict, List

DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cache.db"),
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _q(sql: str, params: tuple = ()) -> List[Dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def _summarise(port, bench, invested, periods, monthly_c, *, is_monthly=False):
    profit  = port - invested
    n       = len(periods)
    annexp  = 12 / n if (is_monthly and n) else (1 / n if n else 1)
    cagr    = ((port / invested) ** annexp - 1) * 100 if invested and n else 0
    pos     = sum(1 for p in periods if p.get("ret", 0) > 0)
    best    = max(periods, key=lambda p: p.get("ret", -999), default=None)
    worst   = min(periods, key=lambda p: p.get("ret",  999), default=None)

    cum = peak = 1.0; max_dd = 0.0
    for p in periods:
        cum *= 1 + p.get("ret", 0) / 100
        if cum > peak:
            peak = cum
        dd = (peak - cum) / peak * 100 if peak else 0
        if dd > max_dd:
            max_dd = dd

    d = {
        "monthly_contribution": monthly_c,
        "total_invested":        round(invested, 2),
        "final_value":           round(port,     2),
        "profit":                round(profit,   2),
        "total_return_pct":      round(profit / invested * 100, 2) if invested else 0,
        "cagr_pct":              round(cagr,     2),
        "bench_final":           round(bench,    2),
        "bench_profit":          round(bench - invested, 2),
        "bench_return_pct":      round((bench - invested) / invested * 100, 2) if invested else 0,
        "max_drawdown_pct":      round(max_dd, 2),
        "strategy_beats_bench":  port > bench,
        "best_period":  {"period": best["period"],  "return": best["ret"]}  if best  else None,
        "worst_period": {"period": worst["period"], "return": worst["ret"]} if worst else None,
    }
    if is_monthly:
        d.update({
            "n_months":        n,
            "positive_months": pos,
            "negative_months": n - pos,
        })
    else:
        d.update({
            "n_years":        n,
            "positive_years": pos,
            "negative_years": n - pos,
        })
    return d


# ── strategies A + B  (annual data) ──────────────────────────────────────────

def _annual_both(monthly_c: float, start_year: int = 2016, end_year: int = None):
    from datetime import date as _date
    if end_year is None:
        end_year = _date.today().year - 1  # last fully completed year
    annual_c = monthly_c * 12

    # One bulk query — all years including signal year (start_year-1)
    rows = _q("""
        SELECT a.year, a.isin, a.return_pct,
               f.name, f.category_mediolanum AS cat, f.manager
        FROM annual_returns a JOIN funds f USING (isin)
        WHERE a.year BETWEEN ? AND ? AND a.return_pct IS NOT NULL
        ORDER BY a.year, a.return_pct DESC
    """, (start_year - 1, end_year))

    by_year: Dict[int, List[Dict]] = {}
    for r in rows:
        by_year.setdefault(r["year"], []).append(r)

    portA = portB = benchA = benchB = invested = 0.0
    periA: List[Dict] = []
    periB: List[Dict] = []

    for yr in range(start_year, end_year + 1):
        if yr - 1 not in by_year or yr not in by_year:
            continue

        top5     = by_year[yr - 1][:5]
        curr_lut = {r["isin"]: r["return_pct"] for r in by_year[yr]}
        actual   = [curr_lut[f["isin"]] for f in top5 if f["isin"] in curr_lut]
        if not actual:
            continue

        sr = sum(actual) / len(actual)
        br = sum(r["return_pct"] for r in by_year[yr]) / len(by_year[yr])

        # A: existing capital earns full return; new contributions earn ~half
        portA  = portA  * (1 + sr / 100) + annual_c * (1 + sr / 200)
        benchA = benchA * (1 + br / 100) + annual_c * (1 + br / 200)
        # B: sell everything + contribution → invest all at full return
        portB  = (portB  + annual_c) * (1 + sr / 100)
        benchB = (benchB + annual_c) * (1 + br / 100)

        invested += annual_c
        funds_det = [{
            "isin": f["isin"], "name": f["name"], "category": f["cat"],
            "signal_return": round(f["return_pct"], 2),
            "actual_return": round(curr_lut[f["isin"]], 2) if f["isin"] in curr_lut else None,
        } for f in top5]

        base = {
            "year": yr, "period": str(yr), "ret": sr,
            "strategy_return":  round(sr, 2),
            "benchmark_return": round(br, 2),
            "total_invested":   round(invested, 2),
            "n_funds":          len(actual),
            "funds_selected":   funds_det,
        }
        periA.append({**base, "portfolio_value": round(portA, 2),
                      "bench_value": round(benchA, 2),
                      "profit_vs_cost": round(portA - invested, 2)})
        periB.append({**base, "portfolio_value": round(portB, 2),
                      "bench_value": round(benchB, 2),
                      "profit_vs_cost": round(portB - invested, 2)})

    # Partial current year — signal = end_year top-5, return = latest period_returns
    latest_ym_row = _q(
        "SELECT MAX(year_month) AS m FROM monthly_nav WHERE return_pct IS NOT NULL"
    )
    latest_ym = latest_ym_row[0]["m"] if latest_ym_row and latest_ym_row[0]["m"] else None

    top5_signal = _q("""
        SELECT a.isin, a.return_pct AS sig_ret,
               f.name, f.category_mediolanum AS cat, p.return_1m
        FROM annual_returns a JOIN funds f USING (isin)
        LEFT JOIN period_returns p USING (isin)
        WHERE a.year = ? AND a.return_pct IS NOT NULL
        ORDER BY a.return_pct DESC LIMIT 5
    """, (end_year,))
    cur_1m = [r["return_1m"] for r in top5_signal if r["return_1m"] is not None]
    b1m_row = _q("SELECT AVG(return_1m) AS a FROM period_returns WHERE return_1m IS NOT NULL")
    b1m = (b1m_row[0]["a"] or 0.0) if b1m_row else 0.0

    if cur_1m and latest_ym:
        r1m = sum(cur_1m) / len(cur_1m)
        portA  *= (1 + r1m / 100);  benchA  *= (1 + b1m / 100)
        portB   = (portB  + monthly_c) * (1 + r1m / 100)
        benchB  = (benchB + monthly_c) * (1 + b1m / 100)
        invested += monthly_c

        # Human-readable label: "2026 (May)"
        MONTHS_SHORT = {
            "01":"Ene","02":"Feb","03":"Mar","04":"Abr","05":"May","06":"Jun",
            "07":"Jul","08":"Ago","09":"Sep","10":"Oct","11":"Nov","12":"Dic",
        }
        ym_parts = latest_ym.split("-")
        partial_label = f"{ym_parts[0]} ({MONTHS_SHORT.get(ym_parts[1], ym_parts[1])})"

        partial_funds = [{
            "isin": r["isin"], "name": r["name"], "category": r["cat"],
            "signal_return": round(r["sig_ret"], 2),
            "actual_return": round(r["return_1m"], 2) if r["return_1m"] else None,
        } for r in top5_signal]

        pbase = {
            "year": partial_label, "period": partial_label, "ret": r1m,
            "strategy_return": round(r1m, 2), "benchmark_return": round(b1m, 2),
            "total_invested": round(invested, 2), "n_funds": len(cur_1m),
            "funds_selected": partial_funds,
        }
        periA.append({**pbase, "portfolio_value": round(portA, 2),
                      "bench_value": round(benchA, 2),
                      "profit_vs_cost": round(portA - invested, 2)})
        periB.append({**pbase, "portfolio_value": round(portB, 2),
                      "bench_value": round(benchB, 2),
                      "profit_vs_cost": round(portB - invested, 2)})

    return {
        "strategy_a": {"summary": _summarise(portA, benchA, invested, periA, monthly_c), "yearly": periA},
        "strategy_b": {"summary": _summarise(portB, benchB, invested, periB, monthly_c), "yearly": periB},
    }


# ── strategy C  (real monthly NAV data) ──────────────────────────────────────

def _monthly_compound(monthly_c: float, start_ym: str = None):
    # ONE bulk query instead of ~860 individual queries
    rows = _q("""
        SELECT m.isin, m.year_month, m.return_pct,
               f.name, f.category_mediolanum AS cat
        FROM monthly_nav m JOIN funds f USING (isin)
        WHERE m.return_pct IS NOT NULL
        ORDER BY m.year_month, m.return_pct DESC
    """)

    if not rows:
        return {"error": "Sin datos mensuales."}

    by_month: Dict[str, List[Dict]] = {}
    for r in rows:
        by_month.setdefault(r["year_month"], []).append(r)

    months = sorted(by_month)

    # Apply start filter — keep start_ym as the first *signal* month
    if start_ym:
        months = [m for m in months if m >= start_ym]

    if len(months) < 3:
        return {"error": "Datos insuficientes para el período seleccionado. Elige una fecha de inicio anterior."}

    port = bench = invested = 0.0
    periods: List[Dict] = []

    for i in range(1, len(months)):
        sig = months[i - 1]
        cur = months[i]

        top5      = by_month[sig][:5]
        curr_lut  = {r["isin"]: r["return_pct"] for r in by_month[cur]}
        bench_all = [r["return_pct"] for r in by_month[cur]]
        actual    = [curr_lut[f["isin"]] for f in top5 if f["isin"] in curr_lut]

        if not actual:
            continue

        sr = sum(actual)     / len(actual)
        br = sum(bench_all)  / len(bench_all)

        port  = (port  + monthly_c) * (1 + sr / 100)
        bench = (bench + monthly_c) * (1 + br / 100)
        invested += monthly_c

        periods.append({
            "month": cur, "signal_month": sig, "period": cur, "ret": sr,
            "strategy_return":  round(sr, 3),
            "benchmark_return": round(br, 3),
            "portfolio_value":  round(port,      2),
            "bench_value":      round(bench,     2),
            "total_invested":   round(invested,  2),
            "profit":           round(port - invested, 2),
            "n_funds":          len(actual),
            "top5": [{
                "isin": f["isin"], "name": f["name"], "category": f["cat"],
                "signal_return": round(f["return_pct"], 2),
                "actual_return": round(curr_lut[f["isin"]], 2) if f["isin"] in curr_lut else None,
            } for f in top5],
        })

    s = _summarise(port, bench, invested, periods, monthly_c, is_monthly=True)
    s["date_range"] = f"{periods[0]['month']} → {periods[-1]['month']}" if periods else ""
    return {"summary": s, "monthly": periods}


# ── public entry ──────────────────────────────────────────────────────────────

def run_all(monthly_contribution: float = 1_000.0,
            start_year: int = None,
            start_month: int = 1) -> dict:
    from datetime import date as _date

    # Default: earliest year with annual data (≈ 2016)
    if start_year is None:
        earliest = _q(
            "SELECT MIN(year) AS y FROM annual_returns WHERE return_pct IS NOT NULL"
        )
        start_year = earliest[0]["y"] if earliest and earliest[0]["y"] else 2016

    start_month = max(1, min(12, int(start_month)))
    start_ym = f"{start_year}-{start_month:02d}"

    ab = _annual_both(monthly_contribution, start_year=start_year)
    c  = _monthly_compound(monthly_contribution, start_ym=start_ym)

    return {
        "strategy_a":  ab["strategy_a"],
        "strategy_b":  ab["strategy_b"],
        "strategy_c":  c,
        "monthly":     monthly_contribution,
        "start_year":  start_year,
        "start_month": start_month,
        "start_ym":    start_ym,
    }


if __name__ == "__main__":
    r = run_all(1_000)
    for k in ("strategy_a", "strategy_b", "strategy_c"):
        s = r[k].get("summary", r[k])
        val = s.get("final_value", 0)
        ret = s.get("total_return_pct", 0)
        cag = s.get("cagr_pct", 0)
        print(f"  {k}: €{val:>10,.0f}  {ret:>+.1f}%  CAGR {cag:>+.1f}%")
