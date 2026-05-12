"""
TRUE monthly momentum backtest using real Morningstar monthly NAV data.

Strategy (exact as requested by user):
  - Each month M: sell everything + add monthly contribution
  - Invest the total in the 5 funds with the BEST return in month M-1
  - Compound month after month

Data: real growth-of-10,000 values from Morningstar Global (2016 onward).
"""

import sqlite3
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")


def _q(sql: str, params: tuple = ()) -> List[Dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def _has_monthly_data() -> bool:
    rows = _q("SELECT COUNT(DISTINCT isin) AS n FROM monthly_nav")
    return rows[0]["n"] >= 5 if rows else False


def run_backtest_monthly(monthly_contribution: float = 1_000.0) -> dict:
    """
    Real monthly compound momentum backtest.
    Returns full month-by-month breakdown.
    """
    if not _has_monthly_data():
        return {"error": "No hay datos mensuales. Ejecuta fetch_monthly_nav.py primero."}

    # ── All months with return data (sorted) ──────────────────────────────
    month_rows = _q("""
        SELECT DISTINCT year_month FROM monthly_nav
        WHERE return_pct IS NOT NULL
        ORDER BY year_month
    """)
    months = [r["year_month"] for r in month_rows]
    if len(months) < 3:
        return {"error": "Datos insuficientes"}

    # ── Fund metadata map ──────────────────────────────────────────────────
    fund_meta = {
        r["isin"]: {"name": r["name"], "category": r["category_mediolanum"], "manager": r["manager"]}
        for r in _q("SELECT isin, name, category_mediolanum, manager FROM funds")
    }

    portfolio   = 0.0
    bench_port  = 0.0
    invested    = 0.0
    monthly_res = []

    # ── Month-by-month loop ────────────────────────────────────────────────
    for i in range(1, len(months)):
        signal_month  = months[i - 1]   # M-1: rank by this
        current_month = months[i]        # M: invest in top-5 of signal_month

        # Top 5 funds by return in signal_month
        top5_signal = _q("""
            SELECT isin, return_pct AS sig_ret
            FROM monthly_nav
            WHERE year_month = ? AND return_pct IS NOT NULL
            ORDER BY return_pct DESC
            LIMIT 5
        """, (signal_month,))

        if not top5_signal:
            continue

        # Their actual returns in current_month
        strat_rets = []
        top5_detail = []
        for f in top5_signal:
            ret_row = _q(
                "SELECT return_pct FROM monthly_nav WHERE isin=? AND year_month=?",
                (f["isin"], current_month)
            )
            actual = ret_row[0]["return_pct"] if ret_row and ret_row[0]["return_pct"] is not None else None
            meta = fund_meta.get(f["isin"], {})
            top5_detail.append({
                "isin":          f["isin"],
                "name":          meta.get("name", f["isin"]),
                "category":      meta.get("category", ""),
                "signal_return": round(f["sig_ret"], 2),
                "actual_return": round(actual, 2) if actual is not None else None,
            })
            if actual is not None:
                strat_rets.append(actual)

        if not strat_rets:
            continue

        strat_ret = sum(strat_rets) / len(strat_rets)

        # Benchmark: equal-weight average of ALL funds in current_month
        bench_row = _q("""
            SELECT AVG(return_pct) AS avg_r
            FROM monthly_nav
            WHERE year_month = ? AND return_pct IS NOT NULL
        """, (current_month,))
        bench_ret = bench_row[0]["avg_r"] or 0.0

        # ── Full compound: sell all + add contribution + invest total ──────
        portfolio  = (portfolio  + monthly_contribution) * (1 + strat_ret / 100)
        bench_port = (bench_port + monthly_contribution) * (1 + bench_ret / 100)
        invested  += monthly_contribution

        monthly_res.append({
            "month":            current_month,
            "signal_month":     signal_month,
            "strategy_return":  round(strat_ret, 3),
            "benchmark_return": round(bench_ret, 3),
            "portfolio_value":  round(portfolio, 2),
            "bench_value":      round(bench_port, 2),
            "total_invested":   round(invested, 2),
            "profit":           round(portfolio - invested, 2),
            "n_funds":          len(strat_rets),
            "top5":             top5_detail,
        })

    if not monthly_res:
        return {"error": "Sin resultados"}

    # ── Summary ────────────────────────────────────────────────────────────
    final_val   = monthly_res[-1]["portfolio_value"]
    bench_final = monthly_res[-1]["bench_value"]
    total_inv   = monthly_res[-1]["total_invested"]
    profit      = final_val - total_inv
    n_months    = len(monthly_res)
    total_ret   = profit / total_inv * 100 if total_inv else 0
    cagr        = ((final_val / total_inv) ** (12 / n_months) - 1) * 100 if total_inv and n_months else 0

    pos_months  = sum(1 for m in monthly_res if m["strategy_return"] > 0)
    neg_months  = n_months - pos_months

    # Drawdown on returns stream (not absolute portfolio value)
    cum = 1.0
    peak_cum = 1.0
    max_dd = 0.0
    for m in monthly_res:
        cum *= (1 + m["strategy_return"] / 100)
        if cum > peak_cum:
            peak_cum = cum
        dd = (peak_cum - cum) / peak_cum * 100
        if dd > max_dd:
            max_dd = dd

    best  = max(monthly_res, key=lambda x: x["strategy_return"])
    worst = min(monthly_res, key=lambda x: x["strategy_return"])

    return {
        "strategy": "monthly_compound",
        "summary": {
            "monthly_contribution": monthly_contribution,
            "total_invested":       round(total_inv, 2),
            "final_value":          round(final_val, 2),
            "profit":               round(profit, 2),
            "total_return_pct":     round(total_ret, 2),
            "cagr_pct":             round(cagr, 2),
            "bench_final":          round(bench_final, 2),
            "bench_profit":         round(bench_final - total_inv, 2),
            "bench_return_pct":     round((bench_final - total_inv) / total_inv * 100, 2) if total_inv else 0,
            "n_months":             n_months,
            "positive_months":      pos_months,
            "negative_months":      neg_months,
            "max_drawdown_pct":     round(max_dd, 2),
            "strategy_beats_bench": final_val > bench_final,
            "date_range":           f"{monthly_res[0]['month']} → {monthly_res[-1]['month']}",
        },
        "best_month":  {"month": best["month"],  "return": best["strategy_return"]},
        "worst_month": {"month": worst["month"], "return": worst["strategy_return"]},
        "monthly":     monthly_res,
    }


if __name__ == "__main__":
    r = run_backtest_monthly(1000)
    s = r["summary"]
    print(f"\n{'='*65}")
    print(f"  SIMULACIÓN MENSUAL REAL — datos Morningstar — €1.000/mes")
    print(f"{'='*65}")
    print(f"  Período           : {s['date_range']}")
    print(f"  Meses simulados   : {s['n_months']}")
    print(f"  Capital invertido : €{s['total_invested']:>12,.0f}")
    print(f"  Valor hoy         : €{s['final_value']:>12,.0f}")
    print(f"  Ganancia          : €{s['profit']:>+12,.0f}")
    print(f"  Rentabilidad total: {s['total_return_pct']:>+.1f}%")
    print(f"  CAGR anualizado   : {s['cagr_pct']:>+.1f}%")
    print(f"  Meses positivos   : {s['positive_months']} / {s['n_months']}")
    print(f"  Max drawdown      : -{s['max_drawdown_pct']:.1f}%")
    print(f"  Benchmark (todos) : €{s['bench_final']:>12,.0f} ({s['bench_return_pct']:+.1f}%)")
    print(f"  Supera benchmark  : {'✅ SÍ' if s['strategy_beats_bench'] else '❌ NO'}")
    if r.get("best_month"):
        print(f"\n  Mejor mes  : {r['best_month']['month']}  ({r['best_month']['return']:+.2f}%)")
    if r.get("worst_month"):
        print(f"  Peor mes   : {r['worst_month']['month']}  ({r['worst_month']['return']:+.2f}%)")
    print(f"\n  Últimos 6 meses:")
    for m in r["monthly"][-6:]:
        funds_str = ", ".join(t["isin"] for t in m["top5"][:3]) + "…"
        print(f"    {m['month']}  {m['strategy_return']:>+.2f}%  cartera: €{m['portfolio_value']:>10,.0f}  [{funds_str}]")
