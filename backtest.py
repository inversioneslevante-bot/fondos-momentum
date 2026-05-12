"""
Backtester: momentum annual strategy
  - Each year: invest in the TOP 5 funds of the PREVIOUS year
  - Rebalance at the start of each year
  - Monthly contribution throughout the year (mid-year approximation)

NOTE: The CSV provides annual returns (2013-2025), so the simulation
uses ANNUAL rebalancing — a faithful approximation of the monthly
strategy the user described. The direction and magnitude of returns
are real Morningstar data; only rebalancing frequency differs.
"""

import sqlite3
import os
import math
from typing import Dict, List, Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")


def _q(sql: str, params: tuple = ()) -> List[Dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def run_backtest(
    monthly_contribution: float = 1_000.0,
    start_year: int = 2016,
    end_year:   int = 2025,
) -> dict:
    """
    Simulate investing monthly_contribution every month in the top-5
    funds ranked by previous year's annual return.

    Capital model:
      - Portfolio at start of year Y = portfolio × (1 + strategy_return_Y-1)
                                       + (year_contribution × mid-year factor)
    """

    portfolio   = 0.0   # strategy portfolio (€)
    bench_port  = 0.0   # equal-weight benchmark (all funds)
    invested    = 0.0   # cumulative cash invested (€)
    annual_c    = monthly_contribution * 12  # €/year
    yearly      = []

    for year in range(start_year, end_year + 1):
        signal = year - 1

        # ── 1. Pick top-5 from signal year ──────────────────────────────
        top5_signal = _q("""
            SELECT a.isin, a.return_pct AS sig_ret,
                   f.name, f.category_mediolanum AS cat,
                   f.manager
            FROM annual_returns a
            JOIN funds f ON f.isin = a.isin
            WHERE a.year = ? AND a.return_pct IS NOT NULL
            ORDER BY a.return_pct DESC
            LIMIT 5
        """, (signal,))

        if not top5_signal:
            continue  # no data for signal year

        # ── 2. Get their ACTUAL return in 'year' ────────────────────────
        actual = []
        funds_detail = []
        for f in top5_signal:
            row = _q("SELECT return_pct FROM annual_returns WHERE isin=? AND year=?",
                     (f["isin"], year))
            ret = row[0]["return_pct"] if row else None
            funds_detail.append({
                "isin":          f["isin"],
                "name":          f["name"],
                "category":      f["cat"],
                "manager":       f["manager"],
                "signal_return": round(f["sig_ret"], 2),
                "actual_return": round(ret, 2) if ret is not None else None,
            })
            if ret is not None:
                actual.append(ret)

        if not actual:
            continue

        strat_ret = sum(actual) / len(actual)   # avg return of the 5 funds

        # ── 3. Benchmark: equal-weight ALL funds ────────────────────────
        bench_row = _q("""
            SELECT AVG(return_pct) AS avg_r
            FROM annual_returns
            WHERE year = ? AND return_pct IS NOT NULL
        """, (year,))
        bench_ret = bench_row[0]["avg_r"] or 0.0

        # ── 4. Update portfolios ─────────────────────────────────────────
        # Existing capital grows by annual return
        port_grown  = portfolio  * (1 + strat_ret  / 100)
        bench_grown = bench_port * (1 + bench_ret / 100)

        # New contributions: assume invested evenly through year
        # ⇒ average exposure = 6 months → half-year return factor
        contrib_factor_s = 1 + (strat_ret  / 100) * 0.5
        contrib_factor_b = 1 + (bench_ret / 100) * 0.5

        portfolio  = port_grown  + annual_c * contrib_factor_s
        bench_port = bench_grown + annual_c * contrib_factor_b
        invested  += annual_c

        yearly.append({
            "year":             year,
            "strategy_return":  round(strat_ret,  2),
            "benchmark_return": round(bench_ret,  2),
            "portfolio_value":  round(portfolio,  2),
            "bench_value":      round(bench_port, 2),
            "total_invested":   round(invested,   2),
            "profit_vs_cost":   round(portfolio - invested, 2),
            "n_funds":          len(actual),
            "funds_selected":   funds_detail,
        })

    # ── 5. Apply current partial period (2026) ───────────────────────────
    # Signal = 2025 top-5; apply their 1m return (March 2026)
    top5_2025 = _q("""
        SELECT a.isin, a.return_pct AS sig_ret,
               f.name, f.category_mediolanum AS cat, f.manager,
               p.return_1m
        FROM annual_returns a
        JOIN funds f ON f.isin = a.isin
        LEFT JOIN period_returns p ON p.isin = a.isin
        WHERE a.year = 2025 AND a.return_pct IS NOT NULL
        ORDER BY a.return_pct DESC
        LIMIT 5
    """)

    current_1m = [r["return_1m"] for r in top5_2025 if r["return_1m"] is not None]
    current_ret = sum(current_1m) / len(current_1m) if current_1m else 0.0

    # Benchmark 1m (all funds average)
    bench_1m_row = _q("SELECT AVG(return_1m) AS avg FROM period_returns WHERE return_1m IS NOT NULL")
    bench_1m = bench_1m_row[0]["avg"] or 0.0

    # No new contribution for partial month (March only)
    portfolio  *= (1 + current_ret / 100)
    bench_port *= (1 + bench_1m   / 100)

    current_period = {
        "year":             "2026 (Mar)",
        "strategy_return":  round(current_ret, 2),
        "benchmark_return": round(bench_1m,    2),
        "portfolio_value":  round(portfolio,   2),
        "bench_value":      round(bench_port,  2),
        "total_invested":   round(invested,    2),
        "profit_vs_cost":   round(portfolio - invested, 2),
        "n_funds":          len(current_1m),
        "funds_selected": [{
            "isin":          r["isin"],
            "name":          r["name"],
            "category":      r["cat"],
            "manager":       r["manager"],
            "signal_return": round(r["sig_ret"], 2),
            "actual_return": round(r["return_1m"], 2) if r["return_1m"] else None,
        } for r in top5_2025],
    }
    yearly.append(current_period)

    # ── 6. Summary ────────────────────────────────────────────────────────
    profit = portfolio - invested
    total_return_pct = profit / invested * 100 if invested > 0 else 0
    n_full_years = len(yearly) - 1  # exclude partial 2026
    cagr = ((portfolio / invested) ** (1 / max(n_full_years, 1)) - 1) * 100 if invested > 0 else 0

    full_years = [y for y in yearly if isinstance(y["year"], int)]
    pos_years = sum(1 for y in full_years if y["strategy_return"] > 0)
    neg_years = len(full_years) - pos_years
    best  = max(full_years, key=lambda y: y["strategy_return"], default=None)
    worst = min(full_years, key=lambda y: y["strategy_return"], default=None)

    # Drawdown: max peak-to-trough in portfolio values
    peak = 0.0
    max_dd = 0.0
    for y in full_years:
        v = y["portfolio_value"]
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    return {
        "summary": {
            "monthly_contribution":  monthly_contribution,
            "total_invested":        round(invested, 2),
            "final_value":           round(portfolio, 2),
            "profit":                round(profit, 2),
            "total_return_pct":      round(total_return_pct, 2),
            "cagr_pct":              round(cagr, 2),
            "bench_final":           round(bench_port, 2),
            "bench_profit":          round(bench_port - invested, 2),
            "bench_return_pct":      round((bench_port - invested) / invested * 100, 2) if invested else 0,
            "n_years":               n_full_years,
            "positive_years":        pos_years,
            "negative_years":        neg_years,
            "max_drawdown_pct":      round(max_dd, 2),
            "strategy_beats_bench":  portfolio > bench_port,
        },
        "best_year":  {"year": best["year"],  "return": best["strategy_return"]}  if best  else None,
        "worst_year": {"year": worst["year"], "return": worst["strategy_return"]} if worst else None,
        "yearly": yearly,
    }


def run_backtest_compound(
    monthly_contribution: float = 1_000.0,
    start_year: int = 2016,
    end_year:   int = 2025,
) -> dict:
    """
    ESTRATEGIA FULL COMPOUND:
    Al inicio de cada año, vendes TODO lo que tienes, añades la aportación
    anual (12 × mensual) y lo reinviertes entero en los 5 mejores fondos
    del año anterior. Todo el capital compone junto en cada período.

    Fórmula: portfolio = (portfolio + aportación_anual) × (1 + retorno/100)

    Diferencia clave vs run_backtest():
    - Toda la cartera (acumulada + nueva aportación) gana/pierde el retorno
      completo del año → mayor efecto compound en años buenos, mayor caída
      en años malos.
    """

    portfolio   = 0.0
    bench_port  = 0.0
    invested    = 0.0
    annual_c    = monthly_contribution * 12
    yearly      = []

    for year in range(start_year, end_year + 1):
        signal = year - 1

        top5_signal = _q("""
            SELECT a.isin, a.return_pct AS sig_ret,
                   f.name, f.category_mediolanum AS cat, f.manager
            FROM annual_returns a
            JOIN funds f ON f.isin = a.isin
            WHERE a.year = ? AND a.return_pct IS NOT NULL
            ORDER BY a.return_pct DESC
            LIMIT 5
        """, (signal,))

        if not top5_signal:
            continue

        actual = []
        funds_detail = []
        for f in top5_signal:
            row = _q("SELECT return_pct FROM annual_returns WHERE isin=? AND year=?",
                     (f["isin"], year))
            ret = row[0]["return_pct"] if row else None
            funds_detail.append({
                "isin":          f["isin"],
                "name":          f["name"],
                "category":      f["cat"],
                "manager":       f["manager"],
                "signal_return": round(f["sig_ret"], 2),
                "actual_return": round(ret, 2) if ret is not None else None,
            })
            if ret is not None:
                actual.append(ret)

        if not actual:
            continue

        strat_ret = sum(actual) / len(actual)

        bench_row = _q("""
            SELECT AVG(return_pct) AS avg_r
            FROM annual_returns
            WHERE year = ? AND return_pct IS NOT NULL
        """, (year,))
        bench_ret = bench_row[0]["avg_r"] or 0.0

        # ── FULL COMPOUND ────────────────────────────────────────────────
        # Sell everything + add annual contribution → invest total → apply return
        portfolio  = (portfolio  + annual_c) * (1 + strat_ret / 100)
        bench_port = (bench_port + annual_c) * (1 + bench_ret / 100)
        invested  += annual_c

        yearly.append({
            "year":             year,
            "strategy_return":  round(strat_ret, 2),
            "benchmark_return": round(bench_ret, 2),
            "portfolio_value":  round(portfolio, 2),
            "bench_value":      round(bench_port, 2),
            "total_invested":   round(invested, 2),
            "profit_vs_cost":   round(portfolio - invested, 2),
            "n_funds":          len(actual),
            "funds_selected":   funds_detail,
        })

    # ── Partial 2026 (March) — same top-5 signal from 2025 ──────────────
    top5_2025 = _q("""
        SELECT a.isin, a.return_pct AS sig_ret,
               f.name, f.category_mediolanum AS cat, f.manager,
               p.return_1m
        FROM annual_returns a
        JOIN funds f ON f.isin = a.isin
        LEFT JOIN period_returns p ON p.isin = a.isin
        WHERE a.year = 2025 AND a.return_pct IS NOT NULL
        ORDER BY a.return_pct DESC
        LIMIT 5
    """)
    current_1m  = [r["return_1m"] for r in top5_2025 if r["return_1m"] is not None]
    current_ret = sum(current_1m) / len(current_1m) if current_1m else 0.0

    bench_1m_row = _q("SELECT AVG(return_1m) AS avg FROM period_returns WHERE return_1m IS NOT NULL")
    bench_1m = bench_1m_row[0]["avg"] or 0.0

    # March 2026: add 1 month contribution, apply 1m return to everything
    portfolio  = (portfolio  + monthly_contribution) * (1 + current_ret / 100)
    bench_port = (bench_port + monthly_contribution) * (1 + bench_1m   / 100)
    invested  += monthly_contribution  # 1 extra month

    current_period = {
        "year":             "2026 (Mar)",
        "strategy_return":  round(current_ret, 2),
        "benchmark_return": round(bench_1m, 2),
        "portfolio_value":  round(portfolio, 2),
        "bench_value":      round(bench_port, 2),
        "total_invested":   round(invested, 2),
        "profit_vs_cost":   round(portfolio - invested, 2),
        "n_funds":          len(current_1m),
        "funds_selected": [{
            "isin":          r["isin"],
            "name":          r["name"],
            "category":      r["cat"],
            "manager":       r["manager"],
            "signal_return": round(r["sig_ret"], 2),
            "actual_return": round(r["return_1m"], 2) if r["return_1m"] else None,
        } for r in top5_2025],
    }
    yearly.append(current_period)

    # ── Summary ──────────────────────────────────────────────────────────
    profit           = portfolio - invested
    total_return_pct = profit / invested * 100 if invested > 0 else 0
    full_years       = [y for y in yearly if isinstance(y["year"], int)]
    n_full_years     = len(full_years)
    cagr             = ((portfolio / invested) ** (1 / max(n_full_years, 1)) - 1) * 100 if invested > 0 else 0
    pos_years        = sum(1 for y in full_years if y["strategy_return"] > 0)
    neg_years        = len(full_years) - pos_years
    best  = max(full_years, key=lambda y: y["strategy_return"], default=None)
    worst = min(full_years, key=lambda y: y["strategy_return"], default=None)

    # Drawdown on PORTFOLIO VALUE (not just return %) — meaningful here
    # because all capital is at risk each year
    peak = 0.0
    max_dd = 0.0
    for y in full_years:
        v = y["portfolio_value"]
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    return {
        "strategy": "compound",
        "summary": {
            "monthly_contribution":  monthly_contribution,
            "total_invested":        round(invested, 2),
            "final_value":           round(portfolio, 2),
            "profit":                round(profit, 2),
            "total_return_pct":      round(total_return_pct, 2),
            "cagr_pct":              round(cagr, 2),
            "bench_final":           round(bench_port, 2),
            "bench_profit":          round(bench_port - invested, 2),
            "bench_return_pct":      round((bench_port - invested) / invested * 100, 2) if invested else 0,
            "n_years":               n_full_years,
            "positive_years":        pos_years,
            "negative_years":        neg_years,
            "max_drawdown_pct":      round(max_dd, 2),
            "strategy_beats_bench":  portfolio > bench_port,
        },
        "best_year":  {"year": best["year"],  "return": best["strategy_return"]}  if best  else None,
        "worst_year": {"year": worst["year"], "return": worst["strategy_return"]} if worst else None,
        "yearly": yearly,
    }


if __name__ == "__main__":
    result = run_backtest(monthly_contribution=1_000)
    s = result["summary"]
    print(f"\n{'='*60}")
    print(f"  SIMULACIÓN MOMENTUM TOP-5 (aportación €1.000/mes)")
    print(f"{'='*60}")
    print(f"  Capital invertido total : €{s['total_invested']:>12,.0f}")
    print(f"  Valor cartera a hoy     : €{s['final_value']:>12,.0f}")
    print(f"  Ganancia / Pérdida      : €{s['profit']:>+12,.0f}")
    print(f"  Rentabilidad total      : {s['total_return_pct']:>+.1f}%")
    print(f"  CAGR anualizado         : {s['cagr_pct']:>+.1f}%")
    print(f"  Años positivos / negativos: {s['positive_years']} / {s['negative_years']}")
    print(f"  Max. drawdown           : -{s['max_drawdown_pct']:.1f}%")
    print(f"  Benchmark (todos fondos): €{s['bench_final']:>12,.0f} ({s['bench_return_pct']:+.1f}%)")
    print(f"\n  {'Año':<10} {'Retorno strat':>14} {'Bench':>8} {'Cartera':>14} {'Invertido':>12}")
    print(f"  {'-'*62}")
    for y in result["yearly"]:
        print(f"  {str(y['year']):<10} {y['strategy_return']:>+13.1f}% {y['benchmark_return']:>+7.1f}%  €{y['portfolio_value']:>12,.0f}  €{y['total_invested']:>10,.0f}")
    if result["best_year"]:
        print(f"\n  Mejor año : {result['best_year']['year']} ({result['best_year']['return']:+.1f}%)")
    if result["worst_year"]:
        print(f"  Peor año  : {result['worst_year']['year']} ({result['worst_year']['return']:+.1f}%)")
    print()
