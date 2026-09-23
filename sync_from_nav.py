"""
Sync period_returns from monthly_nav data.

Recomputes return_1m/3m/6m/ytd/1a as compound monthly returns using the
real Morningstar NAV data already stored in monthly_nav.

Called:
  - On app startup if data is stale
  - By APScheduler daily at 07:00
  - Via POST /api/sync-now
"""
import logging
import os
import sqlite3
from collections import defaultdict

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cache.db"),
)


def _get_complete_month(con) -> str:
    """
    Return the last year_month with near-complete fund coverage (≥95% of
    the previous month). Always excludes the current calendar month, which
    is in-progress and may contain partial NAV data.
    """
    from datetime import date as _date
    today = _date.today()
    current_ym = f"{today.year}-{today.month:02d}"

    rows = con.execute(
        "SELECT year_month, COUNT(*) as n FROM monthly_nav "
        "WHERE return_pct IS NOT NULL AND year_month < ? "
        "GROUP BY year_month ORDER BY year_month DESC LIMIT 2",
        (current_ym,),
    ).fetchall()
    if not rows:
        return None
    latest_n = rows[0][1]
    if len(rows) >= 2 and rows[1][1] > 0 and latest_n < rows[1][1] * 0.95:
        return rows[1][0]  # latest pre-current month is still sparse, use the one before
    return rows[0][0]


def run_sync() -> dict:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    rows = con.execute(
        "SELECT isin, year_month, return_pct FROM monthly_nav "
        "WHERE return_pct IS NOT NULL ORDER BY isin, year_month"
    ).fetchall()

    if not rows:
        con.close()
        return {"synced": 0, "latest_month": None}

    fund_months: dict = defaultdict(list)
    for r in rows:
        fund_months[r["isin"]].append((r["year_month"], r["return_pct"]))

    # Use last COMPLETE month (not partial current month)
    latest_ym = _get_complete_month(con)
    if not latest_ym:
        latest_ym = max(r["year_month"] for r in rows)
    current_year = latest_ym[:4]

    def _compound(months_list, n):
        recent = months_list[-n:]
        v = 1.0
        for _, ret in recent:
            v *= 1 + ret / 100
        return round((v - 1) * 100, 4)

    # Clear stale figures first: funds with no data for the reference month
    # (closed/merged or not yet published) must drop out of the current ranking
    con.execute(
        "UPDATE period_returns SET return_1m=NULL, return_3m=NULL, return_6m=NULL, "
        "return_ytd=NULL, return_1a=NULL"
    )

    updated = 0
    for isin, months in fund_months.items():
        # Build a dict for fast lookup by year_month
        month_dict = {m: r for m, r in months}

        # r1m = return for the last COMPLETE month specifically
        r1m = month_dict.get(latest_ym)
        if r1m is None:
            continue  # fund has no data for the complete reference month

        # For compound periods, use only months up to and including latest_ym
        months_upto = [(m, r) for m, r in months if m <= latest_ym]
        if not months_upto:
            continue

        r3m  = _compound(months_upto, 3)  if len(months_upto) >= 3  else None
        r6m  = _compound(months_upto, 6)  if len(months_upto) >= 6  else None
        r1a  = _compound(months_upto, 12) if len(months_upto) >= 12 else None

        ytd = [(m, r) for m, r in months_upto if m.startswith(current_year)]
        r_ytd = _compound(ytd, len(ytd)) if ytd else None

        con.execute(
            "UPDATE period_returns "
            "SET return_1m=?, return_3m=?, return_6m=?, return_ytd=?, return_1a=?, updated_at=? "
            "WHERE isin=?",
            (r1m, r3m, r6m, r_ytd, r1a, latest_ym + "-15T00:00:00", isin),
        )
        if con.execute("SELECT changes()").fetchone()[0]:
            updated += 1

    # Fill annual_returns for fully completed years missing from the CSV import
    # (e.g. 2026 once December 2026 is in). Existing CSV rows are left untouched.
    last_csv_year = con.execute("SELECT MAX(year) FROM annual_returns").fetchone()[0] or 0
    new_years = set()
    for isin, months in fund_months.items():
        by_year: dict = defaultdict(list)
        for m, r in months:
            if m <= latest_ym and int(m[:4]) > last_csv_year:
                by_year[int(m[:4])].append(r)
        for yr, rets in by_year.items():
            if len(rets) != 12:
                continue
            v = 1.0
            for r in rets:
                v *= 1 + r / 100
            mean = sum(rets) / 12
            vol = (sum((r - mean) ** 2 for r in rets) / 11) ** 0.5 * 12 ** 0.5
            con.execute(
                "INSERT OR IGNORE INTO annual_returns (isin, year, return_pct, volatility_pct) "
                "VALUES (?,?,?,?)",
                (isin, yr, round((v - 1) * 100, 4), round(vol, 4)),
            )
            if con.execute("SELECT changes()").fetchone()[0]:
                new_years.add(yr)

    # Invalidate current-year and newly added years so rankings refresh
    for period in {current_year, *(str(y) for y in new_years)}:
        con.execute("DELETE FROM top5_year_cache WHERE period=?", (period,))
    con.commit()
    con.close()

    logger.info(f"sync_from_nav: updated {updated} funds, latest month = {latest_ym}"
                + (f", new annual years {sorted(new_years)}" if new_years else ""))
    return {"synced": updated, "latest_month": latest_ym}


def needs_sync() -> bool:
    """Return True if period_returns is behind the latest monthly_nav month."""
    try:
        con = sqlite3.connect(DB_PATH)
        latest_nav = con.execute(
            "SELECT MAX(year_month) FROM monthly_nav WHERE return_pct IS NOT NULL"
        ).fetchone()[0]
        latest_pr = con.execute(
            "SELECT MAX(updated_at) FROM period_returns"
        ).fetchone()[0]
        con.close()
        if not latest_nav:
            return False
        if not latest_pr:
            return True
        # If period_returns was last updated before the latest_nav month
        return latest_pr[:7] < latest_nav
    except Exception:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    result = run_sync()
    print(f"Synced {result['synced']} funds. Latest month: {result['latest_month']}")
