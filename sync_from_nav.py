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
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cache.db"),
)


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

    latest_ym = max(r["year_month"] for r in rows)
    current_year = latest_ym[:4]

    def _compound(months_list, n):
        recent = months_list[-n:]
        v = 1.0
        for _, ret in recent:
            v *= 1 + ret / 100
        return round((v - 1) * 100, 4)

    updated = 0
    for isin, months in fund_months.items():
        r1m = months[-1][1]
        r3m  = _compound(months, 3)  if len(months) >= 3  else None
        r6m  = _compound(months, 6)  if len(months) >= 6  else None
        r1a  = _compound(months, 12) if len(months) >= 12 else None

        ytd = [(m, r) for m, r in months if m.startswith(current_year)]
        r_ytd = _compound(ytd, len(ytd)) if ytd else None

        con.execute(
            "UPDATE period_returns "
            "SET return_1m=?, return_3m=?, return_6m=?, return_ytd=?, return_1a=?, updated_at=? "
            "WHERE isin=?",
            (r1m, r3m, r6m, r_ytd, r1a, latest_ym + "-15T00:00:00", isin),
        )
        if con.execute("SELECT changes()").fetchone()[0]:
            updated += 1

    # Invalidate current-year and partial cache so rankings refresh
    con.execute("DELETE FROM top5_year_cache WHERE period=?", (current_year,))
    con.commit()
    con.close()

    logger.info(f"sync_from_nav: updated {updated} funds, latest month = {latest_ym}")
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
