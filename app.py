import os
import logging
from flask import Flask, render_template, jsonify, request
from data_service import (
    get_top_funds_current_period,
    get_top_funds_for_year,
    get_available_years,
    get_import_status,
    get_dashboard_chart_data,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


# ── Auto-sync on startup ──────────────────────────────────────────────────────

def _startup_sync():
    """Sync period_returns from monthly_nav if data is stale."""
    try:
        from sync_from_nav import run_sync, needs_sync
        if needs_sync():
            logger.info("Startup: period_returns is stale — syncing from monthly_nav…")
            r = run_sync()
            logger.info(f"Startup sync done: {r['synced']} funds, latest={r['latest_month']}")
        else:
            logger.info("Startup: period_returns is up to date.")
    except Exception as e:
        logger.error(f"Startup sync error: {e}")


# ── APScheduler: daily auto-update ───────────────────────────────────────────

def _scheduled_job():
    """
    Daily job at 07:00:
      1. Sync period_returns from whatever monthly_nav has.
      2. If we're past the 3rd of the month and don't have the current month's
         data for most funds, trigger a background Morningstar fetch.
    """
    from datetime import date
    logger.info("Scheduler: running daily job…")

    # Always sync period_returns
    try:
        from sync_from_nav import run_sync
        r = run_sync()
        logger.info(f"Scheduler sync: {r['synced']} funds, latest={r['latest_month']}")
    except Exception as e:
        logger.error(f"Scheduler sync error: {e}")

    # Check if we should fetch new NAV data
    try:
        import sqlite3
        db = os.environ.get("FONDOS_DB_PATH",
             os.path.join(os.path.dirname(__file__), "data", "cache.db"))
        con = sqlite3.connect(db)
        latest_ym = con.execute(
            "SELECT MAX(year_month) FROM monthly_nav WHERE return_pct IS NOT NULL"
        ).fetchone()[0]
        con.close()

        today = date.today()
        current_ym = f"{today.year}-{today.month:02d}"

        # Trigger fetch if we're on or past the 3rd and missing current month
        if latest_ym and latest_ym < current_ym and today.day >= 3:
            logger.info(f"Scheduler: missing {current_ym} data — triggering background fetch…")
            import threading
            def _run():
                from fetch_monthly_nav import fetch_all
                fetch_all()
                from sync_from_nav import run_sync
                run_sync()
            threading.Thread(target=_run, daemon=True).start()
    except Exception as e:
        logger.error(f"Scheduler NAV-check error: {e}")


def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(_scheduled_job, "cron", hour=7, minute=0,
                          id="daily_sync", replace_existing=True)
        scheduler.start()
        logger.info("APScheduler started — daily sync at 07:00")
        return scheduler
    except Exception as e:
        logger.error(f"Scheduler start error: {e}")
        return None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", status=get_import_status())


@app.route("/api/current")
def api_current():
    return jsonify(get_top_funds_current_period())


@app.route("/api/annual/<int:year>")
def api_annual(year: int):
    data = get_top_funds_for_year(year)
    return jsonify(data), (400 if "error" in data else 200)


@app.route("/api/available-years")
def api_years():
    return jsonify({"years": get_available_years()})


@app.route("/api/import-status")
def api_status():
    return jsonify(get_import_status())


@app.route("/api/dashboard-chart")
def api_dashboard_chart():
    return jsonify(get_dashboard_chart_data())


# ── Backtest ──────────────────────────────────────────────────────────────────

@app.route("/api/backtest-all")
def api_backtest_all():
    try:
        monthly = float(request.args.get("monthly", 1000))
    except (ValueError, TypeError):
        monthly = 1000.0
    from backtest import run_all
    return jsonify(run_all(monthly_contribution=monthly))


# ── Data management ───────────────────────────────────────────────────────────

@app.route("/api/nav-status")
def api_nav_status():
    try:
        import sqlite3 as sq
        db = os.environ.get("FONDOS_DB_PATH",
             os.path.join(os.path.dirname(__file__), "data", "cache.db"))
        con = sq.connect(db)
        total    = con.execute("SELECT COUNT(DISTINCT isin) FROM funds").fetchone()[0]
        with_nav = con.execute(
            "SELECT COUNT(DISTINCT isin) FROM monthly_nav WHERE return_pct IS NOT NULL"
        ).fetchone()[0]
        latest   = con.execute(
            "SELECT MAX(year_month) FROM monthly_nav WHERE return_pct IS NOT NULL"
        ).fetchone()[0]
        con.close()
        return jsonify({"total": total, "with_monthly_nav": with_nav,
                        "latest_month": latest,
                        "pct": round(with_nav / total * 100, 1) if total else 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fetch-nav", methods=["POST"])
def api_fetch_nav():
    import threading
    def _run():
        from fetch_monthly_nav import fetch_all
        fetch_all()
        from sync_from_nav import run_sync
        run_sync()
        logger.info("Background NAV fetch + sync complete.")
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"started": True})


@app.route("/api/sync-now", methods=["POST"])
def api_sync_now():
    """Manually trigger period_returns sync from monthly_nav."""
    try:
        from sync_from_nav import run_sync
        result = run_sync()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/import-csv", methods=["POST"])
def api_import():
    from import_csv import import_csv as do_import
    csv_path = os.path.join(os.path.dirname(__file__), "rentabilidades.csv")
    if not os.path.exists(csv_path):
        return jsonify({"error": "rentabilidades.csv no encontrado"}), 404
    try:
        n = do_import(csv_path)
        return jsonify({"ok": True, "imported": n})
    except Exception as e:
        logger.error(f"Import error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    status = get_import_status()
    if status.get("fund_count", 0) == 0:
        csv_path = os.path.join(os.path.dirname(__file__), "rentabilidades.csv")
        if os.path.exists(csv_path):
            logger.info("DB vacía — importando CSV…")
            from import_csv import import_csv as do_import
            do_import(csv_path)

    _startup_sync()
    _start_scheduler()

    app.run(debug=False, host="0.0.0.0", port=5050, use_reloader=False)
