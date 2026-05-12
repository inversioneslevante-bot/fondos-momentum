import os
import logging

# Support Render persistent disk (/data) or local ./data
_base = os.environ.get("RENDER_DISK_PATH", os.path.dirname(__file__))
os.environ.setdefault("FONDOS_DB_PATH", os.path.join(_base, "data", "cache.db"))
from flask import Flask, render_template, jsonify, request
from data_service import (
    get_top_funds_current_period,
    get_top_funds_for_year,
    get_available_years,
    get_import_status,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", status=get_import_status())


@app.route("/api/current")
def api_current():
    return jsonify(get_top_funds_current_period())


@app.route("/api/annual/<int:year>")
def api_annual(year: int):
    data = get_top_funds_for_year(year)
    if "error" in data:
        return jsonify(data), 400
    return jsonify(data)


@app.route("/api/available-years")
def api_years():
    return jsonify({"years": get_available_years()})


@app.route("/api/import-status")
def api_status():
    return jsonify(get_import_status())


@app.route("/api/backtest")
def api_backtest():
    try:
        monthly = float(request.args.get("monthly", 1000))
    except (ValueError, TypeError):
        monthly = 1000.0
    from backtest import run_backtest
    return jsonify(run_backtest(monthly_contribution=monthly))


@app.route("/api/backtest2")
def api_backtest2():
    """Full-compound strategy: sell everything each period, reinvest all + new contribution."""
    try:
        monthly = float(request.args.get("monthly", 1000))
    except (ValueError, TypeError):
        monthly = 1000.0
    from backtest import run_backtest_compound
    return jsonify(run_backtest_compound(monthly_contribution=monthly))


@app.route("/api/backtest-monthly")
def api_backtest_monthly():
    try:
        monthly = float(request.args.get("monthly", 1000))
    except (ValueError, TypeError):
        monthly = 1000.0
    from backtest_monthly import run_backtest_monthly
    return jsonify(run_backtest_monthly(monthly_contribution=monthly))


@app.route("/api/fetch-nav", methods=["POST"])
def api_fetch_nav():
    """Trigger Morningstar monthly NAV data fetch (runs in background)."""
    import threading
    def _run():
        from fetch_monthly_nav import fetch_all
        fetch_all()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"started": True, "message": "Fetching started in background"})


@app.route("/api/backtest-compare")
def api_backtest_compare():
    """Return both strategies in one call for side-by-side comparison."""
    try:
        monthly = float(request.args.get("monthly", 1000))
    except (ValueError, TypeError):
        monthly = 1000.0
    from backtest import run_backtest, run_backtest_compound
    return jsonify({
        "strategy_a": run_backtest(monthly_contribution=monthly),
        "strategy_b": run_backtest_compound(monthly_contribution=monthly),
        "monthly": monthly,
    })


@app.route("/api/nav-status")
def api_nav_status():
    import sqlite3 as _sq
    db = os.path.join(os.path.dirname(__file__), "data", "cache.db")
    try:
        con = _sq.connect(db)
        total    = con.execute("SELECT COUNT(DISTINCT isin) FROM funds").fetchone()[0]
        with_nav = con.execute("SELECT COUNT(DISTINCT isin) FROM monthly_nav WHERE return_pct IS NOT NULL").fetchone()[0]
        ids_ok   = con.execute("SELECT COUNT(*) FROM ms_fund_ids WHERE sec_id IS NOT NULL").fetchone()[0]
        con.close()
        return jsonify({
            "total": total,
            "with_monthly_nav": with_nav,
            "ms_ids_resolved": ids_ok,
            "pct": round(with_nav / total * 100, 1) if total else 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/import-csv", methods=["POST"])
def api_import():
    from import_csv import import_csv as do_import
    csv_path = os.path.join(os.path.dirname(__file__), "rentabilidades.csv")
    if not os.path.exists(csv_path):
        return jsonify({"error": "Archivo rentabilidades.csv no encontrado"}), 404
    try:
        n = do_import(csv_path)
        return jsonify({"ok": True, "imported": n})
    except Exception as e:
        logger.error(f"Import error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    status = get_import_status()
    if status.get("fund_count", 0) == 0:
        csv_path = os.path.join(os.path.dirname(__file__), "rentabilidades.csv")
        if os.path.exists(csv_path):
            logger.info("DB vacía — importando CSV automáticamente…")
            from import_csv import import_csv as do_import
            n = do_import(csv_path)
            logger.info(f"Importados {n} fondos.")

    app.run(debug=False, host="0.0.0.0", port=5050, use_reloader=False)
