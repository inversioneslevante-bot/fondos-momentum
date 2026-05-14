import os
import logging
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


# ── Ranking endpoints ─────────────────────────────────────────────────────────

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


# ── Backtest (all 3 strategies in one call) ───────────────────────────────────

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
        total   = con.execute("SELECT COUNT(DISTINCT isin) FROM funds").fetchone()[0]
        with_nav = con.execute(
            "SELECT COUNT(DISTINCT isin) FROM monthly_nav WHERE return_pct IS NOT NULL"
        ).fetchone()[0]
        con.close()
        return jsonify({"total": total, "with_monthly_nav": with_nav,
                        "pct": round(with_nav / total * 100, 1) if total else 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fetch-nav", methods=["POST"])
def api_fetch_nav():
    import threading
    def _run():
        from fetch_monthly_nav import fetch_all
        fetch_all()
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"started": True})


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


if __name__ == "__main__":
    status = get_import_status()
    if status.get("fund_count", 0) == 0:
        csv_path = os.path.join(os.path.dirname(__file__), "rentabilidades.csv")
        if os.path.exists(csv_path):
            logger.info("DB vacía — importando CSV…")
            from import_csv import import_csv as do_import
            do_import(csv_path)

    app.run(debug=False, host="0.0.0.0", port=5050, use_reloader=False)
