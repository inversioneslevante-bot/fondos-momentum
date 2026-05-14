"""
Fetch real monthly NAV data from Morningstar Global for all funds.

Architecture:
  Phase 1: resolve ISIN → Morningstar IDs via search API (curl_cffi, fast)
  Phase 2: fetch monthly chart data using a small pool of persistent browser
           contexts. Each context solves the AWS WAF challenge once, then
           processes its share of funds — no per-fund WAF re-challenge.

Runtime: ~10-20 min for 400 funds with 3 persistent contexts.
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")
N_WORKERS    = 3      # parallel persistent browser contexts
PAGE_TIMEOUT = 25_000  # ms per page load
WAIT_SECS    = 6.0    # seconds to wait for chart XHR after page load
RETRY_DELAY  = 1.0


# ── DB helpers ────────────────────────────────────────────────────────────────

def _init_schema():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS ms_fund_ids (
            isin TEXT PRIMARY KEY, sec_id TEXT, pi_id TEXT,
            name_ms TEXT, fetched_at TEXT);
        CREATE TABLE IF NOT EXISTS monthly_nav (
            isin TEXT NOT NULL, year_month TEXT NOT NULL,
            nav_value REAL NOT NULL, return_pct REAL,
            PRIMARY KEY (isin, year_month));
        CREATE TABLE IF NOT EXISTS monthly_top5_cache (
            year_month TEXT PRIMARY KEY, data TEXT NOT NULL, computed_at TEXT);
    """)
    con.commit(); con.close()


def _get_all_isins() -> List[Tuple[str, str]]:
    con = sqlite3.connect(DB_PATH)
    try:
        return [(r[0], r[1]) for r in con.execute("SELECT isin, name FROM funds").fetchall()]
    finally:
        con.close()


def _already_fetched(isin: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    try:
        return con.execute("SELECT COUNT(*) FROM monthly_nav WHERE isin=?", (isin,)).fetchone()[0] >= 20
    finally:
        con.close()


def _get_ms_ids(isin: str) -> Optional[Tuple[str, str]]:
    con = sqlite3.connect(DB_PATH)
    try:
        r = con.execute("SELECT sec_id, pi_id FROM ms_fund_ids WHERE isin=?", (isin,)).fetchone()
        return (r[0], r[1]) if r and r[0] else None
    finally:
        con.close()


def _save_ms_ids(isin: str, sec_id: str, pi_id: str, name: str):
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("INSERT OR REPLACE INTO ms_fund_ids VALUES (?,?,?,?,?)",
                    (isin, sec_id, pi_id, name, datetime.utcnow().isoformat()))
        con.commit()
    finally:
        con.close()


def _save_nav(isin: str, pts: list) -> int:
    if not pts:
        return 0
    con = sqlite3.connect(DB_PATH)
    try:
        for i, pt in enumerate(pts):
            ym  = pt["date"][:7]
            val = float(pt["value"])
            ret = round((val / float(pts[i-1]["value"]) - 1) * 100, 4) if i > 0 else None
            con.execute("INSERT OR REPLACE INTO monthly_nav VALUES (?,?,?,?)",
                        (isin, ym, val, ret))
        con.commit()
        return len(pts)
    finally:
        con.close()


def _nav_count() -> int:
    con = sqlite3.connect(DB_PATH)
    try:
        return con.execute("SELECT COUNT(DISTINCT isin) FROM monthly_nav").fetchone()[0]
    finally:
        con.close()


# ── Phase 1: Resolve ISINs → MS IDs ──────────────────────────────────────────

def phase1_resolve_ids(isins: List[Tuple[str, str]]) -> int:
    from curl_cffi import requests as cf
    s = cf.Session(impersonate="chrome124")
    s.headers.update({"Accept": "*/*", "Accept-Language": "es-ES,es;q=0.9",
                       "Referer": "https://www.morningstar.es/",
                       "X-Requested-With": "XMLHttpRequest"})
    try:
        s.get("https://www.morningstar.es/", timeout=8)
    except Exception:
        pass

    resolved = 0
    for idx, (isin, _) in enumerate(isins):
        if _get_ms_ids(isin):
            resolved += 1; continue
        try:
            r = s.get("https://www.morningstar.es/es/util/SecuritySearch.ashx",
                      params={"q": isin, "limit": "3", "source": "nav", "secExcluded": "", "version": "2"},
                      timeout=8)
            if r.status_code == 200:
                m = re.search(r'"i"\s*:\s*"([^"]+)".*?"pi"\s*:\s*"([^"]+)".*?"n"\s*:\s*"([^"]+)"', r.text)
                if m:
                    _save_ms_ids(isin, m.group(1), m.group(2), m.group(3))
                    resolved += 1
                    logger.debug(f"  [{idx+1:3d}] {isin} → {m.group(1)} / {m.group(2)}")
        except Exception as e:
            logger.debug(f"  [{idx+1:3d}] {isin}: {e}")
        time.sleep(0.12)
    logger.info(f"Phase 1: {resolved}/{len(isins)} IDs resolved")
    return resolved


# ── Phase 2: Worker — persistent context fetches many funds ──────────────────

async def _worker(worker_id: int, browser, queue: asyncio.Queue, results: list, total: int):
    """
    One persistent browser context that:
    1. Visits Morningstar main page → solves WAF challenge once
    2. Loops: picks a fund from queue → fetches its chart data → saves
    """
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="es-ES",
        timezone_id="Europe/Madrid",
        viewport={"width": 1440, "height": 900},
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )

    # ── Warm-up: solve WAF once ────────────────────────────────────────────
    warmup_page = await ctx.new_page()
    try:
        await warmup_page.goto(
            "https://global.morningstar.com/es/inversiones",
            timeout=PAGE_TIMEOUT, wait_until="domcontentloaded"
        )
        await asyncio.sleep(3)
    except Exception:
        pass
    await warmup_page.close()

    # ── Process funds from queue ───────────────────────────────────────────
    while True:
        try:
            isin, name, sec_id, pi_id, idx = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        chart_data = None
        page = await ctx.new_page()

        try:
            url = f"https://global.morningstar.com/es/inversiones/fondos/{pi_id}/cotizacion"

            # Use expect_response to wait for the chart API call
            try:
                async with page.expect_response(
                    lambda r: "performance/chart" in r.url and r.status == 200,
                    timeout=PAGE_TIMEOUT + 5000
                ) as resp_holder:
                    await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                    await asyncio.sleep(WAIT_SECS)

                resp = await resp_holder.value
                body = await resp.body()
                chart_data = json.loads(body.decode("utf-8"))
            except Exception:
                pass

            # Fallback: call the API via page.evaluate (uses browser's cookies)
            if not chart_data:
                try:
                    txt = await page.evaluate(f"""async () => {{
                        try {{
                            const r = await fetch(
                                'https://api-global.morningstar.com/sal-service/v1/fund/data/performance/chart' +
                                '?shareClassId={sec_id}&secExchangeList=&limitAge=&hideYTD=false' +
                                '&locale=es&clientId=INTLCOM&benchmarkId=mstarorcat&version=4.86.0',
                                {{credentials:'include', headers:{{Accept:'application/json'}}}}
                            );
                            if (!r.ok) return null;
                            return await r.text();
                        }} catch(e) {{ return null; }}
                    }}""")
                    if txt:
                        chart_data = json.loads(txt)
                except Exception:
                    pass

            if chart_data:
                pts = chart_data.get("graphData", {}).get("fund", [])
                if pts:
                    n = _save_nav(isin, pts)
                    logger.info(
                        f"[W{worker_id}] [{idx:3d}/{total}] ✅  {isin}  "
                        f"{pts[0]['date'][:7]}→{pts[-1]['date'][:7]}  "
                        f"{n} pts  {name[:38]}"
                    )
                    results.append((True, isin))
                    queue.task_done(); continue

            logger.warning(f"[W{worker_id}] [{idx:3d}/{total}] ❌  {isin}: no chart data")
            results.append((False, isin))

        except Exception as e:
            logger.warning(f"[W{worker_id}] [{idx:3d}/{total}] ❌  {isin}: {str(e)[:60]}")
            results.append((False, isin))
        finally:
            try:
                await page.close()
            except Exception:
                pass
        queue.task_done()

    await ctx.close()


# ── Phase 2 orchestration ─────────────────────────────────────────────────────

async def phase2_fetch(pending: List[Tuple]):
    from playwright.async_api import async_playwright

    total = len(pending)
    queue: asyncio.Queue = asyncio.Queue()
    results = []

    for i, (isin, name, sec_id, pi_id) in enumerate(pending):
        await queue.put((isin, name, sec_id, pi_id, i + 1))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            channel="chrome",
            headless=True,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        workers = [
            asyncio.create_task(_worker(w + 1, browser, queue, results, total))
            for w in range(min(N_WORKERS, total))
        ]
        await asyncio.gather(*workers)
        await browser.close()

    ok   = sum(1 for s, _ in results if s)
    fail = sum(1 for s, _ in results if not s)
    return ok, fail


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_all(force: bool = False) -> dict:
    _init_schema()
    all_isins = _get_all_isins()
    total = len(all_isins)
    logger.info(f"{'='*55}\n  NAV FETCHER — {total} funds\n{'='*55}")

    # Phase 1
    logger.info("PHASE 1: Resolving Morningstar IDs…")
    phase1_resolve_ids(all_isins)
    logger.info("")

    # Build phase-2 list
    if force:
        pending_nav = all_isins
    else:
        pending_nav = [(i, n) for i, n in all_isins if not _already_fetched(i)]

    pending = []
    for isin, name in pending_nav:
        ids = _get_ms_ids(isin)
        if ids:
            pending.append((isin, name, ids[0], ids[1]))

    already = total - len(pending_nav)
    no_id   = len(pending_nav) - len(pending)
    logger.info(
        f"PHASE 2: {len(pending)} funds to fetch  "
        f"(cached:{already}  no-id:{no_id})"
    )

    if not pending:
        logger.info("Nothing to fetch.")
        return {"total": total, "already_cached": already, "fetched": 0,
                "failed": 0, "funds_with_data": _nav_count()}

    start = time.time()
    ok, fail = asyncio.run(phase2_fetch(pending))
    elapsed = time.time() - start

    result = {
        "total": total,
        "already_cached": already,
        "fetched": ok,
        "failed": fail,
        "no_ms_id": no_id,
        "elapsed_sec": round(elapsed, 1),
        "funds_with_data": _nav_count(),
    }
    logger.info(
        f"\n{'='*55}\n"
        f"  DONE in {elapsed:.0f}s  |  ✅ {ok}  ❌ {fail}  🔢 {result['funds_with_data']} total\n"
        f"{'='*55}"
    )
    return result


if __name__ == "__main__":
    import sys
    fetch_all(force="--force" in sys.argv)
