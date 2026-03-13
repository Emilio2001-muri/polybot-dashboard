"""
PolyBot Data Pusher — Supabase
================================
Runs LOCALLY on your PC alongside the bot.
Pushes trade data, balance, scan logs to Supabase
so the Vercel dashboard can display them in real-time.

Usage:
  1. Set SUPABASE_URL and SUPABASE_SERVICE_KEY in Polymarket/.env
  2. pip install supabase
  3. python pusher.py
"""

import sys
import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add parent dir (Polymarket/) for imports
POLYBOT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(POLYBOT_ROOT))

from dotenv import load_dotenv

# Load env from main Polymarket directory
ENV_PATH = POLYBOT_ROOT / ".env"
load_dotenv(ENV_PATH)

import config
from database import TradeDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("polybot.pusher")

# ============================================================
# CONFIG
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
PUSH_INTERVAL = 10  # seconds between pushes

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env\n"
        "Add these to your Polymarket/.env file"
    )
    sys.exit(1)


# ============================================================
# SUPABASE CLIENT
# ============================================================
try:
    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info(f"Supabase connected: {SUPABASE_URL[:40]}...")
except ImportError:
    logger.error("Install supabase: pip install supabase")
    sys.exit(1)


TABLE = "dashboard_cache"
PREFIX = "pb_"

def upsert(key: str, value):
    """Update a row in dashboard_cache with pb_ prefix."""
    try:
        sb.table(TABLE).upsert({
            "key": PREFIX + key,
            "value": value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        logger.error(f"Supabase upsert '{PREFIX}{key}' failed: {e}")


# ============================================================
# DATA COLLECTION
# ============================================================
def collect_and_push():
    """Read local SQLite DB and push to Supabase."""
    db = TradeDatabase()

    # --- Status ---
    mode = config.TRADING_MODE
    wallet = config.POLYGON_WALLET_ADDRESS
    wallet_short = f"{wallet[:6]}...{wallet[-4:]}" if wallet and len(wallet) > 10 else "—"

    upsert("status", {
        "mode": mode,
        "bot_running": True,
        "last_scan": datetime.now(timezone.utc).isoformat(),
        "wallet": wallet_short,
    })

    # --- Balance ---
    balance_history = db.get_balance_history()
    trade_stats = db.get_trade_stats()

    if mode == "simulation":
        if balance_history:
            current = balance_history[-1].get("balance", config.SIMULATION_BALANCE)
        else:
            current = config.SIMULATION_BALANCE
    else:
        try:
            from polymarket_api import PolymarketClient
            client = PolymarketClient()
            current = client.get_live_balance()
        except Exception:
            current = balance_history[-1].get("balance", 0) if balance_history else 0

    total_pnl = trade_stats.get("total_pnl", 0) or 0
    initial = config.SIMULATION_BALANCE if mode == "simulation" else 50.0
    pnl_pct = (total_pnl / max(initial, 1)) * 100

    upsert("balance", {
        "current": round(current, 2),
        "initial": round(initial, 2),
        "pnl": round(total_pnl, 2),
        "pnl_pct": round(pnl_pct, 1),
    })

    # --- Stats ---
    wins = trade_stats.get("closed_wins", 0) or 0
    losses = trade_stats.get("closed_losses", 0) or 0
    total_trades = trade_stats.get("total_trades", 0) or 0
    win_rate = round(wins / max(1, wins + losses) * 100)

    # Count arb vs dir from trades
    all_trades = db.get_trades(limit=500)
    arb_count = sum(1 for t in all_trades if t.get("trade_type") == "arbitrage")
    dir_count = sum(1 for t in all_trades if t.get("trade_type") == "directional")

    upsert("stats", {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "arb_trades": arb_count,
        "dir_trades": dir_count,
    })

    # --- Trades (last 50) ---
    trades = db.get_trades(limit=50)
    trades_clean = []
    for t in trades:
        trades_clean.append({
            "trade_id": t.get("trade_id", ""),
            "market_question": t.get("market_question", ""),
            "trade_type": t.get("trade_type", ""),
            "side": t.get("side", ""),
            "cost_usd": round(t.get("cost_usd", 0), 4),
            "pnl": round(t.get("pnl", 0), 4),
            "status": t.get("status", ""),
            "timestamp": t.get("timestamp", ""),
        })
    upsert("trades", trades_clean)

    # --- Balance history ---
    bh = []
    for h in balance_history[-200:]:  # last 200 points
        bh.append({
            "b": round(h.get("balance", 0), 2),
        })
    upsert("balance_history", bh)

    # --- Scan log ---
    scan_logs = db.get_scan_logs(limit=20)
    scans_clean = []
    for s in scan_logs:
        scans_clean.append({
            "scan_number": s.get("scan_number", 0),
            "markets_scanned": s.get("markets_scanned", 0),
            "arbitrage_found": s.get("arbitrage_found", 0),
            "trades_executed": s.get("trades_executed", 0),
            "trades_skipped": s.get("trades_skipped", 0),
            "duration_seconds": round(s.get("duration_seconds", 0), 1),
            "errors": s.get("errors", []),
        })
    upsert("scan_log", scans_clean)

    # --- Open positions ---
    open_trades = db.get_trades(status="open")
    open_clean = [{
        "market": t.get("market_question", "")[:50],
        "side": t.get("side", ""),
        "cost_usd": round(t.get("cost_usd", 0), 2),
        "type": t.get("trade_type", ""),
    } for t in open_trades]
    upsert("open_positions", open_clean)

    # --- Risk ---
    bh_last = balance_history[-1] if balance_history else {}
    upsert("risk", {
        "score": bh_last.get("risk_score", 0),
        "is_paused": False,
        "reason": "",
        "daily_pnl": round(bh_last.get("daily_pnl", 0), 2),
    })

    logger.info(
        f"Pushed: balance=${current:.2f} pnl=${total_pnl:+.2f} "
        f"trades={total_trades} mode={mode}"
    )


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    logger.info("=" * 50)
    logger.info("PolyBot Data Pusher — Supabase")
    logger.info(f"Push interval: {PUSH_INTERVAL}s")
    logger.info(f"Supabase: {SUPABASE_URL[:40]}...")
    logger.info("=" * 50)

    while True:
        try:
            collect_and_push()
        except KeyboardInterrupt:
            logger.info("Pusher stopped by user")
            # Mark bot as stopped
            upsert("status", {
                "mode": config.TRADING_MODE,
                "bot_running": False,
                "last_scan": datetime.utcnow().isoformat(),
                "wallet": "",
            })
            break
        except Exception as e:
            logger.error(f"Push cycle error: {e}")

        time.sleep(PUSH_INTERVAL)


if __name__ == "__main__":
    main()
