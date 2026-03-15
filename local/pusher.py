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
_sim_mode_active = False  # True after running simulation


def collect_and_push():
    """Read local SQLite DB and push to Supabase."""
    global _sim_mode_active
    db = TradeDatabase()

    # --- Status ---
    mode = "simulation" if _sim_mode_active else config.TRADING_MODE
    wallet = config.POLYGON_WALLET_ADDRESS
    wallet_short = f"{wallet[:6]}...{wallet[-4:]}" if wallet and len(wallet) > 10 else "—"

    upsert("status", {
        "mode": mode,
        "bot_running": True,
        "autoloop": _autoloop_active,
        "last_scan": datetime.now(timezone.utc).isoformat(),
        "wallet": wallet_short,
    })

    # --- Balance ---
    balance_history = db.get_balance_history()
    trade_stats = db.get_trade_stats()

    if _sim_mode_active or mode == "simulation":
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
# REMOTE COMMAND EXECUTION
# ============================================================
_engine = None  # reuse engine across scans for position tracking
_autoloop_active = False  # True when 24/7 mode is running


def _get_engine():
    global _engine
    if _engine is None:
        from trading_engine import TradingEngine
        _engine = TradingEngine()
    return _engine


def _execute_simulation(balance: float, rounds: int):
    """Run a full simulation using the Simulator, saving results to DB."""
    from simulator import Simulator
    db = TradeDatabase()

    sim = Simulator(initial_balance=balance)
    result = sim.run_simulation(
        num_rounds=rounds, markets_per_round=6, include_arbitrage=True,
    )

    # Save to DB (same as dashboard.py does)
    db.clear_all()
    for trade in result.trades_log:
        db.save_trade({
            "trade_id": trade.get("trade_id", ""),
            "market_question": trade.get("market", ""),
            "condition_id": trade.get("trade_id", ""),
            "side": trade.get("side", trade.get("type", "")),
            "trade_type": trade.get("type", ""),
            "cost_usd": trade.get("cost", 0),
            "pnl": trade.get("pnl", 0),
            "status": "closed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        db.update_trade(trade.get("trade_id", ""), {
            "pnl": trade.get("pnl", 0),
            "status": "closed",
            "closed_at": datetime.now(timezone.utc).isoformat(),
        })
    for _ts, bv in result.balance_history:
        db.save_balance_snapshot({
            "balance": bv, "total_pnl": bv - result.initial_balance,
            "daily_pnl": bv - result.initial_balance,
            "open_positions": 0, "risk_score": 0,
        })
    db.save_scan_log({
        "scan_number": 1, "markets_scanned": rounds * 6,
        "arbitrage_found": result.arbitrage_trades,
        "trades_executed": result.total_trades, "trades_skipped": 0,
        "duration_seconds": result.duration_minutes * 60,
    })

    logger.info(
        f"SIM done: {result.total_trades} trades, "
        f"${result.final_balance:.2f} final, "
        f"${result.total_pnl:+.2f} PnL, "
        f"{result.win_rate:.0%} WR"
    )

    return {
        "total_trades": result.total_trades,
        "final_balance": round(result.final_balance, 2),
        "total_pnl": round(result.total_pnl, 2),
        "win_rate": round(result.win_rate * 100),
        "max_drawdown": round(result.max_drawdown, 2),
        "arb_trades": result.arbitrage_trades,
        "dir_trades": result.directional_trades,
    }


def _execute_scans(count: int, is_loop: bool = False):
    """Run N scan cycles using the TradingEngine, saving results to DB."""
    db = TradeDatabase()
    engine = _get_engine()
    results = []

    for i in range(count):
        try:
            scan = engine.run_scan_cycle()
            state = engine.risk.get_state()

            db.save_scan_log({
                "scan_number": i + 1,
                "markets_scanned": scan.markets_scanned,
                "arbitrage_found": scan.arbitrage_found,
                "trades_executed": scan.trades_executed,
                "trades_skipped": scan.trades_skipped,
                "duration_seconds": scan.duration_seconds,
                "errors": scan.errors,
            })
            db.save_balance_snapshot({
                "balance": state.balance,
                "total_pnl": state.total_pnl,
                "daily_pnl": state.daily_pnl,
                "open_positions": state.open_positions_count,
                "risk_score": state.risk_score,
            })
            results.append({
                "scan": i + 1,
                "markets": scan.markets_scanned,
                "arb": scan.arbitrage_found,
                "trades": scan.trades_executed,
                "duration": scan.duration_seconds,
                "errors": scan.errors,
            })
            logger.info(
                f"CMD scan {i+1}/{count}: "
                f"markets={scan.markets_scanned} arb={scan.arbitrage_found} "
                f"trades={scan.trades_executed}"
            )
        except Exception as e:
            logger.error(f"CMD scan {i+1} error: {e}")
            results.append({"scan": i + 1, "error": str(e)})
            break

        # Wait between scans (except after last)
        if i < count - 1:
            wait = config.SCAN_INTERVAL_SECONDS if is_loop else 3
            time.sleep(wait)

    return results


def check_and_execute_commands():
    """Check Supabase for pending commands from the Vercel dashboard."""
    try:
        resp = sb.table(TABLE).select("value").eq("key", PREFIX + "command").execute()
        if not resp.data:
            return
        cmd = resp.data[0].get("value", {})
        if not cmd or not isinstance(cmd, dict):
            return
        status = cmd.get("status", "")
        if status != "pending":
            return

        action = cmd.get("action", "")
        count = int(cmd.get("count", 1))
        is_loop = cmd.get("is_loop", False)
        cmd_id = cmd.get("id", "")

        logger.info(f"📥 Command received: {action} count={count} id={cmd_id}")

        # Mark as running
        upsert("command", {
            "id": cmd_id,
            "action": action,
            "status": "running",
            "count": count,
            "progress": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })

        if action == "scan":
            results = _execute_scans(count, is_loop)
            # Push fresh data immediately after scans
            collect_and_push()
            # Mark as completed
            upsert("command", {
                "id": cmd_id,
                "action": action,
                "status": "completed",
                "count": count,
                "results": results,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info(f"✅ Command completed: {len(results)} scans done")

        elif action == "simulate":
            global _sim_mode_active
            sim_balance = float(cmd.get("balance", 50))
            sim_rounds = int(cmd.get("rounds", 30))
            sim_result = _execute_simulation(sim_balance, sim_rounds)
            # Push fresh data in simulation mode
            _sim_mode_active = True
            collect_and_push()
            upsert("command", {
                "id": cmd_id,
                "action": action,
                "status": "completed",
                "sim_result": sim_result,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info(f"✅ Simulation completed: {sim_result['total_trades']} trades")

        elif action == "clear":
            db = TradeDatabase()
            db.clear_all()
            collect_and_push()
            upsert("command", {
                "id": cmd_id,
                "action": action,
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("✅ Cache cleared: all trades, logs, balance wiped")

        elif action == "autoloop":
            global _autoloop_active
            _autoloop_active = True
            upsert("command", {
                "id": cmd_id,
                "action": action,
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("✅ Auto-loop 24/7 activated — scanning every 3 min")

        elif action == "stop_loop":
            _autoloop_active = False
            upsert("command", {
                "id": cmd_id,
                "action": action,
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("✅ Auto-loop stopped")

        else:
            upsert("command", {
                "id": cmd_id,
                "status": "error",
                "error": f"Unknown action: {action}",
            })

    except Exception as e:
        logger.error(f"Command check error: {e}")


# ============================================================
# MAIN LOOP
# ============================================================
_last_autoloop_scan = 0  # timestamp of last auto-loop scan


def main():
    global _last_autoloop_scan
    logger.info("=" * 50)
    logger.info("PolyBot Data Pusher — Supabase")
    logger.info(f"Push interval: {PUSH_INTERVAL}s")
    logger.info(f"Supabase: {SUPABASE_URL[:40]}...")
    logger.info("Commands: ENABLED (remote scan execution)")
    logger.info("Auto-loop: ready (activate from dashboard)")
    logger.info("=" * 50)

    while True:
        try:
            check_and_execute_commands()

            # --- Auto-loop: run a scan every SCAN_INTERVAL_SECONDS ---
            if _autoloop_active:
                now = time.time()
                if now - _last_autoloop_scan >= config.SCAN_INTERVAL_SECONDS:
                    _last_autoloop_scan = now
                    logger.info("♾️ Auto-loop scan triggered")
                    try:
                        db = TradeDatabase()
                        engine = _get_engine()
                        scan = engine.run_scan_cycle()
                        state = engine.risk.get_state()
                        db.save_scan_log({
                            "scan_number": len(db.get_scan_logs(limit=9999)) + 1,
                            "markets_scanned": scan.markets_scanned,
                            "arbitrage_found": scan.arbitrage_found,
                            "trades_executed": scan.trades_executed,
                            "trades_skipped": scan.trades_skipped,
                            "duration_seconds": scan.duration_seconds,
                            "errors": scan.errors,
                        })
                        db.save_balance_snapshot({
                            "balance": state.balance,
                            "total_pnl": state.total_pnl,
                            "daily_pnl": state.daily_pnl,
                            "open_positions": state.open_positions_count,
                            "risk_score": state.risk_score,
                        })
                        logger.info(
                            f"♾️ Auto-loop scan done: "
                            f"markets={scan.markets_scanned} arb={scan.arbitrage_found} "
                            f"trades={scan.trades_executed}"
                        )
                    except Exception as e:
                        logger.error(f"Auto-loop scan error: {e}")

            # Regular periodic push uses live mode
            global _sim_mode_active
            _sim_mode_active = False
            collect_and_push()
        except KeyboardInterrupt:
            logger.info("Pusher stopped by user")
            upsert("status", {
                "mode": config.TRADING_MODE,
                "bot_running": False,
                "last_scan": datetime.now(timezone.utc).isoformat(),
                "wallet": "",
            })
            break
        except Exception as e:
            logger.error(f"Push cycle error: {e}")

        time.sleep(PUSH_INTERVAL)


if __name__ == "__main__":
    main()
