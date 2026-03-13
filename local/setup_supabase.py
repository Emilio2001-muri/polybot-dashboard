"""
Verify polybot_cache table exists in Supabase and seed initial rows.
Run AFTER creating the table via SQL Editor (supabase_schema.sql).
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
from supabase import create_client

url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_SERVICE_KEY", "")
if not url or not key:
    print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY in Polymarket/.env")
    sys.exit(1)

sb = create_client(url, key)
print(f"Connected: {url[:40]}...")

try:
    result = sb.table("polybot_cache").select("key").execute()
    existing = [r["key"] for r in result.data] if result.data else []
    print(f"polybot_cache has {len(existing)} rows: {existing}")
except Exception as e:
    print(f"\nERROR: polybot_cache table not found.\n{e}")
    print("\nCrea la tabla ejecutando supabase_schema.sql en el SQL Editor de Supabase.")
    sys.exit(1)

initial = {
    "status": {"mode": "demo", "bot_running": False, "last_scan": None},
    "balance": {"current": 50.0, "initial": 50.0, "pnl": 0, "pnl_pct": 0},
    "stats": {"total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "arb_trades": 0, "dir_trades": 0},
    "trades": [],
    "balance_history": [],
    "scan_log": [],
    "open_positions": [],
    "risk": {"score": 0, "is_paused": False, "reason": "", "daily_pnl": 0},
}
created = 0
for k, v in initial.items():
    if k not in existing:
        sb.table("polybot_cache").insert({"key": k, "value": v}).execute()
        print(f"  Creado: {k}")
        created += 1

print(f"\n{'Se crearon '+str(created)+' filas.' if created else 'Todo listo.'}")
print("Ya puedes ejecutar el pusher.")
