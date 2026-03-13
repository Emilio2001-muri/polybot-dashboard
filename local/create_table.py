"""Create polybot_cache table via Supabase Management API."""
import os, sys, requests
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_SERVICE_KEY", "")

if not url or not key:
    print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
    sys.exit(1)

# Try using PostgREST pg/query endpoint (available with service key)
sql = """
CREATE TABLE IF NOT EXISTS polybot_cache (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE polybot_cache ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename='polybot_cache' AND policyname='Allow public read polybot'
  ) THEN
    CREATE POLICY "Allow public read polybot" ON polybot_cache FOR SELECT USING (true);
  END IF;
END $$;

INSERT INTO polybot_cache (key, value) VALUES
  ('status', '{"mode":"live","bot_running":false,"last_scan":null}'),
  ('balance', '{"current":50.18,"initial":50.0,"pnl":0,"pnl_pct":0}'),
  ('stats', '{"total_trades":0,"wins":0,"losses":0,"win_rate":0,"arb_trades":0,"dir_trades":0}'),
  ('trades', '[]'),
  ('balance_history', '[]'),
  ('scan_log', '[]'),
  ('open_positions', '[]'),
  ('risk', '{"score":0,"is_paused":false,"reason":"","daily_pnl":0}')
ON CONFLICT (key) DO NOTHING;
"""

# Extract project ref from URL
project_ref = url.replace("https://", "").replace(".supabase.co", "")
print(f"Project: {project_ref}")

# Method 1: Try the SQL API endpoint  
headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json",
}

# Try pg-meta endpoint
pg_meta_url = f"{url}/pg/query"
print(f"Trying {pg_meta_url}...")
r = requests.post(pg_meta_url, headers=headers, json={"query": sql})
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print("SUCCESS! Table created.")
    # Verify
    from supabase import create_client
    sb = create_client(url, key)
    result = sb.table("polybot_cache").select("key").execute()
    keys = [row["key"] for row in result.data] if result.data else []
    print(f"Verified - polybot_cache has {len(keys)} rows: {keys}")
    sys.exit(0)
else:
    print(f"Response: {r.text[:300]}")

# Method 2: Try direct PostgREST RPC
print("\nTrying RPC method...")
for endpoint in ["/rest/v1/rpc/exec_sql", "/rest/v1/rpc/query"]:
    try:
        r2 = requests.post(f"{url}{endpoint}", headers=headers, json={"query": sql})
        print(f"  {endpoint}: {r2.status_code}")
        if r2.status_code == 200:
            print("SUCCESS!")
            sys.exit(0)
    except Exception as e:
        print(f"  {endpoint}: {e}")

print("\n" + "="*50)
print("No se pudo crear la tabla automaticamente.")
print("Por favor ejecuta este SQL en Supabase SQL Editor:")
print("="*50)
print("\n1. Ve a la pestana de 'Untitled query | SQL' que tienes abierta")
print("2. Pega este SQL y dale RUN:\n")

# Print simplified SQL for the user
print("""CREATE TABLE IF NOT EXISTS polybot_cache (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE polybot_cache ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read polybot" ON polybot_cache FOR SELECT USING (true);
ALTER PUBLICATION supabase_realtime ADD TABLE polybot_cache;
INSERT INTO polybot_cache (key, value) VALUES
  ('status', '{"mode":"live","bot_running":false,"last_scan":null}'),
  ('balance', '{"current":50.18,"initial":50.0,"pnl":0,"pnl_pct":0}'),
  ('stats', '{"total_trades":0,"wins":0,"losses":0,"win_rate":0,"arb_trades":0,"dir_trades":0}'),
  ('trades', '[]'),
  ('balance_history', '[]'),
  ('scan_log', '[]'),
  ('open_positions', '[]'),
  ('risk', '{"score":0,"is_paused":false,"reason":"","daily_pnl":0}')
ON CONFLICT (key) DO NOTHING;""")
