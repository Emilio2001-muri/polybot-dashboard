-- ============================================================
-- PolyBot Dashboard — Supabase Schema
-- Run in Supabase SQL Editor (same project as NEXUS)
-- ============================================================

-- Polymarket bot data cache (same pattern as NEXUS)
CREATE TABLE IF NOT EXISTS polybot_cache (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enable RLS
ALTER TABLE polybot_cache ENABLE ROW LEVEL SECURITY;

-- Public read
CREATE POLICY "Allow public read polybot" ON polybot_cache
  FOR SELECT USING (true);

-- Realtime
ALTER PUBLICATION supabase_realtime ADD TABLE polybot_cache;

-- Initial rows
INSERT INTO polybot_cache (key, value) VALUES
  ('status', '{"mode":"demo","bot_running":false,"last_scan":null}'),
  ('balance', '{"current":50.0,"initial":50.0,"pnl":0,"pnl_pct":0}'),
  ('stats', '{"total_trades":0,"wins":0,"losses":0,"win_rate":0,"arb_trades":0,"dir_trades":0}'),
  ('trades', '[]'),
  ('balance_history', '[]'),
  ('scan_log', '[]'),
  ('open_positions', '[]'),
  ('risk', '{"score":0,"is_paused":false,"reason":"","daily_pnl":0}')
ON CONFLICT (key) DO NOTHING;
