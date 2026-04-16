CREATE TABLE IF NOT EXISTS tournament (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  event_date TEXT NOT NULL,
  rounds_planned INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  registration_csv_name TEXT,
  registration_enabled INTEGER NOT NULL DEFAULT 0,
  registration_opens_at TEXT,
  registration_form_json TEXT,
  event_time TEXT,
  venue TEXT,
  max_registrations INTEGER,
  source_type TEXT NOT NULL DEFAULT 'local',
  source_ref TEXT,
  primary_tiebreak_label TEXT NOT NULL DEFAULT 'BH',
  secondary_tiebreak_label TEXT NOT NULL DEFAULT 'BH-C1',
  public_insights_json TEXT,
  is_historical INTEGER NOT NULL DEFAULT 0,
  is_public INTEGER NOT NULL DEFAULT 1,
  is_active_public INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tournament_source
ON tournament(source_type, source_ref)
WHERE source_ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_active_public_tournament
ON tournament(is_active_public)
WHERE is_active_public = 1;

CREATE TABLE IF NOT EXISTS player (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  email TEXT,
  canonical_rating_name TEXT,
  member_status TEXT NOT NULL DEFAULT 'unknown',
  historical_rating REAL,
  rating_deviation REAL,
  historical_wins INTEGER NOT NULL DEFAULT 0,
  historical_losses INTEGER NOT NULL DEFAULT 0,
  historical_draws INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_email ON player(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_name ON player(normalized_name);

CREATE TABLE IF NOT EXISTS member_override (
  player_id INTEGER PRIMARY KEY REFERENCES player(id) ON DELETE CASCADE,
  is_member INTEGER NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_member_override_status ON member_override(is_member);

CREATE TABLE IF NOT EXISTS tournament_entry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tournament_id INTEGER NOT NULL REFERENCES tournament(id) ON DELETE CASCADE,
  player_id INTEGER NOT NULL REFERENCES player(id) ON DELETE CASCADE,
  imported_name TEXT NOT NULL,
  imported_email TEXT,
  submitted_at TEXT,
  declared_rating INTEGER,
  seed_rating INTEGER NOT NULL,
  member_status TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  registration_source TEXT,
  registration_order INTEGER,
  registration_answers_json TEXT,
  waitlist_position INTEGER,
  final_rank INTEGER,
  final_score REAL,
  final_primary_tiebreak REAL,
  final_secondary_tiebreak REAL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (tournament_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_tournament_entry_player
ON tournament_entry(player_id);

CREATE TABLE IF NOT EXISTS entry_round_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL REFERENCES tournament_entry(id) ON DELETE CASCADE,
  round_no INTEGER NOT NULL,
  is_available INTEGER NOT NULL DEFAULT 1,
  UNIQUE (entry_id, round_no)
);

CREATE TABLE IF NOT EXISTS pairing (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tournament_id INTEGER NOT NULL REFERENCES tournament(id) ON DELETE CASCADE,
  round_no INTEGER NOT NULL,
  board_no INTEGER NOT NULL,
  white_entry_id INTEGER REFERENCES tournament_entry(id) ON DELETE SET NULL,
  black_entry_id INTEGER REFERENCES tournament_entry(id) ON DELETE SET NULL,
  result_code TEXT,
  manual_override INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (tournament_id, round_no, board_no)
);

CREATE INDEX IF NOT EXISTS idx_pairing_white_entry
ON pairing(white_entry_id);

CREATE INDEX IF NOT EXISTS idx_pairing_black_entry
ON pairing(black_entry_id);

CREATE TABLE IF NOT EXISTS app_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
