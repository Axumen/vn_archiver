-- Enable foreign keys (important)
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

INSERT OR IGNORE INTO schema_version (version) VALUES (1);

CREATE TABLE IF NOT EXISTS visual_novels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Core searchable fields
    title TEXT,
    developer TEXT,
    release_date TEXT,
    version TEXT,
    status TEXT,

    -- File info
    sha256 TEXT UNIQUE,
    file_size INTEGER,

    -- Full metadata storage (future-proof)
    metadata_json TEXT,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_tags (
    vn_id INTEGER,
    tag_id INTEGER,
    UNIQUE(vn_id, tag_id),
    FOREIGN KEY(vn_id) REFERENCES visual_novels(id),
    FOREIGN KEY(tag_id) REFERENCES tags(id)
);

-- Indexes (critical for performance)
CREATE INDEX IF NOT EXISTS idx_vn_title ON visual_novels(title);
CREATE INDEX IF NOT EXISTS idx_vn_developer ON visual_novels(developer);
CREATE INDEX IF NOT EXISTS idx_vn_sha ON visual_novels(sha256);
CREATE INDEX IF NOT EXISTS idx_vn_tags_vn_id ON vn_tags(vn_id);