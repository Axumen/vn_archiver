-- Enable foreign keys (important)
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

INSERT OR IGNORE INTO schema_version (version) VALUES (1);

CREATE TABLE IF NOT EXISTS visual_novels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    developer TEXT,
    engine TEXT,
    language TEXT,
    release_date TEXT,
    sha256 TEXT NOT NULL UNIQUE,
    file_size INTEGER,
    archive_path TEXT NOT NULL,
    version TEXT,
    status TEXT DEFAULT 'archived',
    date_added TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_tags (
    vn_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, tag_id),
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Indexes (critical for performance)
CREATE INDEX IF NOT EXISTS idx_vn_title ON visual_novels(title);
CREATE INDEX IF NOT EXISTS idx_vn_developer ON visual_novels(developer);
CREATE INDEX IF NOT EXISTS idx_vn_engine ON visual_novels(engine);
CREATE INDEX IF NOT EXISTS idx_vn_sha ON visual_novels(sha256);
CREATE INDEX IF NOT EXISTS idx_vn_tags_vn_id ON vn_tags(vn_id);