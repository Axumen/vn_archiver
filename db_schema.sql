PRAGMA foreign_keys = ON;

-- VN identity (title-level)
CREATE TABLE IF NOT EXISTS vn (
    vn_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL UNIQUE,
    series TEXT,
    series_description TEXT,
    aliases TEXT,
    developer TEXT,
    publisher TEXT,
    release_status TEXT,
    content_rating TEXT,
    content_mode TEXT,
    content_type TEXT,
    description TEXT,
    source TEXT,
    tags TEXT,
    original_release_date TEXT,
    CHECK (original_release_date IS NULL OR original_release_date GLOB '????-??-??')
);

-- Build identity (version-level)
CREATE TABLE IF NOT EXISTS build (
    build_id INTEGER PRIMARY KEY,
    vn_id INTEGER NOT NULL,
    version TEXT NOT NULL,
    normalized_version TEXT GENERATED ALWAYS AS (lower(trim(version))) VIRTUAL,
    build_type TEXT,
    distribution_model TEXT,
    distribution_platform TEXT,
    language TEXT,
    translator TEXT,
    edition TEXT,
    release_date TEXT,
    engine TEXT,
    engine_version TEXT,
    target_platform TEXT,
    notes TEXT,
    change_note TEXT,
    CHECK (release_date IS NULL OR release_date GLOB '????-??-??'),
    CHECK (build_type IS NULL OR build_type IN ('full','patch','demo','trial','fandisc','hotfix','april_fools')),
    CHECK (distribution_model IS NULL OR distribution_model IN ('free','paid','freemium','subscription')),
    FOREIGN KEY (vn_id) REFERENCES vn(vn_id) ON DELETE CASCADE
);

-- Deduplicated physical file identity
CREATE TABLE IF NOT EXISTS file (
    file_id INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER,
    first_seen_at TEXT,
    filename TEXT,
    mime_type TEXT,
    CHECK (length(sha256) = 64)
);

-- Build <-> File many-to-many association
CREATE TABLE IF NOT EXISTS build_file (
    build_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    original_filename TEXT,
    archived_at TEXT,
    PRIMARY KEY (build_id, file_id),
    FOREIGN KEY (build_id) REFERENCES build(build_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES file(file_id) ON DELETE CASCADE
);

-- Build relationship semantics using metadata fields + explicit build linkage
CREATE TABLE IF NOT EXISTS build_relation (
    relation_id INTEGER PRIMARY KEY,
    from_build_id INTEGER NOT NULL,
    to_build_id INTEGER,
    parent_vn_title TEXT,
    relationship_type TEXT NOT NULL,
    CHECK (relationship_type <> ''),
    FOREIGN KEY (from_build_id) REFERENCES build(build_id) ON DELETE CASCADE,
    FOREIGN KEY (to_build_id) REFERENCES build(build_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_build_vn ON build(vn_id);
CREATE INDEX IF NOT EXISTS idx_build_type ON build(build_type);
CREATE INDEX IF NOT EXISTS idx_file_sha256 ON file(sha256);
CREATE INDEX IF NOT EXISTS idx_build_relation_from ON build_relation(from_build_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_build_identity ON build(vn_id, normalized_version, language, edition, distribution_platform);
