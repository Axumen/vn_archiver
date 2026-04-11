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

CREATE TABLE IF NOT EXISTS tags (
    tag_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_tags (
    vn_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, tag_id),
    FOREIGN KEY (vn_id) REFERENCES vn(vn_id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS developers (
    developer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_developers (
    vn_id INTEGER NOT NULL,
    developer_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, developer_id),
    FOREIGN KEY (vn_id) REFERENCES vn(vn_id) ON DELETE CASCADE,
    FOREIGN KEY (developer_id) REFERENCES developers(developer_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_publishers (
    vn_id INTEGER NOT NULL,
    publisher_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, publisher_id),
    FOREIGN KEY (vn_id) REFERENCES vn(vn_id) ON DELETE CASCADE,
    FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS languages (
    language_id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS build_languages (
    build_id INTEGER NOT NULL,
    language_id INTEGER NOT NULL,
    PRIMARY KEY (build_id, language_id),
    FOREIGN KEY (build_id) REFERENCES build(build_id) ON DELETE CASCADE,
    FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_build_vn ON build(vn_id);
CREATE INDEX IF NOT EXISTS idx_build_type ON build(build_type);
CREATE INDEX IF NOT EXISTS idx_file_sha256 ON file(sha256);
CREATE INDEX IF NOT EXISTS idx_build_relation_from ON build_relation(from_build_id);
CREATE INDEX IF NOT EXISTS idx_vn_tags_vn ON vn_tags(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_tags_tag ON vn_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_vn_developers_vn ON vn_developers(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_developers_dev ON vn_developers(developer_id);
CREATE INDEX IF NOT EXISTS idx_vn_publishers_vn ON vn_publishers(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_publishers_pub ON vn_publishers(publisher_id);
CREATE INDEX IF NOT EXISTS idx_build_languages_build ON build_languages(build_id);
CREATE INDEX IF NOT EXISTS idx_build_languages_lang ON build_languages(language_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_build_identity ON build(vn_id, normalized_version, language, edition, distribution_platform);
