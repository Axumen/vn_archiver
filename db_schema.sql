PRAGMA foreign_keys = ON;

-- Series identity
CREATE TABLE IF NOT EXISTS series (
    series_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- VN identity (title-level)
CREATE TABLE IF NOT EXISTS vn (
    vn_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL UNIQUE,
    series_id INTEGER,
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
    FOREIGN KEY (series_id) REFERENCES series(series_id) ON DELETE SET NULL
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
    FOREIGN KEY (vn_id) REFERENCES vn(vn_id) ON DELETE CASCADE
);

-- Deduplicated physical file identity
CREATE TABLE IF NOT EXISTS file (
    file_id INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER,
    filename TEXT,
    CHECK (length(sha256) = 64)
);

-- Build <-> File many-to-many association
CREATE TABLE IF NOT EXISTS build_file (
    build_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    original_filename TEXT,
    artifact_type TEXT,
    archived_at TEXT,
    PRIMARY KEY (build_id, file_id),
    FOREIGN KEY (build_id) REFERENCES build(build_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES file(file_id) ON DELETE CASCADE
);

-- Parsed metadata captured at file-attachment time (VN > Build > File workflow)
CREATE TABLE IF NOT EXISTS build_file_metadata (
    metadata_id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    metadata_version INTEGER NOT NULL,
    title TEXT,
    version TEXT,
    build_type TEXT,
    normalized_version TEXT,
    distribution_platform TEXT,
    platform TEXT,
    language TEXT,
    edition TEXT,
    release_date TEXT,
    source_url TEXT,
    notes TEXT,
    change_note TEXT,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (build_id, file_id) REFERENCES build_file(build_id, file_id) ON DELETE CASCADE
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

-- Content-addressed metadata versioning with version chains
CREATE TABLE IF NOT EXISTS metadata_raw_versions (
    metadata_raw_id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL,
    file_id INTEGER,
    raw_json TEXT NOT NULL,
    raw_sha256 TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 0,
    parent_version_id INTEGER,
    change_note TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (build_id) REFERENCES build(build_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES file(file_id) ON DELETE SET NULL,
    FOREIGN KEY (parent_version_id) REFERENCES metadata_raw_versions(metadata_raw_id) ON DELETE SET NULL,
    UNIQUE (build_id, version_number)
);

-- Upload tracking: content-addressed archive objects in cloud storage
CREATE TABLE IF NOT EXISTS archive_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    CHECK (length(sha256) = 64)
);

-- Upload tracking: content-addressed metadata sidecar objects in cloud storage
CREATE TABLE IF NOT EXISTS metadata_file_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    CHECK (length(sha256) = 64)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_vn_series ON vn(series_id);
CREATE INDEX IF NOT EXISTS idx_build_vn ON build(vn_id);
CREATE INDEX IF NOT EXISTS idx_build_type ON build(build_type);
CREATE INDEX IF NOT EXISTS idx_file_sha256 ON file(sha256);
CREATE INDEX IF NOT EXISTS idx_build_file_metadata_pair ON build_file_metadata(build_id, file_id);
CREATE INDEX IF NOT EXISTS idx_vn_tags_vn ON vn_tags(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_tags_tag ON vn_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_vn_developers_vn ON vn_developers(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_developers_dev ON vn_developers(developer_id);
CREATE INDEX IF NOT EXISTS idx_vn_publishers_vn ON vn_publishers(vn_id);
CREATE INDEX IF NOT EXISTS idx_vn_publishers_pub ON vn_publishers(publisher_id);
CREATE INDEX IF NOT EXISTS idx_build_languages_build ON build_languages(build_id);
CREATE INDEX IF NOT EXISTS idx_build_languages_lang ON build_languages(language_id);
CREATE INDEX IF NOT EXISTS idx_metadata_raw_build ON metadata_raw_versions(build_id, version_number DESC);
CREATE INDEX IF NOT EXISTS idx_metadata_raw_sha ON metadata_raw_versions(raw_sha256);
CREATE INDEX IF NOT EXISTS idx_metadata_raw_current ON metadata_raw_versions(build_id, is_current) WHERE is_current = 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_build_identity ON build(vn_id, normalized_version, language, edition, distribution_platform);
