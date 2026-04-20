PRAGMA foreign_keys = ON;

-- Series identity
CREATE TABLE IF NOT EXISTS series (
    series_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Title identity (work-level)
CREATE TABLE IF NOT EXISTS title (
    title_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL UNIQUE,
    series_id INTEGER,
    aliases TEXT,
    release_status TEXT,
    content_rating TEXT,
    content_mode TEXT,
    content_type TEXT,
    description TEXT,
    source TEXT,
    original_release_date TEXT,
    FOREIGN KEY (series_id) REFERENCES series(series_id) ON DELETE SET NULL
);

-- Release identity (version-level)
CREATE TABLE IF NOT EXISTS release (
    release_id INTEGER PRIMARY KEY,
    title_id INTEGER NOT NULL,
    version TEXT NOT NULL,
    normalized_version TEXT GENERATED ALWAYS AS (lower(trim(version))) VIRTUAL,
    release_type TEXT,
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
    FOREIGN KEY (title_id) REFERENCES title(title_id) ON DELETE CASCADE
);

-- Deduplicated physical file identity
CREATE TABLE IF NOT EXISTS file (
    file_id INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER,
    filename TEXT,
    CHECK (length(sha256) = 64)
);

-- Release <-> File many-to-many association
CREATE TABLE IF NOT EXISTS release_file (
    release_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    original_filename TEXT,
    artifact_type TEXT,
    archived_at TEXT,
    PRIMARY KEY (release_id, file_id),
    FOREIGN KEY (release_id) REFERENCES release(release_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES file(file_id) ON DELETE CASCADE
);

-- Parsed metadata captured at file-attachment time (Title > Release > File workflow)
CREATE TABLE IF NOT EXISTS file_snapshot (
    metadata_id INTEGER PRIMARY KEY,
    release_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    metadata_version INTEGER NOT NULL,
    title TEXT,
    version TEXT,
    release_type TEXT,
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
    FOREIGN KEY (release_id, file_id) REFERENCES release_file(release_id, file_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS tag (
    tag_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_tag (
    title_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (title_id, tag_id),
    FOREIGN KEY (title_id) REFERENCES title(title_id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tag(tag_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS developer (
    developer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_developer (
    title_id INTEGER NOT NULL,
    developer_id INTEGER NOT NULL,
    PRIMARY KEY (title_id, developer_id),
    FOREIGN KEY (title_id) REFERENCES title(title_id) ON DELETE CASCADE,
    FOREIGN KEY (developer_id) REFERENCES developer(developer_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_publisher (
    title_id INTEGER NOT NULL,
    publisher_id INTEGER NOT NULL,
    PRIMARY KEY (title_id, publisher_id),
    FOREIGN KEY (title_id) REFERENCES title(title_id) ON DELETE CASCADE,
    FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS language (
    language_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS release_language (
    release_id INTEGER NOT NULL,
    language_id INTEGER NOT NULL,
    PRIMARY KEY (release_id, language_id),
    FOREIGN KEY (release_id) REFERENCES release(release_id) ON DELETE CASCADE,
    FOREIGN KEY (language_id) REFERENCES language(language_id) ON DELETE CASCADE
);

-- Content-addressed metadata versioning with version chains
CREATE TABLE IF NOT EXISTS revision (
    revision_id INTEGER PRIMARY KEY,
    release_id INTEGER NOT NULL,
    file_id INTEGER,
    raw_json TEXT NOT NULL,
    raw_sha256 TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 0,
    parent_version_id INTEGER,
    change_note TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (release_id) REFERENCES release(release_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES file(file_id) ON DELETE SET NULL,
    FOREIGN KEY (parent_version_id) REFERENCES revision(revision_id) ON DELETE SET NULL,
    UNIQUE (release_id, version_number)
);

-- Upload tracking: content-addressed archive objects in cloud storage
CREATE TABLE IF NOT EXISTS cloud_archive (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    CHECK (length(sha256) = 64)
);

-- Upload tracking: content-addressed metadata sidecar objects in cloud storage
CREATE TABLE IF NOT EXISTS cloud_sidecar (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    CHECK (length(sha256) = 64)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_title_series ON title(series_id);
CREATE INDEX IF NOT EXISTS idx_release_title ON release(title_id);
CREATE INDEX IF NOT EXISTS idx_release_type ON release(release_type);
CREATE INDEX IF NOT EXISTS idx_file_sha256 ON file(sha256);
CREATE INDEX IF NOT EXISTS idx_file_snapshot_pair ON file_snapshot(release_id, file_id);
CREATE INDEX IF NOT EXISTS idx_title_tag_title ON title_tag(title_id);
CREATE INDEX IF NOT EXISTS idx_title_tag_tag ON title_tag(tag_id);
CREATE INDEX IF NOT EXISTS idx_title_developer_title ON title_developer(title_id);
CREATE INDEX IF NOT EXISTS idx_title_developer_dev ON title_developer(developer_id);
CREATE INDEX IF NOT EXISTS idx_title_publisher_title ON title_publisher(title_id);
CREATE INDEX IF NOT EXISTS idx_title_publisher_pub ON title_publisher(publisher_id);
CREATE INDEX IF NOT EXISTS idx_release_language_release ON release_language(release_id);
CREATE INDEX IF NOT EXISTS idx_release_language_lang ON release_language(language_id);
CREATE INDEX IF NOT EXISTS idx_revision_release ON revision(release_id, version_number DESC);
CREATE INDEX IF NOT EXISTS idx_revision_sha ON revision(raw_sha256);
CREATE INDEX IF NOT EXISTS idx_revision_current ON revision(release_id, is_current) WHERE is_current = 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_release_identity ON release(title_id, normalized_version, language, edition, distribution_platform);
