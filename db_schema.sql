PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES (1);

CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Canonical VN table used by current ingestion/upsert flows.
CREATE TABLE IF NOT EXISTS visual_novels (
    id INTEGER PRIMARY KEY,
    series_id INTEGER,
    title TEXT NOT NULL UNIQUE,
    canonical_slug TEXT,
    aliases TEXT,
    developer TEXT,
    publisher TEXT,
    description TEXT,
    release_status TEXT,
    content_rating TEXT,
    content_mode TEXT,
    content_type TEXT,
    source TEXT,
    status TEXT NOT NULL DEFAULT 'local',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE SET NULL
);

-- Backward-compatible alias expected by domain repository and tests.
CREATE VIEW IF NOT EXISTS vn AS
SELECT
    id,
    series_id,
    title,
    canonical_slug,
    aliases,
    developer,
    publisher,
    description,
    release_status,
    content_rating,
    content_mode,
    content_type,
    source,
    status,
    created_at,
    updated_at
FROM visual_novels;

CREATE TRIGGER IF NOT EXISTS vn_insert_instead
INSTEAD OF INSERT ON vn
BEGIN
    INSERT INTO visual_novels (
        series_id,
        title,
        canonical_slug,
        aliases,
        developer,
        publisher,
        description,
        release_status,
        content_rating,
        content_mode,
        content_type,
        source,
        status
    ) VALUES (
        NEW.series_id,
        NEW.title,
        NEW.canonical_slug,
        NEW.aliases,
        NEW.developer,
        NEW.publisher,
        NEW.description,
        NEW.release_status,
        NEW.content_rating,
        NEW.content_mode,
        NEW.content_type,
        NEW.source,
        COALESCE(NEW.status, 'local')
    );
END;

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vn_tags (
    vn_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, tag_id),
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS builds (
    id INTEGER PRIMARY KEY,
    vn_id INTEGER NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0',
    normalized_version TEXT NOT NULL DEFAULT '1.0',
    version_string TEXT,
    build_type TEXT,
    release_type TEXT,
    release_status TEXT,
    distribution_model TEXT,
    distribution_platform TEXT,
    platform TEXT,
    language TEXT,
    translator TEXT,
    edition TEXT,
    original_release_date TEXT,
    release_date TEXT,
    engine TEXT,
    engine_version TEXT,
    source TEXT,
    notes TEXT,
    status TEXT NOT NULL DEFAULT 'local',
    archive_object_sha256 TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE
);

CREATE TRIGGER IF NOT EXISTS builds_sync_compat_after_insert
AFTER INSERT ON builds
BEGIN
    UPDATE builds
    SET
        version_string = COALESCE(NULLIF(version_string, ''), normalized_version, LOWER(TRIM(version))),
        normalized_version = COALESCE(NULLIF(normalized_version, ''), LOWER(TRIM(version)), version_string),
        distribution_platform = COALESCE(NULLIF(distribution_platform, ''), platform),
        platform = COALESCE(NULLIF(platform, ''), distribution_platform),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS builds_sync_compat_after_update
AFTER UPDATE ON builds
BEGIN
    UPDATE builds
    SET
        version_string = COALESCE(NULLIF(version_string, ''), normalized_version, LOWER(TRIM(version))),
        normalized_version = COALESCE(NULLIF(normalized_version, ''), LOWER(TRIM(version)), version_string),
        distribution_platform = COALESCE(NULLIF(distribution_platform, ''), platform),
        platform = COALESCE(NULLIF(platform, ''), distribution_platform),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TABLE IF NOT EXISTS target_platforms (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS build_target_platforms (
    build_id INTEGER NOT NULL,
    platform_id INTEGER NOT NULL,
    PRIMARY KEY (build_id, platform_id),
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (platform_id) REFERENCES target_platforms(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS build_relations (
    from_build_id INTEGER NOT NULL,
    to_build_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    PRIMARY KEY (from_build_id, to_build_id, relation_type),
    FOREIGN KEY (from_build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (to_build_id) REFERENCES builds(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS canon_relationships (
    parent_vn_id INTEGER NOT NULL,
    child_vn_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,
    PRIMARY KEY (parent_vn_id, child_vn_id, relationship_type),
    FOREIGN KEY (parent_vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (child_vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    mime_type TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id INTEGER PRIMARY KEY,
    id INTEGER GENERATED ALWAYS AS (artifact_id) VIRTUAL,
    build_id INTEGER NOT NULL,
    artifact_type TEXT NOT NULL DEFAULT 'game_archive',
    type TEXT GENERATED ALWAYS AS (artifact_type) VIRTUAL,
    platform TEXT,
    source_url TEXT,
    filename TEXT,
    path TEXT GENERATED ALWAYS AS (filename) VIRTUAL,
    sha256 TEXT NOT NULL,
    file_id INTEGER,
    file_object_sha256 TEXT,
    base_artifact_id INTEGER,
    release_date TEXT,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL,
    FOREIGN KEY (base_artifact_id) REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    UNIQUE (build_id, sha256)
);

CREATE TABLE IF NOT EXISTS artifact_files (
    artifact_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    path_in_artifact TEXT NOT NULL DEFAULT '',
    is_primary INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artifact_id, file_id, path_in_artifact),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS archives (
    id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    metadata_version INTEGER,
    status TEXT NOT NULL DEFAULT 'local',
    uploaded_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    UNIQUE (build_id, sha256)
);

CREATE TABLE IF NOT EXISTS archive_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata_objects (
    hash TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata_versions (
    id INTEGER PRIMARY KEY,
    vn_id INTEGER NOT NULL,
    build_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (metadata_hash) REFERENCES metadata_objects(hash) ON DELETE CASCADE,
    FOREIGN KEY (parent_version_id) REFERENCES metadata_versions(id) ON DELETE SET NULL,
    UNIQUE (build_id, version_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_versions_current_per_build
ON metadata_versions(build_id) WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS artifact_metadata_objects (
    hash TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artifact_metadata_versions (
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    FOREIGN KEY (metadata_hash) REFERENCES artifact_metadata_objects(hash) ON DELETE CASCADE,
    FOREIGN KEY (parent_version_id) REFERENCES artifact_metadata_versions(id) ON DELETE SET NULL,
    UNIQUE (artifact_id, version_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_artifact_metadata_versions_current
ON artifact_metadata_versions(artifact_id) WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS metadata_raw (
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER,
    source_file TEXT,
    raw_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_builds_vn_id ON builds(vn_id);
CREATE INDEX IF NOT EXISTS idx_builds_lookup ON builds(vn_id, normalized_version, language, release_type, edition, distribution_platform);
CREATE INDEX IF NOT EXISTS idx_builds_version_string_lookup ON builds(vn_id, version_string, language, release_type, platform);
CREATE INDEX IF NOT EXISTS idx_archives_build_id ON archives(build_id);
CREATE INDEX IF NOT EXISTS idx_archives_sha256 ON archives(sha256);
CREATE INDEX IF NOT EXISTS idx_artifacts_build_id ON artifacts(build_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha256 ON artifacts(sha256);
CREATE INDEX IF NOT EXISTS idx_artifacts_file_object_sha ON artifacts(file_object_sha256);
CREATE INDEX IF NOT EXISTS idx_artifacts_base_artifact_id ON artifacts(base_artifact_id);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);

COMMIT;
