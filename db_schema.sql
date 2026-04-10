PRAGMA foreign_keys = OFF;

-- Canonical schema v1 (fresh initialization only).

CREATE TABLE series (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE organizations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE visual_novels (
    id INTEGER PRIMARY KEY,
    series_id INTEGER,
    title TEXT NOT NULL UNIQUE,
    canonical_slug TEXT,
    aliases TEXT,
    developer TEXT,
    publisher TEXT,
    release_status TEXT,
    content_rating TEXT,
    content_mode TEXT,
    description TEXT,
    source TEXT,
    status TEXT DEFAULT 'local',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE SET NULL
);

-- Transitional compatibility table for repository paths that still query `vn`.
CREATE TABLE vn (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL UNIQUE
);

CREATE TABLE vn_developers (
    vn_id INTEGER NOT NULL,
    org_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, org_id),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE vn_publishers (
    vn_id INTEGER NOT NULL,
    org_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, org_id),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE vn_aliases (
    vn_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    PRIMARY KEY (vn_id, alias),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE
);

CREATE TABLE vn_relationships (
    vn_id INTEGER NOT NULL,
    related_vn_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,
    source TEXT,
    PRIMARY KEY (vn_id, related_vn_id, relationship_type),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    FOREIGN KEY (related_vn_id) REFERENCES vn(id) ON DELETE CASCADE
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE vn_tags (
    vn_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, tag_id),
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

CREATE TABLE platforms (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE builds (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE vn_developers (
    vn_id INTEGER NOT NULL,
    org_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, org_id),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE vn_publishers (
    vn_id INTEGER NOT NULL,
    org_id INTEGER NOT NULL,
    PRIMARY KEY (vn_id, org_id),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE vn_aliases (
    vn_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    PRIMARY KEY (vn_id, alias),
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE vn_tags (
    vn_id INTEGER NOT NULL,
    version TEXT NOT NULL,
    normalized_version TEXT,
    build_type TEXT,
    release_type TEXT,
    distribution_model TEXT,
    distribution_platform TEXT,
    language TEXT,
    translator TEXT,
    edition TEXT,
    original_release_date TEXT,
    release_date TEXT,
    engine TEXT,
    engine_version TEXT,
    source TEXT,
    status TEXT DEFAULT 'local',
    archive_object_sha256 TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE
);

CREATE TABLE build_target_platforms (
    build_id INTEGER NOT NULL,
    platform_id INTEGER NOT NULL,
    PRIMARY KEY (build_id, platform_id),
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (platform_id) REFERENCES platforms(id) ON DELETE CASCADE
);

CREATE TABLE build_relations (
    from_build_id INTEGER NOT NULL,
    to_build_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    PRIMARY KEY (from_build_id, to_build_id, relation_type),
    FOREIGN KEY (from_build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (to_build_id) REFERENCES builds(id) ON DELETE CASCADE
);

CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER DEFAULT 0,
    mime_type TEXT,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE artifacts (
    artifact_id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL,
    artifact_type TEXT,
    platform TEXT,
    source_url TEXT,
    filename TEXT,
    sha256 TEXT NOT NULL,
    file_id INTEGER,
    file_object_sha256 TEXT,
    base_artifact_id INTEGER,
    release_date TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL,
    FOREIGN KEY (base_artifact_id) REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    UNIQUE (build_id, sha256)
);

CREATE TABLE artifact_files (
    artifact_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    path_in_artifact TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artifact_id, file_id),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE archive_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER,
    storage_path TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE metadata_objects (
    hash TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE metadata_versions (
    id INTEGER PRIMARY KEY,
    vn_id INTEGER NOT NULL,
    build_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (metadata_hash) REFERENCES metadata_objects(hash),
    FOREIGN KEY (parent_version_id) REFERENCES metadata_versions(id),
    UNIQUE (build_id, version_number)
);

CREATE TABLE artifact_metadata_objects (
    hash TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE artifact_metadata_versions (
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    FOREIGN KEY (metadata_hash) REFERENCES artifact_metadata_objects(hash),
    FOREIGN KEY (parent_version_id) REFERENCES artifact_metadata_versions(id),
    UNIQUE (artifact_id, version_number)
);

CREATE TABLE metadata_raw (
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER,
    source_file TEXT,
    raw_text TEXT NOT NULL,
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE SET NULL
);

CREATE TABLE metadata_extensions (
    build_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (build_id, key),
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE
);

CREATE INDEX idx_visual_novels_series_id ON visual_novels(series_id);
CREATE INDEX idx_builds_vn_id ON builds(vn_id);
CREATE UNIQUE INDEX idx_unique_build_identity
ON builds(
    vn_id,
    normalized_version,
    COALESCE(language, ''),
    COALESCE(release_type, ''),
    COALESCE(edition, '')
);
CREATE INDEX idx_builds_release_date ON builds(release_date);
CREATE INDEX idx_builds_language ON builds(language);
CREATE INDEX idx_artifacts_build_id ON artifacts(build_id);
CREATE INDEX idx_artifacts_sha256 ON artifacts(sha256);
CREATE INDEX idx_artifacts_file_object_sha ON artifacts(file_object_sha256);
CREATE INDEX idx_metadata_versions_build_current ON metadata_versions(build_id, is_current);
CREATE INDEX idx_metadata_raw_artifact_id ON metadata_raw(artifact_id);
CREATE INDEX idx_artifact_metadata_versions_current ON artifact_metadata_versions(artifact_id, is_current);
CREATE INDEX idx_archive_objects_storage_path ON archive_objects(storage_path);
