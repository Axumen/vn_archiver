PRAGMA foreign_keys = ON;

-- =====================================================
-- 1. SERIES
-- =====================================================

CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    status TEXT DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 2. VISUAL NOVELS (Work Identity)
-- =====================================================

CREATE TABLE IF NOT EXISTS visual_novels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER,
    
    title TEXT NOT NULL,
    canonical_slug TEXT UNIQUE,
    aliases TEXT,           -- Stores JSON array or comma-separated alternate titles
    
    developer TEXT,
    publisher TEXT,
    description TEXT,
    release_status TEXT,
    content_rating TEXT,
    source TEXT,
    
    status TEXT DEFAULT 'local', -- Added to track cloud upload status
    
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (series_id) REFERENCES series(id)
);

-- =====================================================
-- 3. CANON RELATIONSHIPS
-- =====================================================

CREATE TABLE IF NOT EXISTS canon_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_vn_id INTEGER NOT NULL,
    child_vn_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (parent_vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (child_vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_canon_parent
ON canon_relationships(parent_vn_id);

CREATE INDEX IF NOT EXISTS idx_canon_child
ON canon_relationships(child_vn_id);

-- =====================================================
-- 4. BUILDS (Version Layer)
-- =====================================================

CREATE TABLE IF NOT EXISTS builds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vn_id INTEGER NOT NULL,

    version TEXT NOT NULL,
    normalized_version TEXT NOT NULL,
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
    archive_object_sha256 TEXT, -- Uploaded bundle object (CAS pointer)

    status TEXT DEFAULT 'local', -- Updated default to 'local' for consistency

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (archive_object_sha256) REFERENCES archive_objects(sha256) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_build_release
ON builds(
    vn_id,
    normalized_version,
    COALESCE(language, ''),
    COALESCE(release_type, COALESCE(build_type, '')),
    COALESCE(edition, ''),
    COALESCE(distribution_platform, '')
);

CREATE TABLE IF NOT EXISTS build_relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_build_id INTEGER NOT NULL,
    to_build_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (from_build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (to_build_id) REFERENCES builds(id) ON DELETE CASCADE,
    UNIQUE(from_build_id, to_build_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_build_relations_from ON build_relations(from_build_id);
CREATE INDEX IF NOT EXISTS idx_build_relations_to ON build_relations(to_build_id);
CREATE INDEX IF NOT EXISTS idx_build_relations_type ON build_relations(relation_type);

-- =====================================================
-- 5. TARGET PLATFORMS (Normalized)
-- =====================================================

CREATE TABLE IF NOT EXISTS target_platforms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS build_target_platforms (
    build_id INTEGER NOT NULL,
    platform_id INTEGER NOT NULL,

    PRIMARY KEY (build_id, platform_id),

    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (platform_id) REFERENCES target_platforms(id) ON DELETE CASCADE
);

-- =====================================================
-- 6. ARCHIVES (Content-Addressable Per Build)
-- =====================================================

CREATE TABLE IF NOT EXISTS archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id INTEGER NOT NULL,

    sha256 TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,

    metadata_json TEXT NOT NULL,
    metadata_version INTEGER NOT NULL,

    status TEXT DEFAULT 'archived',

    archived_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uploaded_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(build_id, sha256),

    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_archives_build_id
ON archives(build_id);

CREATE INDEX IF NOT EXISTS idx_archives_sha256
ON archives(sha256);

CREATE INDEX IF NOT EXISTS idx_builds_archive_object_sha256
ON builds(archive_object_sha256);


-- =====================================================
-- 7. ARTIFACTS (Build-Attached File Objects)
-- =====================================================

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id INTEGER NOT NULL,
    artifact_type TEXT NOT NULL,
    platform TEXT,
    source_url TEXT,
    filename TEXT,
    sha256 TEXT NOT NULL,
    file_id INTEGER,
    file_object_sha256 TEXT,
    base_artifact_id INTEGER,
    release_date TEXT,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL,
    FOREIGN KEY (file_object_sha256) REFERENCES archive_objects(sha256) ON DELETE SET NULL,
    FOREIGN KEY (base_artifact_id) REFERENCES artifacts(artifact_id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_artifacts_build_sha
ON artifacts(build_id, sha256);

CREATE INDEX IF NOT EXISTS idx_artifacts_build
ON artifacts(build_id);

CREATE INDEX IF NOT EXISTS idx_artifacts_base
ON artifacts(base_artifact_id);

CREATE INDEX IF NOT EXISTS idx_artifacts_type
ON artifacts(artifact_type);

CREATE INDEX IF NOT EXISTS idx_artifacts_file_object_sha
ON artifacts(file_object_sha256);

CREATE INDEX IF NOT EXISTS idx_artifacts_file_id
ON artifacts(file_id);

-- Derived artifacts (patch/mod/hotfix/translation_patch) must link to a base artifact.
CREATE TRIGGER IF NOT EXISTS trg_artifacts_require_base_insert
BEFORE INSERT ON artifacts
FOR EACH ROW
WHEN LOWER(COALESCE(NEW.artifact_type, '')) IN ('patch', 'mod', 'hotfix', 'translation_patch')
     AND NEW.base_artifact_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'derived artifacts require base_artifact_id');
END;

CREATE TRIGGER IF NOT EXISTS trg_artifacts_require_base_update
BEFORE UPDATE ON artifacts
FOR EACH ROW
WHEN LOWER(COALESCE(NEW.artifact_type, '')) IN ('patch', 'mod', 'hotfix', 'translation_patch')
     AND NEW.base_artifact_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'derived artifacts require base_artifact_id');
END;

-- =====================================================
-- 8. TAGS (Work-Level)
-- =====================================================

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vn_tags (
    vn_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,

    PRIMARY KEY (vn_id, tag_id),

    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- =====================================================
-- 9. ARCHIVE OBJECTS (Content-Addressed Storage)
-- =====================================================

CREATE TABLE IF NOT EXISTS archive_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256 TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL,
    mime_type TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artifact_files (
    artifact_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    path_in_artifact TEXT NOT NULL DEFAULT '',
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (artifact_id, file_id, path_in_artifact),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifact_files_artifact ON artifact_files(artifact_id);
CREATE INDEX IF NOT EXISTS idx_artifact_files_file ON artifact_files(file_id);

-- =====================================================
-- 10. METADATA FILE OBJECTS (Content-Addressed Storage)
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata_file_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 11. METADATA OBJECTS (Immutable Blob Store)
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata_objects (
    hash TEXT PRIMARY KEY,                 -- sha256 of canonical metadata JSON
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Artifact-scoped immutable metadata blobs
CREATE TABLE IF NOT EXISTS artifact_metadata_objects (
    hash TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 12. METADATA VERSIONS (Version History Per Build)
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vn_id INTEGER NOT NULL,
    build_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    status TEXT DEFAULT 'approved',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (vn_id)
        REFERENCES visual_novels(id)
        ON DELETE CASCADE,

    FOREIGN KEY (build_id)
        REFERENCES builds(id)
        ON DELETE CASCADE,

    FOREIGN KEY (metadata_hash)
        REFERENCES metadata_objects(hash)
        ON DELETE RESTRICT,

    FOREIGN KEY (parent_version_id)
        REFERENCES metadata_versions(id)
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS artifact_metadata_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id INTEGER NOT NULL,
    metadata_hash TEXT NOT NULL,
    parent_version_id INTEGER,
    version_number INTEGER NOT NULL,
    change_note TEXT,
    status TEXT DEFAULT 'approved',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (artifact_id)
        REFERENCES artifacts(artifact_id)
        ON DELETE CASCADE,

    FOREIGN KEY (metadata_hash)
        REFERENCES artifact_metadata_objects(hash)
        ON DELETE RESTRICT,

    FOREIGN KEY (parent_version_id)
        REFERENCES artifact_metadata_versions(id)
        ON DELETE SET NULL
);


-- =====================================================
-- 13. UNIQUE CONSTRAINTS
-- =====================================================

-- Ensure only one current metadata version per build
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_current_metadata
ON metadata_versions(build_id)
WHERE is_current = 1;

-- Ensure version_number increments uniquely per build
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_version_number
ON metadata_versions(build_id, version_number);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_current_artifact_metadata
ON artifact_metadata_versions(artifact_id)
WHERE is_current = 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_artifact_version_number
ON artifact_metadata_versions(artifact_id, version_number);


-- =====================================================
-- 14. INDEXES FOR PERFORMANCE
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_metadata_versions_vn
ON metadata_versions(vn_id);

CREATE INDEX IF NOT EXISTS idx_metadata_versions_build
ON metadata_versions(build_id);

CREATE INDEX IF NOT EXISTS idx_metadata_versions_hash
ON metadata_versions(metadata_hash);

CREATE INDEX IF NOT EXISTS idx_artifact_metadata_versions_artifact
ON artifact_metadata_versions(artifact_id);

CREATE INDEX IF NOT EXISTS idx_artifact_metadata_versions_hash
ON artifact_metadata_versions(metadata_hash);


-- =====================================================
-- 15. TRIGGERS FOR ARCHIVE-ID DRIVEN CASCADE CLEANUP
-- =====================================================

-- If the last archive row for a build is deleted, remove the build.
-- This enables archive-id deletion to cascade through build-linked tables.
CREATE TRIGGER IF NOT EXISTS trg_archives_delete_last_archive_prune_build
AFTER DELETE ON archives
FOR EACH ROW
WHEN NOT EXISTS (
    SELECT 1 FROM archives a WHERE a.build_id = OLD.build_id
)
BEGIN
    DELETE FROM builds WHERE id = OLD.build_id;
END;

-- Remove orphan metadata_objects after metadata_versions are deleted.
CREATE TRIGGER IF NOT EXISTS trg_metadata_versions_delete_prune_objects
AFTER DELETE ON metadata_versions
FOR EACH ROW
BEGIN
    DELETE FROM metadata_objects
    WHERE hash = OLD.metadata_hash
      AND NOT EXISTS (
          SELECT 1
          FROM metadata_versions mv
          WHERE mv.metadata_hash = OLD.metadata_hash
      );
END;

-- Remove orphan artifact_metadata_objects after artifact metadata versions are deleted.
CREATE TRIGGER IF NOT EXISTS trg_artifact_metadata_versions_delete_prune_objects
AFTER DELETE ON artifact_metadata_versions
FOR EACH ROW
BEGIN
    DELETE FROM artifact_metadata_objects
    WHERE hash = OLD.metadata_hash
      AND NOT EXISTS (
          SELECT 1
          FROM artifact_metadata_versions amv
          WHERE amv.metadata_hash = OLD.metadata_hash
      );
END;
