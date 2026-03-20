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
    build_type TEXT,
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
    version,
    COALESCE(language, ''),
    COALESCE(build_type, ''),
    COALESCE(edition, ''),
    COALESCE(distribution_platform, '')
);

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
    filename TEXT,
    sha256 TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    base_artifact_id INTEGER,
    release_date TEXT,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
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

-- =====================================================
-- 14. INDEXES FOR PERFORMANCE
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_metadata_versions_vn
ON metadata_versions(vn_id);

CREATE INDEX IF NOT EXISTS idx_metadata_versions_build
ON metadata_versions(build_id);

CREATE INDEX IF NOT EXISTS idx_metadata_versions_hash
ON metadata_versions(metadata_hash);

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
