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
    canonical_slug TEXT UNIQUE,
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
    release_date TEXT,

    status TEXT DEFAULT 'processed',

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(vn_id, version),

    FOREIGN KEY (vn_id) REFERENCES visual_novels(id) ON DELETE CASCADE
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

-- =====================================================
-- 7. TAGS (Work-Level)
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
-- 8. ARCHIVE OBJECTS (Content-Addressed Storage)
-- =====================================================

CREATE TABLE IF NOT EXISTS archive_objects (
    sha256 TEXT PRIMARY KEY,
    file_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 9. METADATA OBJECTS (Immutable Blob Store)
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata_objects (
    hash TEXT PRIMARY KEY,                 -- sha256 of canonical metadata JSON
    schema_version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 10. METADATA VERSIONS (Version History Per VN)
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vn_id INTEGER NOT NULL,
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

    FOREIGN KEY (metadata_hash)
        REFERENCES metadata_objects(hash)
        ON DELETE RESTRICT,

    FOREIGN KEY (parent_version_id)
        REFERENCES metadata_versions(id)
        ON DELETE SET NULL
);

-- =====================================================
-- 11. UNIQUE CONSTRAINTS
-- =====================================================

-- Ensure only one current metadata version per VN
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_current_metadata
ON metadata_versions(vn_id)
WHERE is_current = 1;

-- Ensure version_number increments uniquely per VN
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_version_number
ON metadata_versions(vn_id, version_number);

-- =====================================================
-- 12. INDEXES FOR PERFORMANCE
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_metadata_versions_vn
ON metadata_versions(vn_id);

CREATE INDEX IF NOT EXISTS idx_metadata_versions_hash
ON metadata_versions(metadata_hash);