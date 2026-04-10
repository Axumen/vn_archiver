PRAGMA foreign_keys = ON;

-- Domain-first schema for VN archival.
-- Core flow: File -> Artifact (sha256) -> Build -> VN

CREATE TABLE IF NOT EXISTS vn (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
    id INTEGER PRIMARY KEY,
    vn_id INTEGER NOT NULL,
    version_string TEXT,
    release_type TEXT,
    language TEXT,
    platform TEXT,
    FOREIGN KEY (vn_id) REFERENCES vn(id) ON DELETE CASCADE,
    UNIQUE (vn_id, version_string, language, release_type, platform)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY,
    build_id INTEGER,
    sha256 TEXT NOT NULL,
    path TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    UNIQUE (build_id, sha256)
);

-- Optional pipeline/runtime state tracking outside identity model.
CREATE TABLE IF NOT EXISTS artifact_state (
    artifact_id INTEGER PRIMARY KEY,
    status TEXT,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metadata_raw (
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER,
    source_file TEXT,
    raw_text TEXT NOT NULL,
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id)
);

CREATE INDEX IF NOT EXISTS idx_builds_vn_id ON builds(vn_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_build_id ON artifacts(build_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha256 ON artifacts(sha256);
CREATE INDEX IF NOT EXISTS idx_artifact_state_status ON artifact_state(status);
