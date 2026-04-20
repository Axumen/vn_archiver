# VN Archiver — Domain Logic (Canonical Architecture)

## Purpose
This document outlines the **current domain model** and canonical data schema for the VN Archiver.
The system is built on a strict hierarchical structure, treating versioned releases as structured domain aggregates rather than standalone file blobs. It focuses on representing immutable files attached to descriptive release entities under a defined title.

---

## Core Principle
The core organizational hierarchy follows:
**Title** → **Release** → **File**

- **Title**: The product identity boundary.
- **Release**: The versioned release and distribution unit.
- **File**: The immutable physical data identity (content-addressed).

---

## Domain Entities

### 1) Title
Represents the highest-level product identity of a visual novel. It aggregates overarching attributes that span all versions or editions.

**Database Table**: `title`
- **Required fields**: `title_id`, `title` (canonical_title)
- **Attribute fields**: `series_id`, `aliases`, `description`, `source`, `original_release_date`, `release_status`, `content_rating`, `content_mode`, `content_type`
- **Tied properties**: Developers, Publishers, Tags (linked via normalized `title_developer`, `title_publisher`, `title_tag` tables).

### 2) Release
Represents a specific version, build, or edition of a `Title`.
Releases act as the primary queryable boundaries for users retrieving metadata.

**Database Table**: `release`
- **Required fields**: `release_id`, `title_id`, `version`
- **Computed fields**: `normalized_version`
- **Classification fields**:
  - `release_type` (e.g., full, patch, demo)
  - `edition` (e.g., standard, limited)
  - `language`
  - `distribution_platform`
  - `distribution_model`
  - `target_platform`
  - `engine` / `engine_version`
- **Lifecycle fields**: `release_date`, `translator`, `notes`, `change_note`.

*Note: The identity of a Release is generally guaranteed uniquely by a combination of `title_id`, `normalized_version`, `language`, `edition`, and `distribution_platform`.*

### 3) File
Represents the physical, deduplicated object binary, identified via content addressing (hash-based).
Files are strictly non-semantic entities. They hold no domain logic beyond their physical storage footprint.

**Database Table**: `file`
- **Required fields**: `file_id`, `sha256`
- **Optional/Calculated fields**: `size_bytes`, `filename`.

---

## Linkages & Metadata Sub-Entities

To map physical files to the logical `Release` aggregate, and capture context at the time of archiving, several auxiliary tables are natively supported:

- **Release-File Mapping (`release_file`)**: Maps `file_id` to `release_id`. Tracks the original filename and artifact type.
- **File Snapshot (`file_snapshot`)**: Captures flattened, denormalized metadata properties specifically tied to the exact moment a file was archived into a given release.
- **Revisions (`revision`)**: Supports content-addressed metadata version management. Stores a timeline of metadata iterations (`raw_json`, `raw_sha256`), tracking changes dynamically for each release.
- **Series Identity (`series`)**: A high-level entity capable of grouping multiple related `Title` records logically.
- **Language Sub-table (`release_language`)**: Associates releases dynamically with locale codes.

---

## Controlled Vocabulary Constraints

The canonical schema normalizes categorization to avoid unstructured free-text anomalies. These fields are defined carefully in processing steps:

- **`release_type`**: Differentiates between 'full' games, patches, demos, fan-discs, DLCs, etc.
- **`language`**: Expected to follow ISO codes (normalized using the domain layer ingestion constraints).
- **`normalized_version`**: Standardized stripped versions heavily used for DB-level uniqueness checks (e.g., stripping 'v' prefixes).

---

## Ingestion Sequence (Domain Layer)

All file processing follows a centralized orchestration enforced by `domain_layer.py`:

1. **Resolve Title**: Parses the core identity payload and attempts to fetch an existing `Title` or create a new one using overarching metadata properties.
2. **Resolve Release**: Maps to an existing database `Release` based on `title_id` and standardized version heuristics.
3. **Ingest Files**: Extracts local physical attributes (`sha256`, size, path).
4. **Create File Attachments**:
   - Commits independent `File` deduplication entries.
   - Maps them to the `Release` via the junction table (`release_file`).
5. **Metadata Revisioning**: Stores raw json metadata payload as a `revision` linked to the primary release and explicitly snapshotted to the attached files.
6. **Yield Result**: The `domain_layer` passes a unified `IngestionResult` back to the orchestration service (like the CLI/TUI), guaranteeing synchronized identity definitions (`title_id`, `release_id`, `metadata_version_number`).

---

## Canonical Distinctions

- **Title ≠ Release**: A Title defines semantics; a Release defines execution and runtime.
- **Release ≠ File**: A Release has semantic scope; a File is just an immutable hash.
- Files cannot redefine release semantics. Releases cannot inherently deduce context solely without Title bounds.

All application design must operate down the tree: `Title` → `Release` → `File`.
