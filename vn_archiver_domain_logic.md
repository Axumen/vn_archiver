# VN Archiver — Domain Logic (Canonical Architecture)

## Purpose

This document defines the **target domain model** for VN Archiver.

It is intentionally **greenfield-first**: architecture correctness is prioritized over backward compatibility or incremental migration concerns.

The system must model releases as domain entities, not as hash-only blobs.

---

## Core Principle

Do **not** design around:

`file → hash → stored`

Design around:

`VN → Build → Artifact → File`

Where:

- **Files** are binary facts (content identity).
- **Artifacts** are distributable packages or installable units.
- **Builds** are release semantics and lineage.
- **VN** is the product identity boundary.

---

## Domain Entities (Required)

### 1) VN

Represents the title-level identity.

Required fields:

- `vn_id`
- `canonical_title`
- `developer`
- `publisher` (optional)
- `aliases` (optional)

### 2) Build

Represents a release of a VN.

Required fields:

- `build_id`
- `vn_id`
- `version_string`
- `normalized_version` (for deterministic ordering/matching)
- `release_type` (controlled vocabulary)
- `release_status` (controlled vocabulary)
- `release_date` (optional, but strongly recommended)

Build is the **semantic unit** users query when asking “which version/release is this?”

### 3) BuildRelation

Represents directed relationships between builds.

Required fields:

- `from_build_id`
- `to_build_id`
- `relation_type` (controlled vocabulary)

Minimum relation types:

- `depends_on`
- `supersedes`
- `branch_of`
- `equivalent_to`

This is mandatory to model patch chains, forks, and release lineage.

### 4) Artifact

Represents a concrete distribution of a build (zip, installer, patch package, etc.).

Required fields:

- `artifact_id`
- `build_id`
- `artifact_type` (controlled vocabulary)
- `platform` (controlled vocabulary)
- `source_url` (if known)
- `acquired_at` (timestamp)
- `acquisition_method`
- `trust_level`

An artifact is **not** a file. It is a packaging/provenance object.

### 5) File

Represents deduplicated binary identity.

Required fields:

- `file_id`
- `sha256`
- `size_bytes`
- `mime_type` (optional)
- `first_seen_at`

File identity must be reusable across any number of artifacts/builds.

### 6) ArtifactFile

Represents artifact composition (many-to-many between artifacts and files).

Required fields:

- `artifact_id`
- `file_id`
- `path_in_artifact` (optional but recommended)
- `is_primary`

This table is required for unpacked/multi-file distributions and for cross-artifact file reuse.

---

## Required Cardinality Rules

- One `VN` has many `Build`s.
- One `Build` has many `Artifact`s.
- One `Artifact` has many `File`s (via `ArtifactFile`).
- One `File` can appear in many `Artifact`s.
- `BuildRelation` is many-to-many self-reference over `Build`.

If these cardinalities are not present, the model is incomplete.

---

## Controlled Vocabulary Requirements

Free-text should not be the long-term source of truth for critical classification.

At minimum, define controlled vocabularies for:

- `release_type`
- `release_status`
- `artifact_type`
- `platform`
- `build_relation_type`

Examples (non-exhaustive):

- `release_type`: full, patch, demo, trial, fandisc, hotfix, april_fools
- `release_status`: stable, prerelease, discontinued, unknown
- `artifact_type`: archive, installer, patch_bundle, executable
- `platform`: windows, linux, macos, android, web

---

## Ingestion Decision Model

Ingestion must follow this sequence:

1. **File identity:** compute hash and resolve/create `File`.
2. **Artifact identity:** resolve/create `Artifact` with provenance.
3. **Composition mapping:** write `ArtifactFile` rows.
4. **Build resolution:** resolve/create `Build` with normalized version + taxonomy.
5. **Lineage resolution:** resolve/create `BuildRelation` links.
6. **VN resolution:** resolve/create canonical VN identity.

Hash equality is only step 1. It must not decide release semantics by itself.

---

## Non-Negotiable Distinctions

- `Build ≠ Artifact`
- `Artifact ≠ File`
- `BuildRelation` is first-class, not inferred ad hoc.
- Domain queries must traverse VN/build/artifact relationships, not only hashes.

---

## Example

Input artifacts:

- `MyVN_v1.zip`
- `MyVN_v2_patch.zip`
- `MyVN_aprilfools.zip`

Canonical interpretation:

- `VN`: MyVN
- `Build`: v1 (release_type=full)
- `Build`: v2 (release_type=patch)
- `Build`: april_fools (release_type=april_fools)
- `BuildRelation`: v2 `depends_on` v1
- `Artifact`s: one per acquired package with provenance
- `File`s: deduplicated binaries linked through `ArtifactFile`

---

## Success Criteria

The architecture is correct when the system can answer all of these directly:

- “Which build does this artifact represent?”
- “What does this patch depend on?”
- “Which builds supersede this one?”
- “Where has this exact file appeared across releases?”
- “Which artifacts belong to VN X on platform Y?”

If a query requires bypassing domain entities and relying only on file hashes, the model is underspecified.
