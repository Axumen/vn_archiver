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

### 3) Artifact

Represents a concrete distribution of a build (zip, installer, patch package, etc.).

Required fields:

- `artifact_id`
- `build_id`
- `artifact_type` (controlled vocabulary)
- `platform` (controlled vocabulary)
- `source_url` (if known)

An artifact is **not** a file. It is a packaging/provenance object.

### 4) File

Represents deduplicated binary identity.

Required fields:

- `file_id`
- `sha256`
- `size_bytes`
- `mime_type` (optional)
- `first_seen_at`

File identity must be reusable across any number of artifacts/builds.

---

## Required Cardinality Rules

- One `VN` has many `Build`s.
- One `Build` has many `Artifact`s.
- One `Artifact` has many `File`s.
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

1. **Ingest files independently:** ZIP becomes an `Artifact` candidate keyed by sha256.
2. **Parse metadata independently:** YAML/JSON sidecar becomes a structured metadata record (not yet VN/Build identity).
3. **Pair artifact + metadata:** if pairing fails, mark the artifact workflow status as `unresolved`.
4. **Resolve VN:** use metadata title to resolve/create VN identity.
5. **Resolve Build:** resolve/create Build from normalized version/language/release keys.
6. **Link Artifact → Build:** attach the artifact to the resolved build.
7. **Split metadata persistence:** `title`/`creator` belong to VN-level fields; `version`/`language`/`release_type` belong to Build-level fields.
8. **Mark completion:** successful pair+resolution transitions artifact workflow status to `classified`.

Hash equality is only stage 1. It must not decide release semantics by itself.

---

## Non-Negotiable Distinctions

- `Build ≠ Artifact`
- `Artifact ≠ File`
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
- `Artifact`s: one per acquired package with provenance

---

## Success Criteria

The architecture is correct when the system can answer all of these directly:

- “Which build does this artifact represent?”
- “What does this patch depend on?”
- “Which builds supersede this one?”
- “Where has this exact file appeared across releases?”
- “Which artifacts belong to VN X on platform Y?”

If a query requires bypassing domain entities and relying only on file hashes, the model is underspecified.
