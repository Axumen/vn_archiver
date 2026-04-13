# VN Archiver — Domain Logic (Simplified Canonical Architecture)

## Purpose

This document defines the **target domain model** for VN Archiver.

The architecture is **build-centric** and models releases as structured domain entities rather than file-driven blobs.

The system must represent:
The system must represent:
- Title identity
- Versioned releases
- Physical files

---

## Core Principle

Do **not** design around:

file → hash → stored

Design around:

Title → Release → File

Where:

- **Title** is the product identity boundary  
- **Release** is the release and distribution unit  
- **File** is the physical binary identity  

---

## Domain Entities (Required)

---

### 1) Title

Represents the **title-level identity** of a visual novel.

This is the highest-level grouping and remains stable across all versions.

#### Required fields:

- title_id
- canonical_title

#### Optional fields:

- developer
- publisher
- aliases
- series
- series_description
- release_status
- content_rating
- content_mode
- content_type
- description
- source
- tags
- original_release_date

---

### 2) Release

Represents a **specific release/version** of a visual novel.

A Release is the **primary unit of querying and classification**.

A Release encapsulates:
- version identity
- release lifecycle
- distribution metadata
- technical/runtime characteristics

#### Required fields:

- release_id
- title_id
- version_string
- normalized_version

#### Lifecycle fields:

- release_type (controlled vocabulary)
- release_status (controlled vocabulary)
- release_date (optional but recommended)

#### Access / Distribution:

- access_model (free, paid, freemium, subscription, etc.)
- distribution_platform (Steam, DLsite, Itch, etc.)

#### Content:

- language
- translator
- edition

#### Technical:

- engine
- engine_version
- target_platform

#### Additional:

- notes (optional)

---

### 3) File

Represents a **deduplicated binary object**.

Files are immutable and identified by content hash.

They are **not semantic entities** and carry no domain meaning beyond storage identity.

#### Required fields:

- file_id
- sha256
- size_bytes

#### Optional fields:

- filename

---

## Required Cardinality Rules

## Required Cardinality Rules

- One Title has many Releases  
- One Release has many Files  
- One File can belong to many Releases  

- ReleaseRelation is a many-to-many self-reference over Release

---

## ReleaseRelation (Required)

Represents relationships between releases.

Used for modeling:
- updates
- dependencies
- alternate releases

#### Fields:

- from_release_id
- to_release_id
- relation_type (controlled vocabulary)

#### Example relation types:

- supersedes
- depends_on
- variant_of
- continuation_of

---

## Controlled Vocabulary Requirements

Free-text must not be used for core classification fields.

At minimum, define controlled vocabularies for:

---

### release_type

Describes the nature of the release.

Examples:

- full
- patch
- demo
- trial
- fandisc
- hotfix
- april_fools

---

### release_status

Describes the stability or lifecycle of the release.

Examples:

- stable
- beta
- alpha
- prerelease
- discontinued
- unknown

---

### access_model

Describes how the release is obtained.

Examples:

- free
- paid
- freemium
- subscription

---

### target_platform

Examples:

- windows
- linux
- macos
- android
- web

---

## Ingestion Decision Model

Ingestion must follow this sequence:

---

### 1. Ingest Files

- Compute sha256
- Create or reuse File record
- Store independently of metadata

---

### 2. Parse Metadata

- Read YAML/JSON metadata
- Store as structured data
- Do not assign domain identity yet

---

### 3. Resolve Title

- Match or create Title using:
  - title
  - aliases (if needed)

---

### 4. Resolve Release

- Match or create Release using:
  - version
  - normalized_version
  - release_type
  - language (if relevant)

---

### 5) Link Files to Release

- Attach all ingested files to the resolved Release

---

### 6) Apply Metadata Mapping

- Title-level fields → Title
- Release-level fields → Release
- File-level fields → File

---

### 7) Mark Completion

- Release is considered classified once:
  - Title is resolved
  - Release is resolved
  - Files are linked

---

## Non-Negotiable Distinctions

- Title ≠ Release
- Release ≠ File

- Files must never determine release semantics  
- Releases must never be inferred solely from hashes  

All domain queries must operate through:

Title → Release → File

---

## Example

### Input files:

MyVN_v1.zip  
MyVN_v2_patch.zip  
MyVN_aprilfools.zip  

---

### Canonical interpretation:

Title: MyVN

Release:
  version: v1
  release_type: full

Release:
  version: v2
  release_type: patch

Release:
  version: april_fools
  release_type: april_fools

Each Release links to one or more Files via SHA-256 identity.

---

## Success Criteria

The architecture is correct when the system can answer:

- Which release does this file belong to?
- What versions exist for this Title?
- Which releases are patches vs full releases?
- Which releases are available on platform X?
- Where has this exact file appeared?

If a query requires bypassing Title/Release and relying only on hashes, the model is incomplete.

---

## Final Statement

This model enforces:

Title = identity  
Release = release + distribution unit  
File = physical data  

All system behavior must conform to this hierarchy.
