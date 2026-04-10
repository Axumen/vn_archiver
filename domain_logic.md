# VN Archiver — Domain Logic (Simplified Canonical Architecture)

## Purpose

This document defines the **target domain model** for VN Archiver.

The architecture is **build-centric** and models releases as structured domain entities rather than file-driven blobs.

The system must represent:
- VN identity
- Versioned releases
- Physical files

---

## Core Principle

Do **not** design around:

file → hash → stored

Design around:

VN → Build → File

Where:

- **VN** is the product identity boundary  
- **Build** is the release and distribution unit  
- **File** is the physical binary identity  

---

## Domain Entities (Required)

---

### 1) VN

Represents the **title-level identity** of a visual novel.

This is the highest-level grouping and remains stable across all versions.

#### Required fields:

- vn_id
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

### 2) Build

Represents a **specific release/version** of a VN.

A Build is the **primary unit of querying and classification**.

A Build encapsulates:
- version identity
- release lifecycle
- distribution metadata
- technical/runtime characteristics

#### Required fields:

- build_id
- vn_id
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
- first_seen_at

#### Optional fields:

- filename
- mime_type

---

## Required Cardinality Rules

- One VN has many Builds  
- One Build has many Files  
- One File can belong to many Builds  

- BuildRelation is a many-to-many self-reference over Build

---

## BuildRelation (Required)

Represents relationships between builds.

Used for modeling:
- updates
- dependencies
- alternate releases

#### Fields:

- from_build_id
- to_build_id
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

Describes the stability or lifecycle of the build.

Examples:

- stable
- beta
- alpha
- prerelease
- discontinued
- unknown

---

### access_model

Describes how the build is obtained.

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

### 3. Resolve VN

- Match or create VN using:
  - title
  - aliases (if needed)

---

### 4. Resolve Build

- Match or create Build using:
  - version
  - normalized_version
  - release_type
  - language (if relevant)

---

### 5. Link Files to Build

- Attach all ingested files to the resolved Build

---

### 6. Apply Metadata Mapping

- VN-level fields → VN
- Build-level fields → Build
- File-level fields → File

---

### 7. Mark Completion

- Build is considered classified once:
  - VN is resolved
  - Build is resolved
  - Files are linked

---

## Non-Negotiable Distinctions

- VN ≠ Build
- Build ≠ File

- Files must never determine release semantics  
- Builds must never be inferred solely from hashes  

All domain queries must operate through:

VN → Build → File

---

## Example

### Input files:

MyVN_v1.zip  
MyVN_v2_patch.zip  
MyVN_aprilfools.zip  

---

### Canonical interpretation:

VN: MyVN

Build:
  version: v1
  release_type: full

Build:
  version: v2
  release_type: patch

Build:
  version: april_fools
  release_type: april_fools

Each Build links to one or more Files via SHA-256 identity.

---

## Success Criteria

The architecture is correct when the system can answer:

- Which build does this file belong to?
- What versions exist for this VN?
- Which builds are patches vs full releases?
- Which builds are available on platform X?
- Where has this exact file appeared?

If a query requires bypassing VN/Build and relying only on hashes, the model is incomplete.

---

## Final Statement

This model enforces:

VN = identity  
Build = release + distribution unit  
File = physical data  

All system behavior must conform to this hierarchy.
