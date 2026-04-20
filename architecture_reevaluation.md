# Architecture & Domain Logic Re-evaluation (2026-04-20)

## Scope
This review re-evaluates the **implemented** architecture and domain logic across:
- `domain_layer.py`
- `ingestion_repository.py`
- `db_schema.sql`
- `vn_archiver.py`
- existing domain tests

It focuses on fidelity between the documented model and runtime behavior.

---

## Executive Summary

The project has a solid **Title → Release → File** core and a practical ingestion orchestration path. The implementation already enforces useful invariants (title required, release-title consistency, non-zero file_count) and has broad unit test coverage for ingestion behavior.

However, there are a few high-impact domain/architecture mismatches that should be addressed:

1. **Metadata history currently depends on file linkage** during ingest, which can drop revision history for metadata-only updates.
2. **Release identity uniqueness relies on nullable fields**, which weakens uniqueness guarantees in SQLite.
3. **Domain documentation and code semantics diverge** on whether Version is identity-neutral vs used as identity keying for release upsert.
4. **Application layer remains tightly coupled** (UI + orchestration + persistence concerns in `vn_archiver.py`), which slows future extension.

---

## Current Architecture Assessment

### 1) Domain layer quality

`VisualNovelDomainService` is a clear orchestration boundary and correctly routes ingest through Title/Release resolution before file linkage.

Strengths:
- Normalization prior to resolution (`creator -> developer`, normalized version/language).
- Duplicate SHA guard inside an ingest payload.
- Domain object return (`IngestionResult`) gives callers a stable aggregate-level result.

Risk:
- `create_metadata_raw(...)` only executes when a primary `file_id` exists, so metadata-only ingest events can skip revision creation.

### 2) Persistence model quality

The canonical schema is cohesive and strongly normalized for identity dictionaries (`developer`, `publisher`, `tag`, `language`) with join tables.

Strengths:
- Proper separation of semantic entities (`title`, `release`) and immutable binary identity (`file`).
- Foreign keys and indexes are generally strong.
- Revision chain model has parent pointers and `(release_id, version_number)` uniqueness.

Risk:
- `ux_release_identity` uses nullable columns (`language`, `edition`, `distribution_platform`) in the unique tuple. In SQLite, `NULL` values can permit logically duplicate rows when any nullable tuple element is `NULL`.

### 3) Application layering

`vn_archiver.py` contains CLI/TUI prompting, template handling, normalization, ingestion orchestration, and upload/rebuild workflow logic in one module.

Strengths:
- High feature velocity and straightforward script-based operation.

Risk:
- Tight coupling increases regression surface and makes independent evolution of UI, domain service, and infrastructure adapters harder.

---

## Domain Logic Consistency Review

### A) Stated vs implemented identity semantics

- Domain docs emphasize Release as primary aggregate and Version as descriptor.
- Implementation still uses normalized version heavily in release matching/upsert and DB uniqueness.

This is not inherently wrong, but the language should be made explicit:
- **Version may be non-root semantically, yet still part of persistence identity keying**.

### B) Metadata revision semantics

Current behavior implicitly treats revisions as attachment-linked events, not purely release-level events.

Recommendation:
- Allow release-level revisions with `file_id = NULL` during metadata-only updates so metadata history is complete even without new files.

### C) Vocabulary and normalization

The codebase has good normalization entry points and dictionary tables. Keep this centralized in the repository/service boundary and avoid spreading normalization into UI paths.

---

## Recommended Next Iteration (prioritized)

1. **Fix release uniqueness for nullable columns**
   - Either coalesce nullable fields in a generated-column/index strategy, or enforce NOT NULL + sentinel values at ingest normalization.

2. **Decouple metadata revision creation from file attachment availability**
   - Persist revisions whenever release metadata changes, regardless of file operations.

3. **Refactor module boundaries**
   - Split `vn_archiver.py` into:
     - `application/services` (use-case orchestration)
     - `adapters/sqlite` (repository implementations)
     - `interfaces/cli_tui` (prompting, menus)

4. **Align documentation wording with actual persistence behavior**
   - Clarify “semantic identity” vs “database uniqueness key”.

---

## Target Architecture (recommended)

- **Domain**: immutable dataclasses + service invariants only.
- **Application**: ingest/update/upload use-cases with explicit transactions.
- **Infrastructure**: repository + schema adapters.
- **Interface**: CLI/TUI purely for I/O.

This preserves current behavior while making the project easier to test and evolve.

---

## Validation Snapshot

At review time, test suite status:
- `28 passed, 2 skipped` via `pytest -q`.

So this re-evaluation is about architecture/semantics hardening, not break/fix.
