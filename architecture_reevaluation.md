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

The highest-impact architecture concerns in the current state are:

1. **Domain documentation and code semantics diverge** on whether Version is identity-neutral vs used as identity keying for release upsert.
2. **Application layer remains tightly coupled** (UI + orchestration + persistence concerns in `vn_archiver.py`), which slows future extension.
3. **Repository update behavior is asymmetric** (release identity fields are stable, but many non-identity release attributes are effectively create-time only), which can leave stale descriptive metadata unless explicit update flows are added.

---

## Current Architecture Assessment

### 1) Domain layer quality

`VisualNovelDomainService` is a clear orchestration boundary and correctly routes ingest through Title/Release resolution before file linkage.

Strengths:
- Normalization prior to resolution (`creator -> developer`, normalized version/language).
- Duplicate SHA guard inside an ingest payload.
- Domain object return (`IngestionResult`) gives callers a stable aggregate-level result.

Current status:
- `create_metadata_raw(...)` is release-scoped and accepts `file_id = NULL`, so metadata-only ingest events are preserved in `revision`.

### 2) Persistence model quality

The canonical schema is cohesive and strongly normalized for identity dictionaries (`developer`, `publisher`, `tag`, `language`) with join tables.

Strengths:
- Proper separation of semantic entities (`title`, `release`) and immutable binary identity (`file`).
- Foreign keys and indexes are generally strong.
- Revision chain model has parent pointers and `(release_id, version_number)` uniqueness.

Current status:
- `ux_release_identity` is robust because identity tuple columns are `NOT NULL DEFAULT ''` in schema and normalized to empty-string sentinels in repository create paths.

Risk:
- Existing release rows are reused by identity lookup, but most non-identity release metadata is not updated on match; this can cause drift between latest ingest payload and persisted release descriptors.

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

Current behavior supports release-level revisions (`file_id = NULL`) and therefore preserves metadata-only ingest history.

Recommendation:
- Add tests asserting expected `is_current` transitions and parent linkage semantics when metadata-only updates are mixed with file-attaching updates.

### C) Vocabulary and normalization

The codebase has good normalization entry points and dictionary tables. Keep this centralized in the repository/service boundary and avoid spreading normalization into UI paths.

---

## Recommended Next Iteration (prioritized)

1. **Refactor module boundaries**
   - Split `vn_archiver.py` into:
     - `application/services` (use-case orchestration)
     - `adapters/sqlite` (repository implementations)
     - `interfaces/cli_tui` (prompting, menus)

2. **Align documentation wording with actual persistence behavior**
   - Clarify “semantic identity” vs “database uniqueness key”.

3. **Introduce explicit release metadata update policy**
   - Either add update-on-match behavior for selected release fields (e.g., translator/notes/change_note) or codify immutability for these fields and store changes only in revision history.

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
