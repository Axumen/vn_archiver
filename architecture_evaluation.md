# Domain Logic & Architecture Evaluation

## Scope reviewed

- Domain orchestration and entities in `domain_layer.py`
- Persistence adapter behavior in `ingestion_repository.py`
- Canonical relational constraints in `db_schema.sql`
- Existing architecture intent in `domain_logic.md`
- Test coverage in `tests/test_domain_layer.py` and related repository/schema tests

## Executive assessment

The current architecture is **directionally strong** and demonstrates a clear transition to a canonical domain model centered on **Title → Release → File**. The implementation enforces practical ingestion invariants (required title, release/file linkage, metadata revisioning) and includes robust schema-presence checks in the repository adapter.

Primary strengths are:

1. Clear aggregate boundaries and naming in the domain layer.
2. Defensive schema validation at startup in the repository adapter.
3. Relational deduplication and history support via `file`, `release_file`, and `revision`.
4. Solid baseline unit/integration tests around key ingest paths.

The main architectural risks are:

1. **Mixed responsibilities** in the repository (identity resolution, normalization, relationship syncing, and revision persistence bundled together).
2. **Inconsistent normalization semantics** between domain-layer language normalization and repository list handling.
3. **Partial aggregate leakage** where `Title` domain object has fewer fields than the persistent `title` concept.
4. **Schema/application drift risk** (e.g., `release_status` appears in domain object but maps to `build_type` in canonical release table).

Overall maturity: **good foundation with medium-term refactoring opportunity**.

---

## Findings

## 1) Domain model coherence

### What works well

- `VisualNovelDomainService` cleanly orchestrates ingest lifecycle and returns a domain-level `IngestionResult` with identity consistency checks. This provides a stable application boundary independent of CLI/TUI call sites.
- Domain dataclasses are immutable (`frozen=True`), reducing accidental mutation bugs in orchestration flow.
- Explicit invariant in `Release.__post_init__` (`file_count >= 1`) is reasonable and paired with a defensive `max(1, len(archives_to_process))` constructor usage.

### Gaps / concerns

- `Title` dataclass currently models only `canonical_title`, `developer`, `publisher`, while the `title` table includes much richer work-level semantics (series, content mode/type/rating, source, etc.). This mismatch can hide drift where domain objects no longer represent true business meaning.
- `Release` uses `release_type` and `release_status` fields, while canonical schema uses `build_type` and does not include `release_status` in `release`; this naming mismatch raises cognitive and maintenance overhead.

### Recommendation

Define explicit domain value objects for canonical concepts already represented in DB (e.g., `BuildType`, `DistributionPlatform`, maybe optional), and align dataclass fields with schema terminology (`build_type`) to reduce semantic translation costs.

---

## 2) Ingestion orchestration and invariants

### What works well

- Pre-resolution normalization in `_prepare_resolution_metadata()` is a good anti-corruption step: creator→developer aliasing, version normalization, and language normalization.
- Duplicate SHA detection across incoming archive payloads is handled before persistence, preventing duplicate file-link attempts in one ingest call.
- Raw metadata revision persistence strips internal keys (`_raw_text`, `_source_file`) before hashing, which improves deterministic metadata provenance.

### Gaps / concerns

- Domain service normalizes language into uppercase for short alphabetic codes, while repository language sync lowercases via tag-normalizer; this introduces representational inconsistency between release column and dictionary tables.
- `candidate_sha256` is computed but not used in persistence decisions besides fallback assignment; this suggests dead or unfinished logic path.

### Recommendation

Centralize normalization policy in one dedicated module (or value object helpers) consumed by both domain service and repository. Remove or complete `candidate_sha256` flow to avoid misleading code.

---

## 3) Repository architecture and data access patterns

### What works well

- `_resolve_schema()` provides strict fail-fast validation for canonical tables and required columns. This is excellent for operational safety and migration clarity.
- `get_or_create_*` and link-sync routines enforce upsert-like behavior and keep many-to-many dictionaries in sync.
- File-level deduplication by SHA and release_file link idempotency are correctly implemented.

### Gaps / concerns

- `VnIngestionRepository` has become a *god adapter* combining:
  - schema introspection,
  - metadata normalization,
  - title/release identity logic,
  - dictionary synchronization,
  - file persistence,
  - revision persistence.

  This high coupling increases test surface and makes future migrations riskier.

- `_sync_title_people_tables()` uses `_normalize_tag_list()` which lowercases names; for human/company proper names this may be undesirable if display casing matters long-term.
- `create_metadata_raw()` uses sorted JSON and hashes that representation; good for determinism, but field ordering expectations described in README may diverge if non-canonical ordering is required for human readability snapshots.

### Recommendation

Split repository into focused collaborators:

1. `SchemaGuard` (startup checks)
2. `TitleReleaseStore` (identity resolution/upsert)
3. `FileStore` (file/release_file/link rules)
4. `RevisionStore` (metadata versioning)

Keep `VnIngestionRepository` as a façade coordinating these components for backward compatibility.

---

## 4) Database schema alignment

### What works well

- Canonical schema enforces referential integrity with useful cascades and unique release identity index.
- Virtual `normalized_version` and unique identity index provide practical dedupe for release semantics.
- Separation between `revision` (release metadata timeline) and `file_snapshot` (attachment-time flattened state) is architecturally sound.

### Gaps / concerns

- `release.language` text plus `release_language` join table can drift unless synchronized consistently; current code syncs both, but this dual representation needs explicit canonical-source rule.
- `release_file.archived_at` and `revision.created_at` are app-generated text timestamps; no DB-level format check, which may allow inconsistent values over time.

### Recommendation

Document one source of truth for language (`release.language` vs. `release_language`) and optionally add integrity triggers or generated helpers if dual representation is kept.

---

## 5) Testability and coverage

### What works well

- Tests cover core domain ingest paths, normalization behavior, duplicate SHA rejection, and schema ingestion integration.
- Existing suite validates that non-file ingests can proceed while skipping file-linked metadata persistence.

### Gaps / opportunities

- Missing explicit tests for casing and normalization consistency between domain layer and repository relationship tables.
- Missing tests for edge cases around release identity uniqueness when fields are null/empty variants.

### Recommendation

Add focused tests for normalization contracts shared across layers (version/language/developer casing) and for release identity conflict behavior.

---

## Suggested roadmap (ordered)

1. **Terminology alignment pass**
   - Replace domain `release_type` usage with canonical `build_type` naming.
2. **Normalization policy extraction**
   - Shared helper(s) for version/language/person-name normalization consumed by domain + repository.
3. **Repository decomposition**
   - Break `VnIngestionRepository` into focused stores while preserving public methods.
4. **Dual-language representation decision**
   - Pick canonical source and enforce with tests/triggers/docs.
5. **Test expansion**
   - Add contract tests for normalization and release identity edge cases.

## Final verdict

The current implementation is **architecturally viable and production-leaning for a single-app codebase**, with clear domain intent and strong canonical schema guardrails. The next quality inflection point is reducing coupling in persistence logic and tightening normalization consistency across layers.
