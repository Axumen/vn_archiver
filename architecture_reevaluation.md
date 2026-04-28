# VN Archiver — Architecture Re-evaluation (2026-04-29)

## Scope

Full audit of the **current** codebase (11 modules, schema, tests, and documentation).
This supersedes the prior evaluation dated 2026-04-27.

| Module | Lines | Role |
|---|---:|---|
| [tui.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/tui.py) | 1 511 | Terminal UI / menus |
| [b2.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/b2.py) | 662 | Backblaze B2 upload |
| [ingestion_repository.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/ingestion_repository.py) | 704 | DB persistence (4 internal stores + façade) |
| [staging.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/staging.py) | 308 | File naming / staging / local archive layout |
| [utils.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/utils.py) | 338 | Pure utilities / normalizers |
| [domain_layer.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/domain_layer.py) | 204 | Domain service + dataclasses |
| [vn_archiver.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/vn_archiver.py) | 198 | Ingestion orchestration |
| [ingestion_service.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/ingestion_service.py) | 133 | Use-case orchestration (Pair/Attach pipelines) |
| [template_service.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/template_service.py) | 165 | Metadata template loading |
| [db_manager.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/db_manager.py) | 157 | SQLite connection / backup |
| [cloud_tracking_repository.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/cloud_tracking_repository.py) | 52 | Cloud upload tracking |
| [rebuild_archive_db_from_yaml.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/rebuild_archive_db_from_yaml.py) | 146 | DB rebuild from YAML |
| [metadata_validation.py](file:///c:/Users/Acer/Desktop/Work/gravity/vn-archiver/vn_archiver/metadata_validation.py) | 53 | Contract validation |

Test suite: **65 passed**, 0 failures (as of this review).

---

## Executive Summary

The project has achieved a high state of technical maturity. The **Title → Release → File** hierarchy is strictly enforced through a decoupled repository layer, and the application now features a professional logging infrastructure, comprehensive test coverage (65 tests), and transactional integrity. Most of the high-severity architectural gaps identified in earlier audits have been successfully closed.

Remaining work focuses on UI decomposition, disaster recovery refinements, and minor semantic alignments.

---

## Findings

### Severity Scale

| Severity | Meaning |
|---|---|
| 🔴 **Critical** | Data-loss risk or silent correctness bug |
| 🟠 **High** | Structural gap that will cause pain at modest scale |
| 🟡 **Medium** | Maintainability / extensibility issue |
| 🟢 **Low** | Cosmetic or minor improvement opportunity |

---

### Category 1 — Data Safety & Transaction Integrity

#### 1A. No transactional boundary ✅ **Resolved**
Ingestion paths in `vn_archiver.py` and `ingestion_service.py` now use `exclusive_transaction(conn)` context managers to wrap multi-step writes (title → release → file → revision).

#### 1B. Concurrent connection opens ✅ **Resolved**
Connections are now shared or passed down through the pipeline where necessary, and the use of `exclusive_transaction` ensures write serialized access.

#### 1C. `_sync_title_tags_tables` deletes + re-inserts ✅ **Resolved**
Repository now performs delta-based updates for tags, people (developers/publishers), and languages. Only changed associations are added or removed.

---

### Category 2 — Layering & Coupling

#### 2A. `VisualNovelDomainService` receives raw `conn` ✅ **Resolved**
The domain service now accepts only the `repository` and the `collect_archives` callback.

#### 2B. `collect_archives_for_db` is a callback 🟢
Still implemented as a callback injected at construction. This is acceptable for testability but could be moved to a service-level default if desired.

#### 2C. `tui.py` is a 1 511-line monolith 🟡
While the TUI now delegates business logic to `ingestion_service.py` and `staging.py`, it still handles menu routing, editor integration, and upload orchestration. Further decomposition into specialized UI components (e.g., `menu_router.py`, `metadata_prompter.py`) is recommended as the feature set grows.

#### 2D. `b2.py` contains inline SQL ✅ **Resolved**
Cloud tracking SQL has been moved to a dedicated `CloudTrackingRepository`.

---

### Category 3 — Domain Model Gaps

#### 3A. `build_type` ↔ `release_type` alias ✅ **Resolved**
The alias is now centrally handled in `utils.py` during metadata normalization. `release_type` is the canonical database key.

---

### Category 4 — Error Handling & Observability

#### 4A. Exceptions are printed and swallowed in TUI paths 🟠
In `_process_incoming_pairs` and other batch loops, `except Exception` blocks print to console and continue. This is fine for interactive use but makes automated batch processing difficult to audit for partial failures.
> [!TIP]
> Consider accumulating a list of failed pairs to show a summary at the end of the batch operation.

#### 4B. `print()` is the only logging mechanism ✅ **Resolved**
A structured `logging` infrastructure has been implemented in `logger.py`. All non-UI modules now use standard `log.info()`, `log.debug()`, etc., with support for rotating log files and coloured console output.

#### 4C. `SchemaGuard` raises `RuntimeError` 🟢
Still uses `RuntimeError` for schema mismatches. A custom `SchemaValidationError` would be a minor improvement.

---

### Category 5 — Test Coverage

#### 5A. No tests for `staging.py` / `ingestion_service.py` ✅ **Resolved**
The test suite has been expanded from 32 to 65 items. Comprehensive tests now exist for `staging.py`, `ingestion_service.py`, and the full ingestion pipeline.

#### 5B. No integration test for full pipeline ✅ **Resolved**
`test_ingestion_service.py` and `test_minimal_schema_pipeline.py` now exercise the end-to-end ingest-and-stage flow.

---

### Category 6 — Rebuild / Disaster Recovery

#### 6A. `rebuild_archive_db_from_yaml.py` return-value mismatch ✅ **Resolved**
Tuple unpacking mismatch has been fixed.

#### 6B. Rebuild path does not restore `release_file` links 🟡
`rebuild_database` relies on the `archives` block being present in the metadata YAML to restore file links. If a sidecar was generated without this block (e.g., metadata-only update), the file link is lost during rebuild.
> [!IMPORTANT]
> The current "Full Sidecar" strategy (storing the entire `archives` block in every sidecar) mitigates this, but it relies on sidecars being generated with file context.

#### 6C. Rebuild does not restore `cloud_archive` / `cloud_sidecar` tables 🟡
`rebuild_database` still calls `initialize_database(reset=True)`, which wipes the cloud tracking tables. After a rebuild, the system forgets which files were uploaded to B2.
> [!WARNING]
> This will cause the TUI to report all files as "Not Uploaded" until they are re-scanned or the tracking table is manually restored.

---

### Category 7 — Naming & Documentation Drift

#### 7A. `architecture_reevaluation.md` in codebase 🟢
The document is still present in the workspace. It should be moved to a `docs/` folder or handled as an ephemeral artifact.

#### 7B. Sidecar naming convention mismatch ✅ **Resolved**
Staging and B2 modules now use standardized naming logic (`build_recommended_metadata_name`), with accompanying tests to ensure consistency.

---

## Progress Since Prior Evaluation (2026-04-27)

| Finding | Status |
|---|---|
| **Logging Infrastructure** | ✅ **Resolved** — `logger.py` implemented and integrated. |
| **Test Coverage Gap** | ✅ **Resolved** — Expanded to 65 tests; staging and services covered. |
| **Cloud Tracking Decoupling** | ✅ **Resolved** — `CloudTrackingRepository` extracted. |
| **Transactional Integrity** | ✅ **Resolved** — `exclusive_transaction` used in all ingestion paths. |
| **Standardized Naming** | ✅ **Resolved** — Sidecar and Archive naming logic unified. |

---

## Recommended Next Priorities

| Priority | Action | Findings |
|:---:|---|---|
| **1** | **Preserve Cloud Tracking during Rebuild**: Modify `rebuild_database` to either backup/restore cloud tables or skip dropping them. | 6C |
| **2** | **TUI Decomposition**: Extract metadata prompting and menu logic into smaller modules. | 2C |
| **3** | **Batch Failure Summary**: Improve TUI batch loops to report a summary of failures rather than just printing mid-loop. | 4A |
| **4** | **Custom Exceptions**: Introduce domain-specific exceptions for schema and ingestion failures. | 4C |

---

## Validation Snapshot

Current test suite status:
- `65 passed` via `pytest -q`.
- Logging output verified in console and rotating file.
- Transactional rollback verified via manual crash injection during ingestion.
