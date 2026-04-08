# Domain Logic Architecture Evaluation

## Scope reviewed
- `vn_archiver.py` domain orchestration and core ingest pipeline.
- `domain_layer.py` service abstraction and repository protocol.
- `ingestion_repository.py` persistence adapter for ingestion operations.
- `metadata_validation.py` contract validation rules.
- `tests/test_domain_layer.py` and related unit tests covering branch behavior.

## Current architecture snapshot

### 1) Layering direction has improved, but boundaries are still transitional
The project now has a recognizable domain-service layer (`VisualNovelDomainService`) and repository abstraction (`IngestionRepository` protocol). This is a meaningful step away from a pure script-style architecture because orchestration decisions (artifact vs non-artifact branch, archive collection, persistence handoff) are centralized in one service.

However, this is still a transitional architecture because core domain rules remain in `vn_archiver.py` functions that are injected into the domain service. In practical terms, the service is orchestrating function pointers rather than rich domain objects/value types.

**Assessment:** good direction, medium maturity.

### 2) Domain logic is concentrated around ingest branching and artifact linkage
The most explicit domain rules are:
- metadata shape/type determination (`is_artifact_metadata`),
- required linking semantics for derived artifacts (`_resolve_base_artifact_id`),
- matching an existing build for artifact sidecars (`resolve_existing_build_for_artifact`),
- versioned metadata object persistence (`finalize_metadata_objects` / artifact variant).

These rules are business-significant and currently encode much of the project’s domain intent.

**Assessment:** strong rule coverage for core ingestion path.

### 3) Validation is present but not deeply domain-specific
`validate_metadata_contract` enforces required fields, unknown-field rejection, and date formatting. This is excellent as contract-level validation, but it does not fully capture cross-field domain invariants (for example, more semantic constraints based on artifact type or build context).

**Assessment:** robust schema-contract guardrails; moderate domain-invariant depth.

### 4) Domain logic and infrastructure are still tightly coupled
Most domain operations directly manipulate SQLite rows and commit behavior in the same flow (`process_archives_for_build` + repository operations). That coupling makes testing and evolution harder for non-SQL concerns (e.g., policy-only rule changes).

**Assessment:** coupling is the main architectural limitation.

### 5) Test coverage targets key branch correctness but remains narrow
`tests/test_domain_layer.py` verifies title requirement plus artifact/non-artifact branch routing. Additional tests in artifact linkage and metadata validation are valuable. Yet, there are fewer tests around end-to-end domain invariants at service boundaries (especially edge combinations for version/build matching and metadata progression).

**Assessment:** foundational tests exist; expansion needed for confidence at scale.

## Architectural strengths
- **Clearer orchestration seam:** `VisualNovelDomainService` provides one entrypoint for ingest flow.
- **Repository abstraction introduced:** `VnIngestionRepository` isolates SQL operation groupings.
- **Practical business rules implemented:** artifact base resolution and build disambiguation are explicit.
- **Data-contract strictness:** unknown metadata fields and malformed dates are rejected early.

## Architectural risks / pain points
- **God-module pressure in `vn_archiver.py`:** domain, I/O, upload, formatting, and persistence utilities coexist in a single large module.
- **Function-injection over typed domain model:** service dependencies are flexible but loosely typed beyond protocol methods.
- **Potential drift between validation and domain invariants:** contract validation and semantic validation are split and may diverge.
- **Transaction and side-effect mixing:** domain decisions and commit strategy are interleaved, reducing composability.

## Recommendations (prioritized)

### Priority 1: carve out a dedicated domain package
Create a `domain/` package with:
- `entities.py` (VN, Build, Artifact metadata value objects),
- `services.py` (ingestion policy + matching rules),
- `policies.py` (artifact linkage, build resolution rules).

Keep SQLite-specific code in repository/adapters modules.

### Priority 2: separate policy validation from template validation
Keep `metadata_validation.py` for contract checks, but add semantic validators for domain rules such as:
- derived artifact must resolve unique base artifact,
- artifact sidecar build identity disambiguation policy,
- immutability/update policies for selected work-level fields.

### Priority 3: formalize use-case orchestration
Introduce explicit use-case functions/classes:
- `IngestVisualNovelUseCase`,
- `IngestArtifactUseCase`.

This avoids conditional branching in a single ingest method and clarifies extension points.

### Priority 4: improve transactional boundary clarity
Move transaction begin/commit/rollback ownership into one adapter/unit-of-work style component so domain service remains persistence-agnostic.

### Priority 5: broaden tests around domain invariants
Add matrix tests for:
- build matching ambiguity resolution,
- artifact base-link fallback behavior,
- metadata version pointer behavior when hashes repeat/change,
- regression tests for invariants spanning multiple tables.

## Overall verdict
The project’s domain logic architecture is **improving and directionally solid**, with a credible domain service and repository seam already in place. The primary next step is to finish the separation by extracting policy-rich domain logic from `vn_archiver.py` into dedicated domain modules and strengthening semantic invariant tests.
