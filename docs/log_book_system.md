# Log-book-first metadata system (event-sourced)

A practical way to achieve "record in order" + "easy cascade deletion" is to make one append-only **log book** table the source of truth, and make all other tables projections that carry a `log_entry_id` foreign key.

## 1) Source-of-truth table

```sql
CREATE TABLE metadata_log_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq_no INTEGER NOT NULL UNIQUE,            -- strict insertion order
    event_type TEXT NOT NULL,                  -- create_vn, create_build, set_tags, archive_added, etc.
    aggregate_kind TEXT NOT NULL,              -- vn, build, archive
    aggregate_key TEXT NOT NULL,               -- stable business key (slug/version pair)

    metadata_hash TEXT,                        -- optional link to metadata_objects(hash)
    payload_json TEXT NOT NULL,                -- full canonical metadata/event payload

    parent_log_entry_id INTEGER,               -- optional for chained edits
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (metadata_hash)
        REFERENCES metadata_objects(hash)
        ON DELETE RESTRICT,

    FOREIGN KEY (parent_log_entry_id)
        REFERENCES metadata_log_book(id)
        ON DELETE SET NULL
);
```

### Important clarification
The log-book table does **not** need every metadata field as a dedicated SQL column.
- Keep a small set of indexed columns for routing/querying (`seq_no`, `event_type`, `aggregate_*`).
- Keep full metadata in `payload_json`.
- Derive normalized table columns by parsing `payload_json` during projection/replay.

## 2) How rows are derived from `payload_json`

Think of this as a deterministic projector:
1. Read log entries ordered by `seq_no`.
2. For each `event_type`, extract fields from `payload_json`.
3. Upsert/insert into normalized tables.
4. Write `log_entry_id = metadata_log_book.id` on derived rows.

### Example payload

```json
{
  "vn": {
    "title": "My VN",
    "canonical_slug": "my-vn",
    "developer": "Studio A",
    "tags": ["romance", "drama"]
  },
  "build": {
    "version": "1.2",
    "language": "en",
    "edition": "standard",
    "target_platforms": ["Windows", "Linux"]
  },
  "archive": {
    "sha256": "...",
    "file_size_bytes": 1234567
  }
}
```

### Example extraction (SQLite JSON1)

```sql
-- Project VN identity/details
INSERT INTO visual_novels (title, canonical_slug, developer, source)
VALUES (
  json_extract(:payload, '$.vn.title'),
  json_extract(:payload, '$.vn.canonical_slug'),
  json_extract(:payload, '$.vn.developer'),
  'metadata_log_book'
)
ON CONFLICT(canonical_slug) DO UPDATE SET
  title = excluded.title,
  developer = excluded.developer;

-- Project build row
INSERT INTO builds (vn_id, version, language, edition, source)
VALUES (
  :vn_id,
  json_extract(:payload, '$.build.version'),
  json_extract(:payload, '$.build.language'),
  json_extract(:payload, '$.build.edition'),
  'metadata_log_book'
)
ON CONFLICT(vn_id, version, COALESCE(language, ''), COALESCE(edition, ''))
DO UPDATE SET source = excluded.source;

-- Project tags from JSON array
INSERT OR IGNORE INTO tags(name)
SELECT value
FROM json_each(:payload, '$.vn.tags');
```

Use projector code (recommended) for multi-step logic like FK lookups, current-version selection, and conflict resolution. SQL snippets above are just the shape of extraction.

## 3) Derived tables point back to the log book

For any table that is wholly derived, add a `log_entry_id` column and cascade from that:

```sql
ALTER TABLE metadata_versions
ADD COLUMN log_entry_id INTEGER
REFERENCES metadata_log_book(id)
ON DELETE CASCADE;
```

Use the same pattern for other fully-derived rows (e.g., normalized tag mappings, platform mappings, snapshots):
- `vn_tags.log_entry_id`
- `build_target_platforms.log_entry_id`
- `archives.log_entry_id` (if archive row is considered projection of event)

If a table row can be produced by *multiple* log entries, use a join table:

```sql
CREATE TABLE build_projection_sources (
    build_id INTEGER NOT NULL,
    log_entry_id INTEGER NOT NULL,
    PRIMARY KEY (build_id, log_entry_id),
    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE,
    FOREIGN KEY (log_entry_id) REFERENCES metadata_log_book(id) ON DELETE CASCADE
);
```

## 4) Cascade behavior you want

Deleting a log entry should automatically remove dependent derived rows:

- Delete from `metadata_log_book` where `id = ?`
- DB automatically deletes all rows with `log_entry_id = ?` via `ON DELETE CASCADE`
- Existing FK cascades then continue pruning deeper dependencies

This gives "single-point delete" semantics for all wholly-derived data.

## 5) Recommended operational model (hybrid-first for this repo)

Given the current schema is normalized and column-driven, use a staged approach:
- Keep current normalized writes initially.
- Also append a canonical event row to `metadata_log_book`.
- Add/verify projector replay so tables can be rebuilt from log if needed.
- Gradually shift writes to "log first, project second" when stable.

Add indexes:
- `metadata_log_book(seq_no)` unique
- `metadata_log_book(aggregate_kind, aggregate_key, seq_no)`
- `<projection_table>(log_entry_id)`

## 6) Is this resilient to metadata field additions/removals/changes?

Yes, **if event/versioning rules are explicit**.

Why it is resilient:
- New metadata fields can be added to `payload_json` without changing the `metadata_log_book` table schema.
- Removed fields can simply stop being emitted by new events; old historical events still replay with old keys.
- Field renames/type changes can be handled by projector version logic keyed by `event_type` + version markers.

Recommended safeguards:
- Add `payload_schema_version` (or include `schema_version` in payload) and branch projector logic by version.
- Keep projector logic backward compatible for at least N previous payload versions.
- Prefer additive changes first; deprecate old keys before hard removal.
- Use defaulting/coalescing during extraction (e.g., `COALESCE(json_extract(...), <default>)`).
- Validate payload on write (JSON schema or explicit validator in app code).

Example (rename `vn.developer` -> `vn.studio`):

```sql
COALESCE(
  json_extract(:payload, '$.vn.studio'),
  json_extract(:payload, '$.vn.developer')
)
```

This keeps replay deterministic across old and new payload shapes.

## 7) Minimal migration plan for this repo

1. Create `metadata_log_book`.
2. Start writing one event row per metadata mutation.
3. Add `log_entry_id` to fully-derived tables.
4. Backfill `log_entry_id` for existing rows where possible.
5. Enforce `NOT NULL` on `log_entry_id` only after backfill.
6. Add and validate a replay script to rebuild derived tables from the log.
7. Introduce payload-version checks and compatibility tests for projector evolution.

This gives you an explicit log-book spine while preserving your current normalized schema and FK cascade strategy.
