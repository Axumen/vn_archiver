"""Tests for staging.py — naming conventions, file staging, and metadata sidecar creation."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("yaml")
import yaml

import staging


# ============================================================
# build_recommended_archive_name
# ============================================================


class TestBuildRecommendedArchiveName:
    def test_basic_name(self):
        meta = {"title": "My VN", "version": "1.0"}
        name = staging.build_recommended_archive_name(meta, "abcdef1234567890")
        assert name == "my-vn_1-0_abcdef12.zip"

    def test_missing_title_uses_unknown(self):
        meta = {"version": "2.0"}
        name = staging.build_recommended_archive_name(meta, "deadbeef")
        assert name.startswith("unknown_")

    def test_missing_version_uses_unknown(self):
        meta = {"title": "Example"}
        name = staging.build_recommended_archive_name(meta, "deadbeef")
        assert "_unknown_" in name

    def test_missing_sha_uses_nohash(self):
        meta = {"title": "Example", "version": "1.0"}
        name = staging.build_recommended_archive_name(meta, None)
        assert "_nohash" in name

    def test_custom_extension(self):
        meta = {"title": "Example", "version": "1.0"}
        name = staging.build_recommended_archive_name(meta, "abc123", ext=".7z")
        assert name.endswith(".7z")

    def test_extension_without_dot_gets_dot(self):
        meta = {"title": "Example", "version": "1.0"}
        name = staging.build_recommended_archive_name(meta, "abc123", ext="rar")
        assert name.endswith(".rar")

    def test_sha256_truncated_to_8_chars(self):
        meta = {"title": "Test", "version": "1.0"}
        full_sha = "a" * 64
        name = staging.build_recommended_archive_name(meta, full_sha)
        # hash part should be exactly 8 chars
        assert "aaaaaaaa" in name
        assert "aaaaaaaaa" not in name


# ============================================================
# build_recommended_metadata_name
# ============================================================


class TestBuildRecommendedMetadataName:
    def test_release_metadata_with_release_id(self):
        meta = {"title": "My VN", "version": "2.0"}
        name = staging.build_recommended_metadata_name(meta, "abcdef12", 1, release_id=42)
        assert name == "my-vn_2-0_00042_r01.yaml"

    def test_release_metadata_without_release_id_uses_hash(self):
        meta = {"title": "My VN", "version": "2.0"}
        name = staging.build_recommended_metadata_name(meta, "abcdef12", 1, release_id=None)
        assert "abcdef12" in name
        assert name.endswith("_r01.yaml")

    def test_file_metadata_uses_artifact_slug_and_hash(self):
        meta = {"title": "My VN", "artifact_type": "patch"}
        name = staging.build_recommended_metadata_name(meta, "deadbeef", 3)
        assert name == "my-vn_patch_deadbeef_r03.yaml"

    def test_revision_number_zero_padded(self):
        meta = {"title": "Test", "version": "1.0"}
        name = staging.build_recommended_metadata_name(meta, "abc", 5, release_id=1)
        assert "_r05.yaml" in name

    def test_none_revision_defaults_to_r01(self):
        meta = {"title": "Test", "version": "1.0"}
        name = staging.build_recommended_metadata_name(meta, "abc", None, release_id=1)
        assert "_r01.yaml" in name

    def test_release_id_zero_padded_to_5_digits(self):
        meta = {"title": "Test", "version": "1.0"}
        name = staging.build_recommended_metadata_name(meta, "abc", 1, release_id=7)
        assert "_00007_" in name


# ============================================================
# stage_metadata_yaml_for_upload
# ============================================================


class TestStageMetadataYamlForUpload:
    def test_creates_yaml_with_correct_name(self, tmp_path):
        meta = {"title": "Example VN", "version": "1.0", "developer": "Studio A"}
        result = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256="abcdef12", release_id=10, target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        assert result.exists()
        assert result.parent == tmp_path
        assert result.suffix == ".yaml"
        assert "example-vn" in result.name
        assert "_00010_" in result.name

    def test_strips_internal_keys_from_yaml(self, tmp_path):
        meta = {
            "title": "Test",
            "version": "1.0",
            "_raw_text": "should not appear",
            "_source_file": "incoming/test.yaml",
        }
        result = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256="abc", target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        content = yaml.safe_load(result.read_text(encoding="utf-8"))
        assert "_raw_text" not in content
        assert "_source_file" not in content
        assert content["title"] == "Test"

    def test_fallback_sha_from_metadata(self, tmp_path):
        meta = {"title": "Test", "version": "1.0", "sha256": "frommetadata"}
        result = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256=None, target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        assert "frommetad" in result.name or "frommeta" in result.name

    def test_fallback_sha_from_archives_block(self, tmp_path):
        meta = {
            "title": "Test",
            "version": "1.0",
            "archives": [{"sha256": "archivesha256hash", "filename": "vn.zip"}],
        }
        result = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256=None, target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        assert "archives" in result.name or "archivesha256hash"[:8] in result.name

    def test_overwrites_existing_sidecar(self, tmp_path):
        meta = {"title": "Test", "version": "1.0"}
        path1 = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256="abc", release_id=1, target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        path1.write_text("old content", encoding="utf-8")
        path2 = staging.stage_metadata_yaml_for_upload(
            meta, 1, sha256="abc", release_id=1, target_dir=tmp_path,
            order_fn=lambda m: m,
        )
        assert path1 == path2
        content = yaml.safe_load(path2.read_text(encoding="utf-8"))
        assert content["title"] == "Test"


# ============================================================
# stage_ingested_files_for_upload
# ============================================================


class TestStageIngestedFilesForUpload:
    def test_moves_archive_to_uploading(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        source = tmp_path / "incoming" / "game.zip"
        source.parent.mkdir()
        source.write_bytes(b"zipdata")

        archives_data = [
            {"original_path": str(source), "filename": "game.zip", "sha256": "abc12345"},
        ]
        staged, meta_path = staging.stage_ingested_files_for_upload(
            {"title": "Test VN", "version": "1.0"},
            archives_data,
            metadata_version_number=None,
        )
        assert len(staged) == 1
        assert staged[0].exists()
        assert not source.exists()
        assert meta_path is None  # no metadata_version_number → no sidecar

    def test_stages_metadata_when_version_number_provided(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        source = tmp_path / "incoming" / "game.zip"
        source.parent.mkdir()
        source.write_bytes(b"zipdata")

        archives_data = [
            {"original_path": str(source), "filename": "game.zip", "sha256": "abc12345"},
        ]
        staged, meta_path = staging.stage_ingested_files_for_upload(
            {"title": "Test VN", "version": "1.0"},
            archives_data,
            metadata_version_number=1,
            release_id=5,
            order_fn=lambda m: m,
        )
        assert meta_path is not None
        assert meta_path.exists()
        assert meta_path.suffix == ".yaml"

    def test_skips_missing_source_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        archives_data = [
            {"original_path": str(tmp_path / "nonexistent.zip"), "filename": "nope.zip", "sha256": "abc"},
        ]
        staged, _ = staging.stage_ingested_files_for_upload(
            {"title": "Test", "version": "1.0"},
            archives_data,
        )
        assert len(staged) == 0

    def test_skips_entry_without_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        archives_data = [{"sha256": "abc"}]
        staged, _ = staging.stage_ingested_files_for_upload(
            {"title": "Test", "version": "1.0"},
            archives_data,
        )
        assert len(staged) == 0

    def test_populates_staged_upload_path_on_archive_data(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "UPLOADING_DIR", str(tmp_path / "uploading"))

        source = tmp_path / "incoming" / "game.zip"
        source.parent.mkdir()
        source.write_bytes(b"data")

        archives_data = [
            {"original_path": str(source), "filename": "game.zip", "sha256": "abc12345"},
        ]
        staging.stage_ingested_files_for_upload(
            {"title": "Test", "version": "1.0"},
            archives_data,
        )
        assert "staged_upload_path" in archives_data[0]


# ============================================================
# get_vn_archive_version_dir
# ============================================================


class TestGetVnArchiveVersionDir:
    def test_creates_version_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "VN_ARCHIVE_DIR", str(tmp_path / "vn archive"))

        result = staging.get_vn_archive_version_dir({"title": "My Game", "version": "1.0"})
        assert result.exists()
        assert result.is_dir()
        assert "1.0" in str(result)
        assert "My Game" in str(result)

    def test_missing_title_uses_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "VN_ARCHIVE_DIR", str(tmp_path / "vn archive"))

        result = staging.get_vn_archive_version_dir({"version": "1.0"})
        assert result.exists()
        assert "Unknown Title" in str(result)

    def test_missing_version_uses_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "VN_ARCHIVE_DIR", str(tmp_path / "vn archive"))

        result = staging.get_vn_archive_version_dir({"title": "Example"})
        assert result.exists()
        assert "unknown" in str(result)


# ============================================================
# mirror_metadata_for_rebuild
# ============================================================


class TestMirrorMetadataForRebuild:
    def test_mirrors_metadata_with_archive_id_prefix(self, tmp_path, monkeypatch):
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE file (file_id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE)")
        conn.execute("CREATE TABLE release_file (release_id INTEGER NOT NULL, file_id INTEGER NOT NULL)")
        conn.execute("INSERT INTO file (file_id, sha256) VALUES (13, 'deadbeef')")
        conn.execute("INSERT INTO release_file (release_id, file_id) VALUES (7, 13)")

        staged = tmp_path / "meta.yaml"
        staged.write_text("title: Test\n", encoding="utf-8")

        rebuild_dir = tmp_path / "rebuild_metadata"
        monkeypatch.setattr(staging, "REBUILD_METADATA_DIR", str(rebuild_dir))

        mirrored = staging.mirror_metadata_for_rebuild(
            str(staged), [{"sha256": "deadbeef"}], release_id=7, conn=conn,
        )
        assert len(mirrored) == 1
        assert mirrored[0].name.startswith("13_")

    def test_returns_empty_when_no_release_id(self, tmp_path, monkeypatch):
        monkeypatch.setattr(staging, "REBUILD_METADATA_DIR", str(tmp_path / "rebuild"))

        mirrored = staging.mirror_metadata_for_rebuild(
            str(tmp_path / "meta.yaml"), [{"sha256": "abc"}], release_id=None,
        )
        assert mirrored == []

    def test_returns_empty_when_no_matching_sha(self, tmp_path, monkeypatch):
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE file (file_id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE)")
        conn.execute("CREATE TABLE release_file (release_id INTEGER NOT NULL, file_id INTEGER NOT NULL)")
        conn.execute("INSERT INTO file (file_id, sha256) VALUES (1, 'aaaa')")
        conn.execute("INSERT INTO release_file (release_id, file_id) VALUES (5, 1)")

        staged = tmp_path / "meta.yaml"
        staged.write_text("title: Test\n", encoding="utf-8")

        monkeypatch.setattr(staging, "REBUILD_METADATA_DIR", str(tmp_path / "rebuild"))

        mirrored = staging.mirror_metadata_for_rebuild(
            str(staged), [{"sha256": "no-match"}], release_id=5, conn=conn,
        )
        assert len(mirrored) == 0
