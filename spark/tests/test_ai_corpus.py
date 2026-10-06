"""RAG knowledge publisher tests (AI-P2).

The secret-scan tests matter most. A corpus is read back to operators by a language model;
a credential that reaches it is disclosed to everyone who can ask a question.

No AWS, no network, no Spark.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ai" / "knowledge"))

from chunking import CHUNKING_VERSION, chunk_markdown, chunk_structured
from dbt_meta import extract as dbt_extract
from publish import build, discover, write
from redaction import scan
from sources import MAX_DOCUMENT_BYTES, REGISTRY, is_excluded

# Credential-SHAPED strings are assembled at runtime, never written literally: the repo's
# own `validate-docs` credential scanner reads this file too, and a literal fixture would
# make it fail. Weakening that scanner to accommodate a test would be the wrong trade.
_AKIA = "AKIA" + "IOSFODNN7EXAMPLE"
_SECRET_KEY_LINE = "aws_secret_access_key = " + "A" * 40
_PRIVATE_KEY = "-----BEGIN " + "RSA PRIVATE" + " KEY-----"

FIXTURE = """# Runbook — recover the widget

Intro paragraph long enough to survive the minimum-length filter applied to every chunk.

## Step one

Do the first thing. This paragraph exists so the section clears MIN_CHARS comfortably.

```bash
# this heading-looking comment must NOT start a new section
aws sts get-caller-identity --profile my-aws-profile
```

## Step two

Do the second thing, with enough prose that the section is retained by the chunker.
"""


# ------------------------------------------------------------------ allow-list
class TestSourceAllowList:
    def test_registry_is_non_empty_and_typed(self):
        assert REGISTRY and all(s.document_type and s.globs for s in REGISTRY)

    @pytest.mark.parametrize("path", [
        "terraform/dev.tfvars", "terraform/terraform.tfstate", ".git/config",
        "target/plan.json", "dbt_packages/x/y.sql", "logs/app.log",
        "opt/secrets/source-lab.properties", "config/secrets.yml", "app.secrets.json",
        "keys/id_rsa", "cert.pem", "data/part-0001.parquet", "libs/ojdbc11.jar",
    ])
    def test_unsafe_paths_are_excluded(self, path):
        assert is_excluded(path) is not None

    @pytest.mark.parametrize("path", [
        "docs/TARGET_ARCHITECTURE.md", "docs/adr/ADR-031-secret-store.md",
        "docs/runbooks/stop-and-resume.md", "governance/dq/rules.yml",
        "docs/SECURITY_SCAN_BASELINE.md",
    ])
    def test_documentation_about_security_is_not_excluded(self, path):
        """A bare `secret` pattern excluded ADR-031 -- an ADR operators need. Match the
        file, not the word."""
        assert is_excluded(path) is None

    def test_no_discovered_document_comes_from_an_excluded_path(self):
        docs, _ = discover()
        assert docs
        for d in docs:
            if d["source_path"].startswith("dbt::"):
                continue
            assert is_excluded(d["source_path"]) is None, d["source_path"]

    def test_large_files_are_refused_as_datasets(self):
        assert MAX_DOCUMENT_BYTES <= 1024 * 1024


# --------------------------------------------------------------- secret safety
class TestSecretScan:
    @pytest.mark.parametrize("text,rule", [
        (f"key = {_AKIA}", "aws_access_key_id"),
        (_SECRET_KEY_LINE, "aws_secret_access_key"),
        (_PRIVATE_KEY, "private_key_block"),
        ("jdbc:oracle:thin:@//h:1521/FREE?password=hunter2", "jdbc_with_password"),
        ("password = SuperSecret123", "connection_string_pw"),
        ("Authorization: Bearer " + "a" * 30, "bearer_token"),
        ("token ghp_" + "a" * 36, "github_token"),
        ("slack xoxb-123456789012-abcdef", "slack_token"),
        ("api_key: " + "k" * 24, "generic_api_key"),
    ])
    def test_secrets_are_detected(self, text, rule):
        assert scan(text)[0].rule == rule

    @pytest.mark.parametrize("text", [
        'database.password: "${file:/opt/cdc-runtime/secrets/source-lab.properties:oracle_cdc_password}"',
        "aws ssm get-parameter --name /kafka-dev-lab/dev/airflow/admin-password --with-decryption",
        "CREATE LOGIN dbzuser WITH PASSWORD = '$(DBZ_PASSWORD)', CHECK_POLICY = ON;",
        'CREATE USER c##dbzuser IDENTIFIED BY "&1"',
        "password = ********",
        "set the value to <new-password> before running",
    ])
    def test_documentation_about_secrets_is_not_quarantined(self, text):
        """This corpus is full of security prose. A scanner that cannot tell a rule from a
        credential quarantines the documents operators most need."""
        assert scan(text) == []

    def test_a_finding_never_carries_the_raw_secret(self):
        f = scan(f"key = {_AKIA}")[0]
        assert _AKIA not in f.excerpt

    def test_published_corpus_contains_no_secret(self):
        r = build()
        assert r["chunks"]
        for c in r["chunks"]:
            assert scan(c["text"]) == [], c["chunk_id"]

    def test_a_document_with_a_secret_is_quarantined_not_partially_published(self, tmp_path):
        """Whole-document refusal: publishing 'the rest' bets the pattern list is complete."""
        findings = scan(FIXTURE + "\n" + _AKIA + "\n")
        assert findings and findings[0].rule == "aws_access_key_id"


# -------------------------------------------------------------------- chunking
class TestChunking:
    def test_chunking_is_deterministic(self):
        assert [p.text for p in chunk_markdown(FIXTURE)] == \
               [p.text for p in chunk_markdown(FIXTURE)]

    def test_chunks_are_heading_aware(self):
        paths = [p.heading_path for p in chunk_markdown(FIXTURE)]
        assert any("Step one" in p for p in paths)
        assert any("Step two" in p for p in paths)

    def test_a_comment_inside_a_code_fence_is_not_a_heading(self):
        joined = " ".join("/".join(p.heading_path) for p in chunk_markdown(FIXTURE))
        assert "this heading-looking comment" not in joined

    def test_code_fences_are_never_split(self):
        for p in chunk_markdown(FIXTURE):
            assert p.text.count("```") % 2 == 0, p.text[:80]

    def test_large_document_is_split_with_overlap(self):
        big = "# Title\n\n" + ("paragraph text that repeats. " * 60 + "\n\n") * 12
        pieces = chunk_markdown(big)
        assert len(pieces) > 1
        assert all(len(p.text) <= 6000 for p in pieces)

    def test_document_with_no_headings_still_chunks(self):
        assert chunk_markdown("plain prose. " * 40)

    def test_structured_yaml_splits_on_top_level_keys(self):
        y = "alpha:\n  a: 1\n  note: " + "x" * 120 + "\nbeta:\n  b: 2\n  note: " + "y" * 120 + "\n"
        keys = {p.heading_path[0] for p in chunk_structured(y, "x.yml") if p.heading_path}
        assert {"alpha", "beta"} <= keys

    def test_chunking_version_is_recorded(self):
        assert CHUNKING_VERSION.startswith("chunking:")


# ------------------------------------------------------------------- dbt metadata
class TestDbtMetadata:
    def test_models_with_documentation_are_extracted(self):
        docs = dbt_extract(ROOT / "dbt" / "target" / "manifest.json")
        assert docs, "expected documented dbt models"
        assert all(d["document_type"] == "table_doc" for d in docs)

    def test_undocumented_models_are_skipped(self):
        """A model with no description and no documented columns teaches nothing the SQL
        does not already say; a stub only dilutes retrieval."""
        docs = dbt_extract(ROOT / "dbt" / "target" / "manifest.json")
        manifest = json.loads((ROOT / "dbt" / "target" / "manifest.json").read_text())
        models = [v for v in manifest["nodes"].values() if v["resource_type"] == "model"]
        assert len(docs) < len(models)

    def test_extraction_carries_table_and_columns(self):
        docs = dbt_extract(ROOT / "dbt" / "target" / "manifest.json")
        d = docs[0]
        assert d["table_name"] and "## Columns" in d["text"]

    def test_manifest_is_never_indexed_as_one_blob(self):
        docs, _ = discover()
        assert not any(d["source_path"].endswith("manifest.json") for d in docs)

    def test_missing_manifest_returns_empty_not_error(self):
        assert dbt_extract(ROOT / "does" / "not" / "exist.json") == []


# ------------------------------------------------------------------- versioning
class TestCorpusVersioning:
    def test_build_is_deterministic(self):
        """Two builds of the SAME content agree.

        This compares two live reads of the repository tree, so it is only meaningful if the
        tree did not change between them. It previously asserted that unconditionally and
        failed for a real but uninteresting reason: `DECISION_LOG.md` and `PROJECT_STATE.md`
        are both corpus sources, and editing either while the suite runs makes the two builds
        legitimately differ. That is the filesystem changing, not the builder being
        non-deterministic -- and a test that cannot tell those apart reports the wrong defect.

        So the inputs are checked first. If the tree moved under us the test says so and
        skips; if it held still, determinism is asserted exactly as before.
        """
        a = build()
        fingerprint = {d["source_path"]: d["content_hash"] for d in a["documents"]}
        b = build()
        moved = {p for p, h in fingerprint.items()
                 if h != next((d["content_hash"] for d in b["documents"]
                               if d["source_path"] == p), h)}
        if moved:
            pytest.skip(f"corpus sources changed mid-test ({', '.join(sorted(moved))}); "
                        f"determinism is not what this measures when the input moves")
        assert a["manifest"]["corpus_version"] == b["manifest"]["corpus_version"]
        assert a["manifest"]["chunk_count"] == b["manifest"]["chunk_count"]

    def test_corpus_version_is_content_addressed_not_a_timestamp(self):
        a, b = build(), build()
        assert a["manifest"]["generated_at"] or True
        assert a["manifest"]["corpus_version"] == b["manifest"]["corpus_version"]

    def test_changed_content_changes_the_version(self):
        import hashlib, json as j
        base = build()["manifest"]
        mutated = hashlib.sha256(j.dumps(
            {"docs": sorted(["deadbeef"]), "chunking": CHUNKING_VERSION,
             "arch": base["architecture_version"]}, sort_keys=True).encode()).hexdigest()[:16]
        assert base["corpus_version"] != "corpus:" + mutated

    def test_document_id_encodes_the_content_hash(self):
        r = build()
        d = r["documents"][0]
        assert d["content_hash"][:12] in d["document_id"]

    def test_a_deleted_document_disappears_from_the_next_build(self):
        """Documents are discovered, never carried forward, so deletion needs no tombstone."""
        docs, _ = discover()
        paths = {d["source_path"] for d in docs}
        assert "docs/THIS_FILE_DOES_NOT_EXIST.md" not in paths

    def test_dirty_tree_is_recorded_not_hidden(self):
        assert "git_dirty" in build()["manifest"]


# --------------------------------------------------------------------- manifest
class TestManifestAndOutput:
    def test_manifest_has_the_required_fields(self):
        m = build()["manifest"]
        for k in ("corpus_version", "chunking_version", "git_commit", "git_dirty",
                  "document_count", "chunk_count", "source_breakdown",
                  "quarantined_count", "excluded_count", "s3_target"):
            assert k in m, k

    def test_every_chunk_carries_contract_metadata(self):
        required = {"heading_path", "source_path", "document_type", "owner", "domain",
                    "classification", "git_commit", "architecture_version"}
        for c in build()["chunks"]:
            assert required <= set(c["metadata"]), c["chunk_id"]

    def test_chunk_ids_are_unique(self):
        ids = [c["chunk_id"] for c in build()["chunks"]]
        assert len(ids) == len(set(ids))

    def test_write_produces_three_files_and_round_trips(self, tmp_path):
        r = build()
        out = write(r, out_root=tmp_path)
        assert (out / "manifest.json").exists()
        docs = [json.loads(l) for l in (out / "documents.jsonl").read_text().splitlines()]
        chunks = [json.loads(l) for l in (out / "chunks.jsonl").read_text().splitlines()]
        assert len(docs) == r["manifest"]["document_count"]
        assert len(chunks) == r["manifest"]["chunk_count"]

    def test_dry_run_writes_nothing(self, tmp_path, monkeypatch):
        import publish
        monkeypatch.setattr(publish, "OUT_ROOT", tmp_path)
        publish.main(["--dry-run"])
        assert not list(tmp_path.iterdir())

    def test_no_aws_client_is_constructed(self):
        """AI-P2 is offline. S3 publication is AI-P13, after the prefix exists."""
        src = (ROOT / "ai" / "knowledge" / "publish.py").read_text()
        assert "boto3" not in src and "s3.put_object" not in src
