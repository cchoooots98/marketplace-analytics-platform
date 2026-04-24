from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DOC_PATHS = (
    REPO_ROOT / "README.md",
    *sorted((REPO_ROOT / "docs").glob("*.md")),
)
LOCAL_MARKDOWN_LINK_PATTERN = re.compile(r"\[[^\]]+\]\((?!https?://|mailto:)([^)#]+)")
BANNED_PUBLIC_TERMS = (
    "portfolio",
    "recruiter",
    "hiring manager",
    "interview-ready",
    "portfolio-grade",
)


def test_public_docs_avoid_job_search_language() -> None:
    """Keep public-facing documents in repository-internal enterprise tone."""
    for document_path in PUBLIC_DOC_PATHS:
        document_text = document_path.read_text(encoding="utf-8").lower()
        for banned_term in BANNED_PUBLIC_TERMS:
            assert (
                banned_term not in document_text
            ), f"Found banned public term {banned_term!r} in {document_path.name}"


def test_markdown_links_resolve_to_local_files() -> None:
    """Ensure README and docs link only to existing local markdown targets."""
    for document_path in PUBLIC_DOC_PATHS:
        document_text = document_path.read_text(encoding="utf-8")
        for relative_target in LOCAL_MARKDOWN_LINK_PATTERN.findall(document_text):
            if relative_target.startswith("#"):
                continue

            target_path = (document_path.parent / relative_target).resolve()
            assert target_path.exists(), (
                f"Broken local markdown link {relative_target!r} in "
                f"{document_path.name}"
            )


def test_readme_reader_paths_cover_primary_docs() -> None:
    """Keep the main repository entrypoint wired to the core operating docs."""
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    required_links = (
        "docs/architecture.md",
        "docs/data_contracts.md",
        "docs/metric_definitions.md",
        "docs/operations_runbook.md",
        "docs/dashboard_specs.md",
        "docs/decisions.md",
    )

    for required_link in required_links:
        assert required_link in readme_text
