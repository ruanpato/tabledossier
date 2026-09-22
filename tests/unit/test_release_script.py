"""The release helper used by the release workflow (scripts/release.py)."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import release  # noqa: E402

REPO = "owner/project"
CHANGELOG = """# Changelog

Intro text.

## [Unreleased]

- Work in progress.

## [1.2.0] - 2026-10-01

"Theme": a sentence.

### Added

- A feature, see [the docs](docs/feature.md#usage) and [an issue](https://example.org/1).
- Another one ([anchor](#top)).

### Fixed

- A fix.

## [1.1.0] - Unreleased

- Draft.

## [1.0.0] - 2026-01-01

- First.

[Unreleased]: https://github.com/owner/project/compare/v1.2.0...develop
[1.2.0]: https://github.com/owner/project/releases/tag/v1.2.0
[1.0.0]: https://github.com/owner/project/releases/tag/v1.0.0
"""


def test_the_section_stops_at_the_next_heading_and_at_link_definitions():
    date, body = release.changelog_section(CHANGELOG, "1.2.0")
    assert date == "2026-10-01"
    assert body.startswith('"Theme": a sentence.')
    assert body.endswith("- A fix.")
    assert "## [" not in body
    date, body = release.changelog_section(CHANGELOG, "1.0.0")
    assert body == "- First.", "link definitions are not part of the last section"


def test_missing_and_empty_sections_are_errors():
    with pytest.raises(release.ReleaseError, match=r"no '## \[9.9.9\]' section"):
        release.changelog_section(CHANGELOG, "9.9.9")
    with pytest.raises(release.ReleaseError, match="empty"):
        release.changelog_section("## [2.0.0] - 2026-01-01\n\n## [1.0.0] - x\n- a\n", "2.0.0")
    with pytest.raises(release.ReleaseError):
        release.changelog_section(CHANGELOG, "1.2")  # no prefix matching


def test_a_release_needs_a_dated_section_and_its_tag_link():
    assert release.release_problems(CHANGELOG, "1.2.0", REPO) == []
    problems = release.release_problems(CHANGELOG, "1.1.0", REPO)
    assert any("not dated" in p for p in problems)
    assert any(
        "[1.1.0]: https://github.com/owner/project/releases/tag/v1.1.0" in p for p in problems
    )
    assert release.release_problems(CHANGELOG, "1.0.0", "other/repo") != []


@pytest.mark.parametrize(
    ("tag", "version", "ok"),
    [
        ("v0.4.0", "0.4.0", True),
        ("v1.0.0rc1", "1.0.0rc1", True),
        ("0.4.0", "0.4.0", False),
        ("v0.4.1", "0.4.0", False),
        ("v0.4.0", "0.4", False),
        ("v0.4.0-dev", "0.4.0-dev", False),
    ],
)
def test_the_tag_must_match_the_package_version(tag, version, ok):
    if ok:
        release.check_tag(tag, version)
    else:
        with pytest.raises(release.ReleaseError):
            release.check_tag(tag, version)


def test_notes_contain_the_section_installation_and_absolute_links():
    notes = release.release_notes(CHANGELOG, "1.2.0", REPO)
    assert notes.startswith('"Theme": a sentence.')
    assert "- Work in progress." not in notes and "- Draft." not in notes
    base = "https://github.com/owner/project/blob/v1.2.0/"
    assert f"[the docs]({base}docs/feature.md#usage)" in notes
    assert "[an issue](https://example.org/1)" in notes
    assert "[anchor](#top)" in notes
    assert 'pip install "tabledossier @ git+https://github.com/owner/project@v1.2.0"' in notes
    assert "tabledossier-1.2.0-py3-none-any.whl" in notes
    assert f"[CHANGELOG]({base}CHANGELOG.md)" in notes
    assert "Not published on PyPI." in notes


def test_titles_and_prerelease_flags():
    assert release.release_title("0.4.0") == "TableDossier 0.4.0 (alpha)"
    assert release.release_title("1.0.0") == "TableDossier 1.0.0"
    assert release.is_prerelease("0.4.0") and release.is_prerelease("1.0.0rc1")
    assert not release.is_prerelease("1.0.0")


def test_the_version_is_read_without_importing_the_package(tmp_path):
    path = tmp_path / "_version.py"
    path.write_text('"""Doc."""\n\n__version__ = "3.2.1"\n', encoding="utf-8")
    assert release.read_version(path) == "3.2.1"
    path.write_text("VERSION = '1'\n", encoding="utf-8")
    with pytest.raises(release.ReleaseError):
        release.read_version(path)


def _files(tmp_path, version, changelog):
    version_file = tmp_path / "_version.py"
    version_file.write_text(f'__version__ = "{version}"\n', encoding="utf-8")
    changelog_file = tmp_path / "CHANGELOG.md"
    changelog_file.write_text(changelog, encoding="utf-8")
    return ["--changelog", str(changelog_file), "--version-file", str(version_file)]


def test_check_prints_the_workflow_outputs(tmp_path, capsys):
    args = _files(tmp_path, "1.2.0", CHANGELOG)
    assert release.main(["check", "--tag", "v1.2.0", "--repository", REPO, *args]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "version=1.2.0",
        "title=TableDossier 1.2.0",
        "prerelease=false",
    ]
    assert release.main(["check", "--tag", "v1.2.1", "--repository", REPO, *args]) == 1
    assert "does not match" in capsys.readouterr().err


def test_dry_run_warns_where_a_tag_fails(tmp_path, capsys):
    args = _files(tmp_path, "1.1.0", CHANGELOG)
    assert release.main(["check", "--tag", "v1.1.0", "--repository", REPO, *args]) == 1
    assert "::error::" in capsys.readouterr().err
    assert release.main(["check", "--tag", "v1.1.0", "--repository", REPO, "--dry-run", *args]) == 0
    captured = capsys.readouterr()
    assert "::warning::" in captured.err and "version=1.1.0" in captured.out
    output = tmp_path / "notes.md"
    code = release.main(
        [
            "notes",
            "--tag",
            "v1.1.0",
            "--repository",
            REPO,
            "--dry-run",
            "--output",
            str(output),
            *args,
        ]
    )
    assert code == 0 and output.read_text(encoding="utf-8").startswith("- Draft.")


def test_the_repository_is_consistent_with_its_own_release_rules():
    """The current version has a CHANGELOG section, as the release workflow requires."""
    version = release.read_version()
    _, body = release.changelog_section(release.CHANGELOG.read_text(encoding="utf-8"), version)
    assert body
    release.check_tag(f"v{version}", version)
