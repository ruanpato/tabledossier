"""Release helper used by ``.github/workflows/release.yml`` (standard library only).

Not part of the TableDossier package. It checks that a release tag matches the
package version and that the CHANGELOG has a section for it, and it turns that
section into the notes of the GitHub release::

    python scripts/release.py version
    python scripts/release.py check --tag v0.4.0 [--dry-run]
    python scripts/release.py notes --tag v0.4.0 --output notes.md [--repository owner/name]

``check`` prints ``key=value`` lines for ``$GITHUB_OUTPUT``. With ``--dry-run``
(pull requests), problems that only matter for a published release, such as an
undated section, are reported as warnings instead of errors.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "src" / "tabledossier" / "_version.py"
CHANGELOG = ROOT / "CHANGELOG.md"
VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?$")
HEADING = re.compile(r"^## \[(?P<version>[^\]]+)\](?: - (?P<date>.+?))?\s*$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
LINK_DEFINITION = re.compile(r"^\[[^\]]+\]: \S+")
RELATIVE_LINK = re.compile(r"\]\((?!https?://|#|mailto:)([^)\s]+)\)")


class ReleaseError(ValueError):
    """Raised when the repository is not ready for the requested release."""


def read_version(path: Path = VERSION_FILE) -> str:
    """Return ``__version__`` from ``_version.py`` without importing the package."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__version__" for t in node.targets)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            return node.value.value
    raise ReleaseError(f"{path} does not assign a string to __version__")


def check_tag(tag: str, version: str) -> None:
    """Raise :class:`ReleaseError` unless ``tag`` is ``v`` followed by ``version``."""
    if not VERSION_PATTERN.match(version):
        raise ReleaseError(f"package version {version!r} is not X.Y.Z (optionally aN, bN or rcN)")
    if tag != f"v{version}":
        raise ReleaseError(f"tag {tag!r} does not match the package version {version!r}")


def changelog_section(text: str, version: str) -> tuple[str | None, str]:
    """Return ``(date, body)`` of the ``## [version] - date`` section of a CHANGELOG.

    The body stops at the next ``## `` heading or at the link definitions at the
    end of the file; surrounding blank lines are removed.
    """
    lines = text.splitlines()
    start = None
    date = None
    for index, line in enumerate(lines):
        match = HEADING.match(line)
        if match and match.group("version") == version:
            start, date = index + 1, match.group("date")
            break
    if start is None:
        raise ReleaseError(f"CHANGELOG.md has no '## [{version}]' section")
    body = []
    for line in lines[start:]:
        if line.startswith("## ") or LINK_DEFINITION.match(line):
            break
        body.append(line)
    content = "\n".join(body).strip("\n")
    if not content.strip():
        raise ReleaseError(f"the CHANGELOG section of {version} is empty")
    return date, content


def release_problems(text: str, version: str, repository: str) -> list[str]:
    """Return what would make ``version`` an incomplete release (empty when ready)."""
    date, _ = changelog_section(text, version)
    problems = []
    if date is None or not DATE.match(date):
        problems.append(
            f"the CHANGELOG section of {version} is not dated (## [{version}] - YYYY-MM-DD)"
        )
    link = f"[{version}]: https://github.com/{repository}/releases/tag/v{version}"
    if link not in text.splitlines():
        problems.append(f"CHANGELOG.md does not define the link {link}")
    return problems


def release_title(version: str) -> str:
    """Return the release title; 0.x releases are alpha releases."""
    return f"TableDossier {version}" + (" (alpha)" if version.startswith("0.") else "")


def is_prerelease(version: str) -> bool:
    """0.x versions and aN/bN/rcN versions are published as pre-releases."""
    return version.startswith("0.") or not re.match(r"^\d+\.\d+\.\d+$", version)


def release_notes(text: str, version: str, repository: str) -> str:
    """Return the release notes: the CHANGELOG section, installation and links."""
    _, body = changelog_section(text, version)
    tag = f"v{version}"
    base = f"https://github.com/{repository}/blob/{tag}/"
    body = RELATIVE_LINK.sub(lambda match: f"]({base}{match.group(1).lstrip('./')})", body)
    return "\n".join(
        [
            body,
            "",
            "### Install",
            "",
            "```bash",
            f'python -m pip install "tabledossier @ git+https://github.com/{repository}@{tag}"',
            "# or the wheel attached to this release",
            f"python -m pip install tabledossier-{version}-py3-none-any.whl",
            "```",
            "",
            "Not published on PyPI.",
            "",
            f"Full list: [CHANGELOG]({base}CHANGELOG.md). Checksums: `SHA256SUMS`.",
            "",
        ]
    )


def _warn(message: str) -> None:
    print(f"::warning::{message}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    """Run the command line; return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version", help="print the package version")
    for name in ("check", "notes"):
        command = commands.add_parser(name)
        command.add_argument("--tag", required=True, help="release tag, e.g. v0.4.0")
        command.add_argument("--repository", default="ruanpato/tabledossier", help="owner/name")
        command.add_argument(
            "--dry-run", action="store_true", help="warn instead of failing on undated sections"
        )
        command.add_argument("--changelog", type=Path, default=CHANGELOG)
        command.add_argument("--version-file", type=Path, default=VERSION_FILE)
        if name == "notes":
            command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "version":
            print(read_version())
            return 0
        version = read_version(args.version_file)
        check_tag(args.tag, version)
        text = args.changelog.read_text(encoding="utf-8")
        problems = release_problems(text, version, args.repository)
        for problem in problems:
            if not args.dry_run:
                raise ReleaseError(problem)
            _warn(problem)
        if args.command == "check":
            print(f"version={version}")
            print(f"title={release_title(version)}")
            print(f"prerelease={'true' if is_prerelease(version) else 'false'}")
        else:
            args.output.write_text(release_notes(text, version, args.repository), encoding="utf-8")
            print(f"Wrote {args.output}")
    except ReleaseError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
