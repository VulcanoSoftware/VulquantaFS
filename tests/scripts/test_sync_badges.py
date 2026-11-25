"""The README link sync survives renames and case changes.

GITHUB_REPOSITORY carries the repository's display name (``VulquantaFS``),
while every URL in a README may spell that repository in any case the
author had at writing time -- lowercase like the Pages server, or a name
left over from before a rename. The versioned sync broke on such a
spelling once the repository gained capitals: the case-sensitive regex
no longer matched the Pages links it exists to repoint, so a fork's
README kept sending its readers to the upstream site.
"""

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "sync_badges.py"

_spec = importlib.util.spec_from_file_location("sync_badges", SCRIPT)
assert _spec and _spec.loader
sync_badges = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync_badges)

README_SAMPLE = """\
{start}
<!-- generated block comment -->
[![Tests](https://img.shields.io/github/actions/workflow/status/old/x/test.yml)](https://github.com/old/x)
{end}

[Documentation](https://somebody.github.io/vulquantafs/docs/) ·
[Docker Hub](https://hub.docker.com/r/somebody/vulquantafs)
"""


def sync(repository: str, text: str, branch: str = "master", namespace: str = ""):
    return sync_badges.sync_text(
        text.format(start=sync_badges.START, end=sync_badges.END),
        repository,
        branch,
        namespace,
    )


def test_a_fork_repoints_the_pages_and_docker_hub_links():
    synced = sync("some-owner/VulquantaFS", README_SAMPLE)
    assert "https://some-owner.github.io/VulquantaFS/docs/" in synced
    assert "https://hub.docker.com/r/some-owner/vulquantafs" in synced
    assert "https://github.com/some-owner/VulquantaFS/actions" in synced


def test_a_renamed_repository_heals_lowercase_pages_links():
    synced = sync("some-owner/VulquantaFS", README_SAMPLE)
    assert "somebody.github.io" not in synced


def test_a_mixed_case_repo_name_is_normalized_to_the_pages_spelling():
    text = README_SAMPLE.replace(".github.io/vulquantafs", ".github.io/VulquantaFS")
    synced = sync("some-owner/VulquantaFS", text)
    assert "https://some-owner.github.io/VulquantaFS/docs/" in synced


def test_a_docker_namespace_variable_overrides_the_owner():
    synced = sync("some-owner/VulquantaFS", README_SAMPLE, namespace="Some-Namespace")
    assert "https://hub.docker.com/r/some-namespace/vulquantafs" in synced


def test_a_missing_badge_block_is_an_error():
    text = "## No markers here\n"
    with pytest.raises(ValueError, match="markers were not found"):
        sync("some-owner/VulquantaFS", text)


def test_the_default_repository_is_the_upstream():
    assert sync_badges.UPSTREAM_REPOSITORY == "VulcanoSoftware/VulquantaFS"


README_AFTER_RENAME = """\
{start}
{end}

[Documentation](https://vecstack-prog.github.io/VulquantaFS/docs/) ·
[Getting started](https://vecstack-prog.github.io/VulquantaFS/getting-started/) ·
[Config generator](https://vecstack-prog.github.io/VulquantaFS/config-generator/)
[Fork setup](https://vecstack-prog.github.io/VulquantaFS/docs/developers/fork-setup/)
[Blog](https://someone.github.io/portfolio/)
"""


def test_links_written_under_the_old_owner_and_name_follow_a_rename():
    synced = sync("VulcanoSoftware/VulquantaFS", README_AFTER_RENAME)
    assert "vecstack-prog" not in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/docs/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/getting-started/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/config-generator/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/docs/developers/fork-setup/" in synced


# A README committed while the repository was still called ``tgdcfs``. GitHub
# Pages does not redirect the old project-site URL, so every pre-rename link
# has to be recognised and repointed by name.
README_BEFORE_RENAME = """\
{start}
{end}

[Documentation](https://vulcanosoftware.github.io/tgdcfs/docs/) ·
[Getting started](https://vulcanosoftware.github.io/tgdcfs/getting-started/) ·
[Config generator](https://vulcanosoftware.github.io/tgdcfs/config-generator/)
[Fork setup](https://vulcanosoftware.github.io/tgdcfs/docs/developers/fork-setup/)
"""


def test_a_readme_written_under_the_legacy_name_heals_to_the_new_one():
    synced = sync("VulcanoSoftware/VulquantaFS", README_BEFORE_RENAME)
    assert "tgdcfs" not in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/docs/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/getting-started/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/config-generator/)" in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/docs/developers/fork-setup/" in synced


def test_site_links_under_an_unlisted_old_name_are_healed_too():
    text = README_AFTER_RENAME.replace("VulquantaFS", "SomeOtherOldName")
    synced = sync("VulcanoSoftware/VulquantaFS", text)
    assert "SomeOtherOldName" not in synced
    assert "https://vulcanosoftware.github.io/VulquantaFS/docs/)" in synced


def test_unrelated_github_io_links_are_left_alone():
    synced = sync("VulcanoSoftware/VulquantaFS", README_AFTER_RENAME)
    assert "https://someone.github.io/portfolio/" in synced


def test_the_sync_is_idempotent():
    once = sync("VulcanoSoftware/VulquantaFS", README_AFTER_RENAME)
    again = sync_badges.sync_text(once, "VulcanoSoftware/VulquantaFS", "master")
    assert again == once
