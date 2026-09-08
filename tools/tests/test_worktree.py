"""Integration tests for tools/worktree's nested linked-worktree topology."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


TOOL = Path(__file__).parents[1] / "worktree"


def run(*arguments, cwd, check=True):
    environment = os.environ.copy()
    environment.update({
        "GIT_AUTHOR_NAME": "Worktree Test",
        "GIT_AUTHOR_EMAIL": "worktree@example.invalid",
        "GIT_COMMITTER_NAME": "Worktree Test",
        "GIT_COMMITTER_EMAIL": "worktree@example.invalid",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "protocol.file.allow",
        "GIT_CONFIG_VALUE_0": "always",
    })
    result = subprocess.run(
        [str(argument) for argument in arguments],
        cwd=cwd,
        env=environment,
        text=True,
        capture_output=True,
    )
    if check and result.returncode:
        pytest.fail(
            f"command failed ({result.returncode}): {arguments}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    return result


def git(repository, *arguments, check=True):
    return run("git", "-C", repository, *arguments, cwd=repository, check=check)


@pytest.fixture
def project(tmp_path):
    submodule = tmp_path / "submodule"
    submodule.mkdir()
    git(submodule, "init", "-q", "-b", "main")
    (submodule / "file").write_text("base\n")
    git(submodule, "add", "file")
    git(submodule, "commit", "-qm", "initial submodule commit")

    superproject = tmp_path / "superproject"
    superproject.mkdir()
    git(superproject, "init", "-q", "-b", "main")
    git(superproject, "submodule", "add", "-q", str(submodule), "sub")
    git(superproject, "commit", "-qam", "initial superproject commit")
    return superproject, submodule


def tool(project, *arguments, check=True, cwd=None):
    return run(TOOL, *arguments, cwd=cwd or project, check=check)


def common_dir(repository):
    return Path(git(
        repository, "rev-parse", "--path-format=absolute", "--git-common-dir"
    ).stdout.strip())


def test_create_shares_submodule_common_dir_and_refs(project):
    superproject, _ = project
    tool(superproject, "create", "demo")
    linked = superproject / ".worktrees" / "demo"

    assert common_dir(superproject / "sub") == common_dir(linked / "sub")
    git(linked / "sub", "branch", "shared-from-linked")
    assert git(
        superproject / "sub",
        "show-ref", "--verify", "--quiet", "refs/heads/shared-from-linked",
        check=False,
    ).returncode == 0

    tool(superproject, "remove", "demo")
    assert not linked.exists()
    listing = git(superproject / "sub", "worktree", "list", "--porcelain").stdout
    assert str(linked / "sub") not in listing
    tool(superproject, "clean", "demo")
    assert git(
        superproject,
        "show-ref", "--verify", "--quiet", "refs/heads/worktree/demo",
        check=False,
    ).returncode == 1


def test_remove_refuses_dirty_submodule_without_force(project):
    superproject, _ = project
    tool(superproject, "create", "dirty")
    linked = superproject / ".worktrees" / "dirty"
    file = linked / "sub" / "file"
    file.write_text("valuable uncommitted change\n")

    result = tool(superproject, "remove", "dirty", check=False)
    assert result.returncode == 1
    assert "submodule has staged, unstaged, or untracked changes" in result.stderr
    assert file.read_text() == "valuable uncommitted change\n"

    tool(superproject, "remove", "dirty", "--force")
    assert not linked.exists()


def test_remove_recovers_after_submodule_was_deinitialized(project):
    superproject, _ = project
    tool(superproject, "create", "deinitialized")
    linked = superproject / ".worktrees" / "deinitialized"

    # Submodule porcelain may clear the path but leave the shared repository's
    # linked-worktree registration stale.
    git(linked, "submodule", "deinit", "-f", "--", "sub", check=False)
    tool(superproject, "remove", "deinitialized")

    assert not linked.exists()
    listing = git(superproject / "sub", "worktree", "list", "--porcelain").stdout
    assert str(linked / "sub") not in listing


def test_remove_refuses_foreign_replacement_at_managed_path(project):
    superproject, submodule = project
    tool(superproject, "create", "foreign")
    linked = superproject / ".worktrees" / "foreign"
    linked_submodule = linked / "sub"
    shared = common_dir(superproject / "sub")

    run(
        "git", f"--git-dir={shared}", "worktree", "remove", "--force",
        linked_submodule,
        cwd=superproject,
    )
    git(superproject, "clone", "-q", str(submodule), str(linked_submodule))

    result = tool(superproject, "remove", "foreign", "--force", check=False)
    assert result.returncode == 1
    assert "foreign repository" in result.stderr
    assert (linked_submodule / "file").exists()


def test_invocation_from_linked_worktree_still_uses_main_base(project):
    superproject, _ = project
    tool(superproject, "create", "first")
    first = superproject / ".worktrees" / "first"

    tool(superproject, "create", "second", cwd=first)
    second = superproject / ".worktrees" / "second"
    assert second.is_dir()
    assert not (first / ".worktrees" / "second").exists()

    tool(superproject, "remove", "second")
    tool(superproject, "remove", "first")


def test_committed_submodule_advance_is_safe_to_remove(project):
    superproject, _ = project
    tool(superproject, "create", "advanced")
    linked = superproject / ".worktrees" / "advanced"
    linked_submodule = linked / "sub"

    (linked_submodule / "file").write_text("new committed contents\n")
    git(linked_submodule, "add", "file")
    git(linked_submodule, "commit", "-qm", "advance shared submodule")
    git(linked, "add", "sub")
    git(linked, "commit", "-qm", "record advanced submodule")

    tool(superproject, "remove", "advanced")
    result = tool(superproject, "clean", "advanced", check=False)
    assert result.returncode == 1
    assert "not fully merged" in result.stderr
    tool(superproject, "clean", "advanced", "--force")


def test_nested_submodules_are_removed_leaf_first(tmp_path):
    leaf = tmp_path / "leaf"
    leaf.mkdir()
    git(leaf, "init", "-q", "-b", "main")
    (leaf / "leaf-file").write_text("leaf\n")
    git(leaf, "add", "leaf-file")
    git(leaf, "commit", "-qm", "leaf")

    parent = tmp_path / "parent"
    parent.mkdir()
    git(parent, "init", "-q", "-b", "main")
    (parent / "parent-file").write_text("parent\n")
    git(parent, "add", "parent-file")
    git(parent, "commit", "-qm", "parent")
    git(parent, "submodule", "add", "-q", str(leaf), "nested")
    git(parent, "commit", "-qam", "add nested")

    superproject = tmp_path / "superproject"
    superproject.mkdir()
    git(superproject, "init", "-q", "-b", "main")
    git(superproject, "submodule", "add", "-q", str(parent), "sub")
    git(superproject, "commit", "-qam", "add parent")
    git(superproject, "submodule", "update", "--init", "--recursive")

    tool(superproject, "create", "nested")
    linked = superproject / ".worktrees" / "nested"
    assert common_dir(
        superproject / "sub" / "nested"
    ) == common_dir(linked / "sub" / "nested")

    tool(superproject, "remove", "nested")
    assert not linked.exists()


def test_failed_create_rolls_back_outer_worktree_and_branch(project):
    superproject, _ = project
    git(superproject, "submodule", "deinit", "-f", "--", "sub")

    result = tool(superproject, "create", "rollback", check=False)
    assert result.returncode == 1
    assert "source submodule" in result.stderr
    assert not (superproject / ".worktrees" / "rollback").exists()
    assert git(
        superproject,
        "show-ref", "--verify", "--quiet", "refs/heads/worktree/rollback",
        check=False,
    ).returncode == 1

    status = tool(superproject, "status", "rollback", check=False)
    assert status.returncode == 0
    assert "rolled_back" in status.stdout
    tool(superproject, "clean", "rollback")


def test_create_absorbs_old_form_source_submodule(project):
    superproject, _ = project
    source = superproject / "sub"
    embedded = source / ".git"
    absorbed = common_dir(source)

    git(source, "config", "--unset", "core.worktree")
    embedded.unlink()
    shutil.move(absorbed, embedded)
    assert embedded.is_dir()

    tool(superproject, "create", "old-form")
    linked = superproject / ".worktrees" / "old-form"
    assert embedded.is_file()
    assert common_dir(source) == common_dir(linked / "sub")
    assert not common_dir(source).is_relative_to(source)

    tool(superproject, "remove", "old-form")
