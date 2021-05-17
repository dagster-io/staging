import subprocess

import pytest
from automation import git


@pytest.fixture(autouse=True)
def repo(tmpdir):
    with tmpdir.as_cwd():
        subprocess.call(["git", "init"])
        subprocess.call(["git", "config", "user.name", "automation-tests"])
        subprocess.call(["git", "commit", "--allow-empty", "-m", "Initial commit"])

        yield


def test_git_check_status(tmpdir):
    git.git_check_status()

    new_file = tmpdir / "new.txt"
    new_file.write_text("Hello, World!", encoding="utf-8")

    with pytest.raises(Exception):
        git.git_check_status()


def test_git_repo_root(tmpdir):
    assert git.git_repo_root() == tmpdir


def test_get_git_tag():
    with pytest.raises(Exception):
        assert not git.get_git_tag()

    subprocess.call(["git", "tag", "-m", "foo", "foo"])

    assert git.get_git_tag() == "foo"

    subprocess.call(["git", "commit", "--allow-empty", "-m", "Another commit"])

    with pytest.raises(Exception):
        assert not git.get_git_tag()


def test_get_most_recent_git_tag():
    with pytest.raises(Exception):
        assert not git.get_most_recent_git_tag()

    subprocess.call(["git", "tag", "-m", "foo", "foo"])
    subprocess.call(["git", "commit", "--allow-empty", "-m", "Another commit"])

    assert git.get_most_recent_git_tag() == "foo"


def test_get_git_repo_branch():
    assert git.get_git_repo_branch() == "master"

    subprocess.call(["git", "checkout", "-b", "new-branch"])

    assert git.get_git_repo_branch() == "new-branch"


def test_set_git_tag():
    assert git.set_git_tag("0.1.0", dry_run=True) == "0.1.0"

    with pytest.raises(Exception):
        git.get_git_tag()

    assert git.set_git_tag("0.1.0", dry_run=False) == "0.1.0"
    assert git.get_git_tag() == "0.1.0"

    # The tag already exists
    assert git.set_git_tag("0.1.0", dry_run=True) == "0.1.0"
    with pytest.raises(Exception):
        git.set_git_tag("0.1.0", dry_run=False)

    assert git.set_git_tag("0.2.0", signed=True, dry_run=True) == "0.2.0"
    # We can't sign the tag because no GPG key exists
    with pytest.raises(Exception):
        git.set_git_tag("0.2.0", signed=True, dry_run=False)


def test_commit_updates(tmpdir):
    assert "Initial commit" in subprocess.check_output(["git", "log"]).decode("utf-8")
    assert "New commit" not in subprocess.check_output(["git", "log"]).decode("utf-8")

    new_file = tmpdir / "new.txt"
    new_file.write_text("Hello, World!", encoding="utf-8")

    git.git_commit_updates(tmpdir, "New commit")

    assert "Initial commit" in subprocess.check_output(["git", "log"]).decode("utf-8")
    assert "New commit" in subprocess.check_output(["git", "log"]).decode("utf-8")
