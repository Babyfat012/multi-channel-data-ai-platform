"""Kiểm phạm vi Git và cách tính mutation score, không cần mutmut hay dịch vụ ngoài."""

import subprocess

import pytest

from scripts.evidence import run_changed_mutation
from scripts.evidence.run_changed_mutation import git, select_paths, snapshot, summarize


def commit(repo, message):
    """Lưu fixture bằng Git local với danh tính chỉ áp dụng cho lệnh này."""
    git(repo, "add", ".")
    git(repo, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD").decode().strip()


def test_snapshot_uses_head_and_only_changed_configured_code(tmp_path):
    """Code/test lấy đúng head; file không đổi, bị xóa và file ngoài scope không được mutate."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    (repo / "serving/tests").mkdir(parents=True)
    for name in ("app.py", "stable.py", "deleted.py"):
        (repo / "serving" / name).write_text("value = 1\n")
    (repo / "serving/tests/test_app.py").write_text("def test_ok(): assert True\n")
    (repo / "other.py").write_text("value = 1\n")
    base = commit(repo, "base")
    (repo / "serving/app.py").write_text("value = 2\n")
    (repo / "serving/new.py").write_text("value = 3\n")
    (repo / "serving/tests/test_app.py").write_text("def test_ok(): assert 2 == 2\n")
    (repo / "serving/deleted.py").unlink()
    (repo / "other.py").write_text("value = 2\n")
    head = commit(repo, "head")
    (repo / "serving/app.py").write_text("UNCOMMITTED\n")
    changed = (
        git(
            repo,
            "diff",
            "--no-renames",
            "--diff-filter=AM",
            "--name-only",
            "-z",
            base,
            head,
            "--",
            "*.py",
        )
        .decode()
        .strip("\0")
        .split("\0")
    )
    selected = select_paths(changed, ["serving/"])
    assert selected == ["serving/app.py", "serving/new.py"]
    work = tmp_path / "work"
    work.mkdir()
    config = {"tests_dir": ["serving/tests"], "also_copy": ["serving/tests"]}
    config_text = snapshot(repo, head, work, config, selected)
    assert (work / "serving/app.py").read_text() == "value = 2\n"
    assert "UNCOMMITTED" not in (work / "serving/app.py").read_text()
    assert "stable.py" not in config_text
    # Không copy cả serving đè lên mã mutant sau lúc mutmut sinh mutant.
    assert 'also_copy = ["serving/tests"]' in config_text
    assert (work / "serving/tests/test_app.py").read_text() == "def test_ok(): assert 2 == 2\n"


def test_renames_are_selected_at_new_path(tmp_path):
    """Đổi tên được coi là xóa/thêm để file mới vẫn nằm trong phạm vi mutation."""
    git(tmp_path, "init")
    (tmp_path / "old.py").write_text("value = 1\n")
    base = commit(tmp_path, "base")
    (tmp_path / "old.py").rename(tmp_path / "new.py")
    head = commit(tmp_path, "rename")
    changed = (
        git(tmp_path, "diff", "--no-renames", "--diff-filter=AM", "--name-only", base, head)
        .decode()
        .splitlines()
    )
    assert select_paths(changed, ["new.py"]) == ["new.py"]


def test_no_matching_files_is_empty():
    """Thay docs hoặc test không được dùng làm bằng chứng mutation pass."""
    assert select_paths(["README.md", "serving/tests/test_app.py"], ["serving"]) == []
    assert select_paths(["serving/app.py"], ["serving/other.py"]) == []


def test_score_keeps_no_tests_and_timeouts_separate():
    """Score thô không loại no-tests hay tính timeout thành killed để vượt ngưỡng."""
    output = "\n".join(
        f"    serving.app.x_f__mutmut_{i}: {status}"
        for i, status in enumerate(["killed"] * 8 + ["timeout", "no tests"])
    )
    result = summarize(output)
    assert result["total"] == 10
    assert result["killed_percent_all"] == 80
    assert result["detected_percent_tested"] == 100
    assert not result["passed"]  # Rubric yêu cầu >80, không phải >=80.
    assert summarize(output + "\n    serving.app.x_f__mutmut_10: killed")["passed"]


def test_incomplete_or_missing_results_never_pass():
    """Kết quả rỗng hoặc còn mutant chưa kiểm đều không đạt."""
    assert not summarize("")["passed"]
    output = "\n".join(f"    m.x_f__mutmut_{i}: killed" for i in range(10))
    assert not summarize(output + "\n    m.x_f__mutmut_10: not checked")["passed"]


def test_bad_ref_fails_instead_of_mutating_everything(tmp_path):
    """Ref sai dừng ngay tại Git, không fallback sang tất cả file."""
    git(tmp_path, "init")
    with pytest.raises(subprocess.CalledProcessError):
        git(tmp_path, "rev-parse", "--verify", "--end-of-options", "missing^{commit}")


@pytest.mark.parametrize("dry_run", [False, True])
def test_manifest_skip_and_dry_run_do_not_execute_mutmut(tmp_path, monkeypatch, dry_run):
    """Không có diff thì skip; dry-run có diff chỉ ghi plan và không chạy mutation."""
    import json

    git(tmp_path, "init")
    (tmp_path / "app.py").write_text("value = 1\n")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.mutmut]\npaths_to_mutate = ["app.py"]\ntests_dir = ["tests"]\n'
    )
    base = commit(tmp_path, "base")
    if dry_run:
        (tmp_path / "app.py").write_text("value = 2\n")
        commit(tmp_path, "change")
    monkeypatch.setattr(run_changed_mutation, "ROOT", tmp_path)

    def unexpected_run(*args, **kwargs):
        raise AssertionError("Không được chạy mutmut khi skip hoặc dry-run")

    monkeypatch.setattr(run_changed_mutation, "run_logged", unexpected_run)
    output = tmp_path / "evidence"
    args = ["--base", base, "--output", str(output)] + (["--dry-run"] if dry_run else [])
    assert run_changed_mutation.main(args) == 0
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["status"] == ("planned" if dry_run else "skipped")
    assert manifest["selected_files"] == (["app.py"] if dry_run else [])
    assert "summary" not in manifest
