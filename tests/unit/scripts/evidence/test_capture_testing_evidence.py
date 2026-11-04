"""Phần dựng output mutation của capture_testing_evidence (không cần Chrome hay chạy lại mutmut)."""

import json

import pytest

from scripts.evidence.capture_testing_evidence import mutation_output


def run_dir(tmp_path, status="passed", progress="⠦ 245/245  🎉 219 🫥 14  ⏰ 0  🤔 0  🙁 12  🔇 0"):
    """Thư mục kết quả giả cùng cấu trúc `run_changed_mutation.py`: run.txt (có spinner) và manifest.json."""
    spinner = "⠋ 1/245  🎉 0 🫥 1  ⏰ 0  🤔 0  🙁 0  🔇 0\r"
    (tmp_path / "run.txt").write_text(spinner + progress + "\n1.79 mutations/second\n")
    manifest = {
        "mutmut_version": "3.3.1",
        "base": "3cd3fcde78b736b86cab578983ea19a1f5e96b98",
        "head": "89cd44e5d3b2405b58ea71bd4c8400b354f5908f",
        "selected_files": ["serving/app.py"],
        "status": status,
        "run_exit_code": 0,
        "results_exit_code": 0,
        "started_at": "2026-11-04T15:10:35+00:00",
        "finished_at": "2026-11-04T15:12:55+00:00",
        "summary": {"killed_percent_all": 89.39, "killed_percent_tested": 94.81},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


def test_mutation_output_uses_final_progress_line_not_first_spinner(tmp_path):
    """Lấy dòng tiến độ CUỐI (kết quả), không lấy dòng spinner đầu; ghi cả score và mốc Git."""
    text, code = mutation_output(run_dir(tmp_path))
    assert code == 0
    assert "245/245  killed 219  no tests 14  timeout 0  suspicious 0  survived 12" in text
    assert "1/245" not in text and "3cd3fcd..89cd44e" in text and "serving/app.py" in text
    assert "89.4%" in text and "94.8%" in text


def test_mutation_output_fails_when_manifest_not_passed_or_no_progress(tmp_path):
    """Manifest không passed ⇒ mã thoát 1; run.txt không có dòng tiến độ ⇒ lỗi rõ, không chụp ảnh rỗng."""
    failed = tmp_path / "failed"
    failed.mkdir()
    assert mutation_output(run_dir(failed, status="failed"))[1] == 1
    empty = tmp_path / "empty"
    empty.mkdir()
    run_dir(empty)
    (empty / "run.txt").write_text("không có tiến độ\n")
    with pytest.raises(ValueError, match="không có dòng tiến độ"):
        mutation_output(empty)
