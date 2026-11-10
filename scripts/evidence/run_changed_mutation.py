"""Select the files changed between two commits and run mutmut on a Git snapshot in a temp directory."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import tomllib

ROOT = Path(__file__).resolve().parents[2]
TERMINAL_STATUSES = {"killed", "survived", "timeout", "no tests"}


def git(repo, *args):
    """Read Git with a separate argv; stop if the ref or command is invalid."""
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True).stdout


def select_paths(changed, configured):
    """Intersect the changed Python files with the configured scope, always excluding tests."""
    selected = []
    for name in sorted(set(changed)):
        path = PurePosixPath(name)
        if path.suffix != ".py" or {"tests", "test"}.intersection(path.parts):
            continue
        if path.name.startswith("test_") or path.name.endswith("_test.py"):
            continue
        if any(name == scope or name.startswith(scope.rstrip("/") + "/") for scope in configured):
            selected.append(name)
    return selected


def summarize(output):
    """Count mutmut's actual statuses; keep mutants without tests in the raw denominator."""
    statuses = re.findall(r"^\s+\S+__mutmut_\d+: ([^\r\n]+)$", output, re.MULTILINE)
    counts = Counter(statuses)
    total = len(statuses)
    tested = total - counts["no tests"]
    complete = bool(total) and set(statuses) <= TERMINAL_STATUSES
    return {
        "counts": dict(counts),
        "total": total,
        "complete": complete,
        "killed_percent_all": 100 * counts["killed"] / total if total else None,
        "killed_percent_tested": 100 * counts["killed"] / tested if tested else None,
        "detected_percent_tested": (
            100 * (counts["killed"] + counts["timeout"]) / tested if tested else None
        ),
        # Conservative threshold: killed only, counting no tests, without folding timeouts into killed.
        "passed": complete and counts["killed"] / total > 0.8,
    }


def snapshot(repo, head, target, config, selected):
    """Export code and internal dependencies from head and create a dedicated mutation config."""
    paths = selected + config["tests_dir"] + config.get("also_copy", [])
    roots = sorted({PurePosixPath(p).parts[0] for p in paths})
    archive = git(repo, "archive", "--format=tar", head, "--", *roots)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(target, filter="data")
    options = {
        "paths_to_mutate": selected,
        "tests_dir": config["tests_dir"],
        "also_copy": config.get("also_copy", []),
    }
    text = '[tool.pytest.ini_options]\npythonpath = ["."]\n\n[tool.mutmut]\n'
    text += "\n".join(f"{key} = {json.dumps(value)}" for key, value in options.items()) + "\n"
    (target / "pyproject.toml").write_text(text)
    return text


def run_logged(command, cwd, log):
    """Save full stdout/stderr and return the exit code so execution errors are not hidden."""
    print(f"Running {' '.join(command)}; log: {log}", flush=True)
    env = os.environ.copy()
    for name in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
        env.pop(name, None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    with log.open("w") as output:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
    return result.returncode


def main(argv=None):
    """Write the scope manifest before running; skip is not pass, a low score returns an error."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    parser.add_argument(
        "--output", type=Path, required=True, help="New directory to store the evidence"
    )
    parser.add_argument("--max-children", type=int, default=2)
    parser.add_argument(
        "--dry-run", action="store_true", help="Only write the scope, do not run mutmut"
    )
    args = parser.parse_args(argv)
    if args.max_children < 1:
        parser.error("--max-children must be >= 1")
    base = (
        git(ROOT, "rev-parse", "--verify", "--end-of-options", args.base + "^{commit}")
        .decode()
        .strip()
    )
    head = (
        git(ROOT, "rev-parse", "--verify", "--end-of-options", args.head + "^{commit}")
        .decode()
        .strip()
    )
    config_bytes = (ROOT / "pyproject.toml").read_bytes()
    config = tomllib.loads(config_bytes.decode())["tool"]["mutmut"]
    changed = (
        git(
            ROOT,
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
        .split("\0")
    )
    changed = sorted(name for name in changed if name)
    selected = select_paths(changed, config["paths_to_mutate"])
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "base": base,
        "head": head,
        "comparison": "base tree to head tree; added/modified files; renames treated as delete/add",
        "scope": config["paths_to_mutate"],
        "changed_python_files": changed,
        "selected_files": selected,
        "excluded_files": sorted(set(changed) - set(selected)),
        "granularity": "whole changed files, not changed lines",
        "runner_config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "source_sha256": {
            name: hashlib.sha256(git(ROOT, "show", f"{head}:{name}")).hexdigest()
            for name in selected
        },
        "status": "planned" if selected else "skipped",
    }
    manifest_path = args.output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2), flush=True)
    if not selected or args.dry_run:
        return 0
    version = importlib.metadata.version("mutmut")
    if version != "3.3.1":
        raise RuntimeError(f"Requires mutmut 3.3.1, found {version}")
    manifest["mutmut_version"] = version
    manifest["status"] = "error"
    try:
        with tempfile.TemporaryDirectory(prefix="changed-mutation-") as directory:
            work = Path(directory)
            config_text = snapshot(ROOT, head, work, config, selected)
            (args.output / "mutation-config.toml").write_text(config_text)
            executable = shutil.which("mutmut")
            if not executable:
                raise RuntimeError("mutmut CLI not found in the runtime environment")
            command = [executable]
            run_code = run_logged(
                [*command, "run", "--max-children", str(args.max_children)],
                work,
                args.output / "run.txt",
            )
            manifest["run_exit_code"] = run_code
            result_code = run_logged(
                [*command, "results", "--all", "true"], work, args.output / "results.txt"
            )
            manifest["results_exit_code"] = result_code
            summary = summarize((args.output / "results.txt").read_text())
            manifest["summary"] = summary
            manifest["status"] = (
                "passed" if run_code == result_code == 0 and summary["passed"] else "failed"
            )
    finally:
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2), flush=True)
    return 0 if manifest["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
