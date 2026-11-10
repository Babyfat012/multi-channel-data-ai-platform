"""infer/diff/check-registry/gate CLI, standard library only."""

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from spark_jobs.lib.data_contracts.compat import diff, verdict
from spark_jobs.lib.data_contracts.gate import gate_root
from spark_jobs.lib.data_contracts.registry import Registry
from spark_jobs.lib.data_contracts.schema import infer_schema


def main(argv=None):
    """Print JSON and return an error code for a breaking registry or diff."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    infer = commands.add_parser("infer")
    infer.add_argument("files", nargs="+")
    infer.add_argument("--output")
    compare = commands.add_parser("diff")
    compare.add_argument("old")
    compare.add_argument("new")
    compare.add_argument(
        "--mode", default="BACKWARD", choices=["BACKWARD", "FORWARD", "FULL", "NONE"]
    )
    compare.add_argument("--migrations", help="JSON file mapping old path -> new")
    check = commands.add_parser("check-registry")
    check.add_argument("--contracts-dir", default=None)
    gate = commands.add_parser("gate")
    gate.add_argument("root")
    gate.add_argument("--contracts-dir", default=None)
    args = parser.parse_args(argv)
    code = 0
    if args.command == "infer":
        result = infer_schema(
            json.loads(line)
            for filename in args.files
            for line in Path(filename).read_text().splitlines()
            if line.strip()
        )
    elif args.command == "diff":
        migrations = json.loads(Path(args.migrations).read_text()) if args.migrations else None
        changes = diff(
            json.loads(Path(args.old).read_text()),
            json.loads(Path(args.new).read_text()),
            migrations,
        )
        compatible = verdict(changes, args.mode)
        result = {
            "mode": args.mode,
            "compatible": compatible,
            "changes": [asdict(c) for c in changes],
        }
        code = 0 if compatible else 1
    elif args.command == "check-registry":
        errors = Registry(args.contracts_dir).check_registry()
        result = {"errors": errors}
        code = int(bool(errors))
    else:
        result = gate_root(args.root, Registry(args.contracts_dir))
    output = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.command == "infer" and args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(output)
    else:
        print(output, end="")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
