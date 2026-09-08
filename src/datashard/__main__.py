"""
Command-line entry point: `datashard verify | migrate | relocate`.
"""

import argparse
import json
import sys
from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    """Run the datashard CLI. Returns the process exit code.

    `verify` exits 1 when the table cannot be read, so it can be used directly as a
    health check; the other commands exit 0 on success and raise on failure.
    """
    from . import __version__

    parser = argparse.ArgumentParser(
        prog="datashard",
        description=f"datashard {__version__} - Iceberg v2 tables with ACID commits",
    )
    parser.add_argument("--version", action="version", version=f"datashard {__version__}")
    sub = parser.add_subparsers(dest="command")

    ver = sub.add_parser(
        "verify",
        help="read every data file the current snapshot references and report whether the table is readable",
        description=(
            "Opens every data file the snapshot references, through the same code path a "
            "scan uses, and prints the report as JSON. Exits 1 when the table is not "
            "readable, so it can be cronned as a health check. Unlike row_count, which "
            "answers from metadata alone, this cannot report green on an unreadable table. "
            "The default answers 'can this be read, and do the rows match the manifests'; "
            "--deep answers 'is every byte as written' and downloads everything."
        ),
    )
    ver.add_argument("table")
    ver.add_argument("--deep", action="store_true",
                     help="also re-hash every file against the checksum recorded at write time (downloads every byte)")
    ver.add_argument("--limit", type=int, metavar="N",
                     help="check only N data files (a sampled check for a very large table)")
    ver.add_argument("--snapshot-id", type=int, help="verify a historical snapshot instead of the current one")

    mig = sub.add_parser("migrate", help="migrate a pre-0.10 table to the Iceberg v2 layout (no downgrade)")
    mig.add_argument("table")
    mig.add_argument("--dry-run", action="store_true",
                     help="report what would be rewritten, and the headroom it needs; write nothing")
    mig.add_argument("--metadata-file", help="the committed legacy metadata file, when the hint is missing and ambiguous")

    rel = sub.add_parser("relocate", help="rewrite metadata so paths point at where the table lives now")
    rel.add_argument("table")
    args = parser.parse_args(argv)

    if args.command == "verify":
        from .table import Table

        # Deliberately NOT load_table(): that raises for a missing or un-migrated
        # table, and a health check must answer in every case rather than traceback.
        # Table(create_if_not_exists=False) reads nothing until verify() does, and
        # verify() reports what it finds.
        report = Table(args.table, create_if_not_exists=False).verify(
            deep=args.deep, limit=args.limit, snapshot_id=args.snapshot_id
        )
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "migrate":
        from .migrate import migrate_table

        print(json.dumps(migrate_table(args.table, dry_run=args.dry_run, metadata_file=args.metadata_file), indent=2))
        return 0
    if args.command == "relocate":
        from .iceberg import load_table

        print(json.dumps(load_table(args.table).relocate(), indent=2))
        return 0

    print(f"datashard {__version__}")
    print("Iceberg v2 tables with ACID commits on local disk and S3-compatible storage.")
    print("Docs: https://datashard.readthedocs.io/   Commands: verify, migrate, relocate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
