"""
Command-line entry point: `datashard migrate <table>` / `datashard relocate <table>`.
"""

import argparse
import json
import sys
from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    from . import __version__

    parser = argparse.ArgumentParser(prog="datashard", description=f"datashard {__version__}")
    sub = parser.add_subparsers(dest="command")
    mig = sub.add_parser("migrate", help="migrate a pre-0.10 table to the Iceberg v2 layout (no downgrade)")
    mig.add_argument("table")
    mig.add_argument("--dry-run", action="store_true", help="report what would be rewritten; write nothing")
    mig.add_argument("--metadata-file", help="the committed legacy metadata file, when the hint is missing and ambiguous")
    rel = sub.add_parser("relocate", help="rewrite metadata so paths point at where the table lives now")
    rel.add_argument("table")
    args = parser.parse_args(argv)

    if args.command == "migrate":
        from .migrate import migrate_table

        report = migrate_table(args.table, dry_run=args.dry_run, metadata_file=args.metadata_file)
        print(json.dumps(report, indent=2))
        return 0
    if args.command == "relocate":
        from .iceberg import load_table

        print(json.dumps(load_table(args.table).relocate(), indent=2))
        return 0

    print(f"datashard {__version__}")
    print("Iceberg v2 tables with ACID commits on local disk and S3-compatible storage.")
    print("Docs: https://datashard.readthedocs.io/   Commands: migrate, relocate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
