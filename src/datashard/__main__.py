"""
Main entry point for the Python Iceberg implementation
"""


def main() -> None:
    """Main entry point when module is executed directly"""
    from . import __version__

    print(f"datashard {__version__}")
    print("=" * 30)
    print("A Python implementation of Apache Iceberg concepts:")
    print("ACID transactions, snapshots, and metadata management")
    print("for local filesystem and S3-compatible storage.")
    print()
    print("Docs:  https://datashard.readthedocs.io/")
    print("Usage: python -c 'import datashard; t = datashard.create_table(...)'")


if __name__ == "__main__":
    main()
