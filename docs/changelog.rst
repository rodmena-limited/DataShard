Changelog
=========

All notable changes to the datashard project will be documented in this file.

Version 0.2.1 (2025-01-15)
----------------------------

- Complete documentation overhaul with accurate code examples
- Fixed all Schema API examples to use required schema_id and field id
- Updated transaction examples to show correct patterns
- Improved time travel documentation with working examples
- Enhanced concurrency examples with proper Schema usage
- Converted README from .txt to .md with clean Markdown formatting
- Removed all non-ASCII characters and emojis from documentation

Version 0.1.5 (2025-01-14)
----------------------------

- Fixed packaging issues with setuptools_scm dependency
- Synchronized version numbers between pyproject.toml and __init__.py
- Improved build process for better compatibility

Version 0.1.4 (2024-12-XX)
----------------------------

- Added support for more file format operations
- Improved transaction performance
- Enhanced error handling and validation

Version 0.1.3 (2024-11-XX)
----------------------------

Critical Bug Fixes:
- **Fixed manifest creation bug**: Manifest lists were created empty, causing queries to return no data
- Fixed transaction.py line 184 where data_files parameter was missing from _create_manifest_list_with_id()
- Added comprehensive test coverage for manifest creation (6 new tests)
- Removed workaround from Highway engine that directly read parquet files

Improvements:
- Implemented comprehensive metadata management
- Added time travel functionality
- Improved concurrency control with OCC
- Enhanced schema management capabilities

Version 0.1.2 (2024-10-XX)
----------------------------

- Initial release with basic table operations
- Basic ACID transaction support
- Support for Parquet, Avro, and ORC formats
- Simple snapshot and metadata functionality
