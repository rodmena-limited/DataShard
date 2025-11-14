# Changelog

All notable changes to DataShard will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2025-11-14

### Fixed

#### Critical S3 Table Path Bug 🔧
- **Fixed `create_storage_backend()` ignoring table_path for S3 storage**
  - Previously, all S3 tables were created at the bucket root regardless of the `table_path` parameter
  - Now correctly combines `DATASHARD_S3_PREFIX` environment variable with `table_path`
  - Example: `create_table("logs/workflows", schema)` now creates `s3://bucket/logs/workflows/` instead of `s3://bucket/`
  - This fix is critical for multi-table applications using S3 storage

- **Impact**: Without this fix, multiple tables would overwrite each other's data in S3
  - ❌ Before: `create_table("table1", schema)` and `create_table("table2", schema)` both wrote to `s3://bucket/`
  - ✅ After: Tables correctly write to `s3://bucket/table1/` and `s3://bucket/table2/`

### Changed

- **S3 prefix handling** in `storage_backend.py`
  - `create_storage_backend()` now constructs full S3 prefix from both environment variable and table path
  - Logic: `full_prefix = f"{env_prefix}/{table_path}"` (with proper path normalization)
  - Maintains backward compatibility: empty prefixes handled correctly

## [0.2.2] - 2025-01-14

### Added

#### S3-Compatible Storage Support 🎯
- **Complete S3 backend implementation** for distributed workflows
  - AWS S3, MinIO, DigitalOcean Spaces, Wasabi support
  - Environment variable configuration (`DATASHARD_STORAGE_TYPE=s3`)
  - Transparent API - same code works for local and S3 storage

- **Storage Backend Abstraction** (`storage_backend.py`)
  - `StorageBackend` abstract base class
  - `LocalStorageBackend` for filesystem operations
  - `S3StorageBackend` for S3-compatible storage
  - `create_storage_backend()` factory function

- **PyArrow S3FileSystem Integration**
  - Native S3 support for Parquet files
  - Efficient columnar data I/O
  - Compression support maintained

- **Dual-API Architecture**
  - boto3 S3 client for metadata (JSON files, manifests)
  - PyArrow S3FileSystem for data (Parquet files)
  - Coordinated through unified StorageBackend interface

- **Comprehensive Documentation**
  - New `docs/S3_STORAGE.md` - Complete S3 usage guide
  - Updated `README.md` with S3 examples
  - Configuration guide for AWS, MinIO, and other providers
  - Performance benchmarks and cost analysis
  - Security best practices

- **S3 Integration Tests**
  - Full test suite with MinIO
  - Table creation, read/write operations
  - Multiple concurrent transactions
  - Cross-region scenario testing

#### Dependencies
- Added `boto3>=1.26.0` as optional dependency
  - Install with `pip install datashard[s3]`
  - Included in dev dependencies

### Changed

- **All Core Components Updated for S3**
  - `file_manager.py` - Uses storage backend for all file operations
  - `data_operations.py` - S3-aware Parquet I/O with PyArrow
  - `metadata_manager.py` - Storage backend for JSON metadata
  - `iceberg.py` - Storage backend creation in table operations
  - `transaction.py` - Fixed path handling for S3 compatibility

- **Path Handling**
  - Normalized path operations to work with both local and S3
  - Fixed directory existence checks for S3 prefix-based approach
  - Updated file path construction for cross-platform compatibility

### Fixed

- S3 "directory" existence checks now properly query object prefixes
- Path separators normalized for S3 compatibility
- PyArrow S3 paths include bucket prefix for correct routing

### Technical Details

**Storage Backend Interface:**
- `read_file(path) -> bytes`
- `write_file(path, content)`
- `read_json(path) -> dict`
- `write_json(path, data)`
- `exists(path) -> bool`
- `list_files(prefix) -> List[str]`
- `delete_file(path)`
- `makedirs(path, exist_ok)`
- `get_size(path) -> int`

**Environment Variables:**
```bash
DATASHARD_STORAGE_TYPE=s3          # Enable S3 backend
DATASHARD_S3_ENDPOINT=<url>        # S3 endpoint URL
DATASHARD_S3_ACCESS_KEY=<key>      # Access key ID
DATASHARD_S3_SECRET_KEY=<secret>   # Secret access key
DATASHARD_S3_BUCKET=<bucket>       # Bucket name
DATASHARD_S3_REGION=<region>       # AWS region
DATASHARD_S3_PREFIX=<prefix>       # Optional prefix
```

## [0.2.1] - 2025-01-13

### Added
- Workflow execution logging integration
- Comprehensive documentation

### Changed
- Improved pandas integration
- Enhanced schema validation

## [0.2.0] - 2025-01-12

### Added
- Initial public release
- ACID transactions
- Time travel queries
- Safe concurrent writes
- pandas integration
- Optimistic Concurrency Control (OCC)

### Core Features
- Apache Iceberg-inspired architecture
- Pure Python implementation
- No Java dependencies
- Local filesystem storage

---

[0.2.3]: https://github.com/rodmena-limited/datashard/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/rodmena-limited/datashard/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/rodmena-limited/datashard/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rodmena-limited/datashard/releases/tag/v0.2.0
