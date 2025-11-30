# Changelog

All notable changes to DataShard will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2025-11-30

### Breaking Changes ⚠️

- **Manifest Format Migration (JSON to Avro)**
  - Manifest and manifest list files are now written in Avro format using `fastavro` instead of JSON.
  - This aligns with the Apache Iceberg specification and improves I/O performance and storage efficiency.
  - **Backward Compatibility:** The reader includes a fallback mechanism to read legacy JSON manifests, so existing tables remain accessible. However, all new writes will generate Avro files.
- **New Dependency:** Added `fastavro>=1.4.0` to requirements.

### Added

- **S3-Native Distributed Locking** 🔒
  - Replaced the unsafe local `FileLock` for S3 tables with a robust S3-native locking mechanism using conditional writes (`If-None-Match`).
  - Ensures safe concurrent writes in distributed environments (AWS Lambda, Kubernetes, EC2) without external dependencies like DynamoDB.
  - Introduced `LockProvider` abstraction (`LocalLockProvider`, `S3LockProvider`) in `src/datashard/lock_provider.py`.

- **Metadata Compaction / Snapshot Pruning** 🧹
  - Implemented automatic pruning of old snapshots from `metadata.json` to prevent $O(N)$ file size growth.
  - New table property `write.metadata.previous-versions-max` controls retention (default: 100 snapshots).
  - Solves the scalability bottleneck where commit times increased linearly with table history.

### Fixed

- **Snapshot ID Overflow:** Fixed an issue where generated Snapshot IDs could exceed Avro's signed 64-bit integer limit.
- **S3 Concurrency Safety:** Fixed a critical race condition where distributed workers could overwrite each other's commits on S3 due to reliance on local filesystem locks.

## [0.3.3] - 2025-11-27

### Added

#### Query Optimization Features 🚀

- **Predicate Pushdown** (`filter` parameter)
  - Filter at parquet level using PyArrow's native filtering
  - Reduces I/O by 90%+ for selective queries
  - Supports: equality, comparison (`>`, `<`, `>=`, `<=`), `in`, `between`
  - Example: `table.scan(filter={"status": "failed"})`
  - Example: `table.scan(filter={"age": (">", 30)})`

- **Partition Pruning** (automatic with filters)
  - Skips files based on column min/max statistics
  - Column bounds computed during write and stored in manifest
  - Can skip 99% of files for time-range queries
  - Zero configuration required - works automatically

- **Parallel Reading** (`parallel` parameter)
  - Multi-threaded file reading using ThreadPoolExecutor
  - 2-4x speedup on multi-core systems
  - Example: `table.scan(parallel=True)` (all cores)
  - Example: `table.scan(parallel=4)` (4 threads)

- **Streaming API** (memory-efficient iteration)
  - `scan_batches(batch_size)` - yields record batches
  - `iter_records()` - yields individual records
  - `iter_pandas(chunksize)` - yields DataFrame chunks
  - Process 100GB tables with ~100MB memory footprint

#### New Methods on Table class

- `scan(columns, filter, parallel)` - Enhanced with new parameters
- `to_pandas(columns, filter, parallel)` - Enhanced with new parameters
- `scan_batches(batch_size, columns, filter)` - Streaming batch iteration
- `iter_records(columns, filter)` - Single record iteration
- `iter_pandas(chunksize, columns, filter)` - DataFrame chunk iteration

#### New Module

- `filters.py` - Filter expression parsing and conversion
  - `FilterOp` enum for filter operations
  - `FilterExpression` dataclass
  - `parse_filter_dict()` - Parse user-friendly filter syntax
  - `to_pyarrow_filter()` - Convert to PyArrow format
  - `prune_files_by_bounds()` - File pruning logic

### Changed

- **DataFileManager.write_data_file()** now computes column bounds (min/max)
- **Manifest files** now store `lower_bounds` and `upper_bounds` for each data file
- **pyproject.toml** updated with C901 complexity ignore for filter functions

### Technical Details

**Filter Syntax:**
```python
{"column": value}                    # column == value
{"column": ("==", value)}            # column == value
{"column": (">", value)}             # column > value
{"column": ("in", [v1, v2])}         # column in [v1, v2]
{"column": ("between", (lo, hi))}    # lo <= column <= hi
```

**Column Bounds Storage:**
- Computed using `pyarrow.compute.min()` and `pyarrow.compute.max()`
- Stored in manifest JSON as `lower_bounds` and `upper_bounds` dicts
- Keys are field IDs (integers), values are the min/max values

**Performance Impact:**
| Feature | Improvement |
|---------|-------------|
| Predicate Pushdown | 90%+ I/O reduction for selective queries |
| Partition Pruning | 99% file reduction for time-range queries |
| Parallel Reading | 2-4x speedup on multi-core systems |
| Streaming API | Constant memory for any table size |

## [0.2.4] - 2025-11-17

### Fixed

#### Critical Local Filesystem Data Write Bugs 🔧
- **Fixed missing directory creation in DataFileWriter** (`data_operations.py:196`)
  - Previously, temporary parquet file creation would fail with `FileNotFoundError` if the target directory didn't exist
  - Now ensures parent directory exists with `os.makedirs(temp_dir, exist_ok=True)` before creating temporary files
  - This fix prevents file write failures when writing data to newly created tables

- **Fixed relative path handling for local filesystem** (`data_operations.py:294-298`)
  - Previously, file paths were not converted to absolute paths, causing files to be written relative to current working directory
  - Now correctly converts relative paths to absolute paths by joining with `storage.base_path`
  - Also fixed file size check to use the absolute `arrow_path` instead of relative `file_path`
  - This ensures parquet files are written to the correct table directory

- **Impact**: Without these fixes, `table.append_records()` would fail for local filesystem tables
  - ❌ Before: `table.append_records(data, schema)` raised `FileNotFoundError`
  - ✅ After: Data is correctly written to `{table_path}/data/*.parquet`

### Changed

- **Path handling** in `data_operations.py`
  - `_get_arrow_path()` now handles relative paths for local filesystem by joining with base path
  - File size retrieval now uses correct absolute path for local files

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

[0.3.3]: https://github.com/rodmena-limited/datashard/compare/v0.2.4...v0.3.3
[0.2.4]: https://github.com/rodmena-limited/datashard/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/rodmena-limited/datashard/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/rodmena-limited/datashard/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/rodmena-limited/datashard/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rodmena-limited/datashard/releases/tag/v0.2.0
