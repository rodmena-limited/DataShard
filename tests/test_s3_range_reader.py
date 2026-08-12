"""S3 parquet reads go through our own backend, not pyarrow's S3 client (#54).

WHY THIS FILE EXISTS

pyarrow's S3FileSystem cannot read from every S3-compatible provider. Against
OVH Object Storage its bundled AWS SDK sends an ``x-amz-checksum-mode`` header
on GetObject and OVH rejects it outright:

    AWS Error [code 134] during GetObject operation:
    Value for x-amz-checksum-mode header is invalid.

boto3 reads the identical object with the identical credentials without
complaint, so datashard now reads parquet through its own storage backend.

These tests use a stub boto3 client, so they run anywhere — no bucket, no
credentials, no network. The real end-to-end proof is
`tests/test_s3_integration.py` against a live bucket.

The property that must not regress is SEEKABILITY. A parquet schema lives in the
file's footer; given a seekable object pyarrow reads a few kilobytes. Replacing
this with `BytesIO(read_file(path))` would still be correct and would silently
turn every schema check into a full download.
"""

from __future__ import annotations

import io

import pytest

from datashard.storage_backend import S3RangeFile


class _StubBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        pass


class _StubS3:
    """Minimal boto3-like client that records the ranges it was asked for."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.ranges: list[str] = []

    def get_object(self, Bucket: str, Key: str, Range: str | None = None):  # noqa: N803
        assert Range is not None, "the range reader must always send a Range header"
        self.ranges.append(Range)
        first, last = Range.removeprefix("bytes=").split("-")
        return {"Body": _StubBody(self.payload[int(first) : int(last) + 1])}


PAYLOAD = bytes(range(256)) * 8  # 2048 bytes, every byte value present


def _reader(payload: bytes = PAYLOAD) -> tuple[S3RangeFile, _StubS3]:
    s3 = _StubS3(payload)
    return S3RangeFile(s3, "bucket", "key", len(payload)), s3


def test_reports_itself_as_seekable() -> None:
    """The whole point. pyarrow only does footer reads on a seekable source."""
    f, _ = _reader()
    assert f.seekable() is True
    assert f.readable() is True
    assert f.writable() is False


def test_sequential_read_matches_the_object() -> None:
    f, _ = _reader()
    assert f.read(10) == PAYLOAD[:10]
    assert f.tell() == 10
    assert f.read(10) == PAYLOAD[10:20]


def test_seek_set_cur_and_end() -> None:
    f, _ = _reader()
    assert f.seek(100) == 100
    assert f.read(4) == PAYLOAD[100:104]

    assert f.seek(10, io.SEEK_CUR) == 114
    assert f.read(4) == PAYLOAD[114:118]

    # SEEK_END with a negative offset is exactly how a parquet footer is found.
    assert f.seek(-8, io.SEEK_END) == len(PAYLOAD) - 8
    assert f.read(8) == PAYLOAD[-8:]


def test_reading_the_footer_transfers_only_the_footer() -> None:
    """The performance guarantee, asserted rather than assumed.

    If someone replaces this with a whole-object fetch, the bytes requested
    stop being proportional to the footer and this fails.
    """
    f, s3 = _reader()
    f.seek(-8, io.SEEK_END)
    got = f.read(8)

    assert got == PAYLOAD[-8:]
    assert len(s3.ranges) == 1, f"expected one range request, got {s3.ranges}"
    first, last = s3.ranges[0].removeprefix("bytes=").split("-")
    transferred = int(last) - int(first) + 1
    assert transferred == 8, (
        f"asked S3 for {transferred} bytes to read an 8-byte footer — a "
        f"whole-object read has crept back in ({len(PAYLOAD)} bytes total)"
    )


def test_read_past_eof_returns_empty_not_an_error() -> None:
    f, _ = _reader()
    f.seek(len(PAYLOAD))
    assert f.read(16) == b""


def test_read_clamps_at_eof() -> None:
    f, _ = _reader()
    f.seek(len(PAYLOAD) - 4)
    assert f.read(100) == PAYLOAD[-4:]


def test_readall_from_offset() -> None:
    f, _ = _reader()
    f.seek(2040)
    assert f.readall() == PAYLOAD[2040:]


def test_negative_seek_is_rejected() -> None:
    f, _ = _reader()
    with pytest.raises(ValueError):
        f.seek(-1)


def test_invalid_whence_is_rejected() -> None:
    f, _ = _reader()
    with pytest.raises(ValueError):
        f.seek(0, 99)


def test_buffered_reader_wrapping_works() -> None:
    """open_seekable wraps this in a BufferedReader; it must survive that."""
    f, s3 = _reader()
    buffered = io.BufferedReader(f, buffer_size=64)
    assert buffered.read(4) == PAYLOAD[:4]
    buffered.seek(1000)
    assert buffered.read(4) == PAYLOAD[1000:1004]
    # Buffering exists to coalesce pyarrow's many small reads into few requests.
    assert len(s3.ranges) <= 3, f"buffering is not coalescing: {s3.ranges}"


def test_local_backend_open_seekable_round_trips(tmp_path) -> None:
    """The local path must be unchanged by all of this."""
    from datashard.storage_backend import LocalStorageBackend

    backend = LocalStorageBackend(str(tmp_path))
    (tmp_path / "f.bin").write_bytes(PAYLOAD)

    with backend.open_seekable("f.bin") as fh:
        assert fh.seekable() is True
        fh.seek(-8, io.SEEK_END)
        assert fh.read(8) == PAYLOAD[-8:]
