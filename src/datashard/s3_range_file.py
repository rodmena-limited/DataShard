"""
Seekable, range-reading file objects over S3 objects (split out of
storage_backend.py for the 500-line file cap, #71).
"""

import io
from typing import Any, Optional, Tuple

try:
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - boto3 optional
    ClientError = Exception


class S3FileStream:
    """Wrapper for S3 StreamingBody to support context manager protocol."""

    def __init__(self, body: Any):
        self.body = body

    def read(self, n: Optional[int] = None) -> bytes:
        data: bytes = self.body.read(n)
        return data

    def close(self) -> None:
        self.body.close()

    def __enter__(self) -> "S3FileStream":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

class S3RangeFile(io.RawIOBase):
    """A seekable, range-reading file object over an S3 object (#54).

    WHY THIS EXISTS

    pyarrow's own S3FileSystem cannot read from every S3-compatible provider.
    Against OVH Object Storage its bundled AWS SDK sends an
    ``x-amz-checksum-mode`` header on GetObject and OVH rejects it:

        AWS Error [code 134] during GetObject operation:
        Value for x-amz-checksum-mode header is invalid.

    boto3 reads the identical object, from the identical bucket, with the
    identical credentials, without complaint. So the fix is to read parquet
    through OUR backend rather than pyarrow's.

    WHY SEEKABLE, RATHER THAN JUST BytesIO(read_file(path))

    A parquet file's schema lives in its FOOTER. Given a seekable file object,
    pyarrow reads the last few bytes, then the footer, then only the column
    chunks it actually needs — kilobytes, not the whole object. Handing it a
    fully-materialised BytesIO would be simpler but would download the entire
    data file to answer "what is its schema", turning an O(footer) operation
    into O(file). On a table of any size that is the difference between a
    schema check and a full transfer.

    Wrap this in io.BufferedReader (see ``open_seekable``) so pyarrow's many
    small reads coalesce into few HTTP range requests.

    SIZE IS LEARNT LAZILY (#67). Parquet readers start at the end of the file,
    so the first request is a suffix-range GET of the last TAIL_BYTES: its
    Content-Range header carries the object size and its body IS the footer,
    which is cached. A small file therefore costs ONE request instead of a HEAD
    plus two or three GETs. Pass `size` to skip the tail fetch.
    """

    TAIL_BYTES = 64 * 1024

    def __init__(self, s3: Any, bucket: str, key: str, size: Optional[int] = None) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._key = key
        self._size: Optional[int] = size
        self._pos = 0
        self._tail: bytes = b""
        self._tail_start = 0

    # --- capabilities -----------------------------------------------------
    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    # --- positioning ------------------------------------------------------
    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new = offset
        elif whence == io.SEEK_CUR:
            new = self._pos + offset
        elif whence == io.SEEK_END:
            new = self.size() + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        if new < 0:
            raise ValueError("negative seek position")
        # Seeking past EOF is legal; reads there simply return b"".
        self._pos = new
        return self._pos

    def size(self) -> int:
        if self._size is None:
            self._fetch_tail()
        assert self._size is not None
        return self._size

    def _fetch_tail(self) -> None:
        """Learn the object size from a suffix-range GET and cache the footer bytes."""
        from .s3_consistency import with_s3_retry

        def op() -> Tuple[int, bytes]:
            try:
                resp = self._s3.get_object(
                    Bucket=self._bucket, Key=self._key, Range=f"bytes=-{self.TAIL_BYTES}"
                )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in ("NoSuchKey", "404", "NotFound"):
                    raise FileNotFoundError(f"S3 object not found: s3://{self._bucket}/{self._key}") from e
                if code == "InvalidRange":  # empty object: no bytes to suffix-range
                    head = self._s3.head_object(Bucket=self._bucket, Key=self._key)
                    return int(head["ContentLength"]), b""
                raise
            body = resp["Body"]
            try:
                data = bytes(body.read())
            finally:
                body.close()
            content_range = resp.get("ContentRange") or ""
            if "/" in content_range and content_range.rsplit("/", 1)[1].isdigit():
                total = int(content_range.rsplit("/", 1)[1])
            else:  # provider returned the whole object without a Content-Range
                total = int(resp.get("ContentLength", len(data)))
            return total, data

        total, data = with_s3_retry(op, f"S3 tail read: {self._key}")
        self._size = total
        self._tail = data
        self._tail_start = total - len(data)

    # --- reading ----------------------------------------------------------
    def _read_span(self, first: int, last: int) -> bytes:
        """Bytes [first, last] inclusive, served from the cached tail when possible."""
        if self._tail and first >= self._tail_start:
            off = first - self._tail_start
            return self._tail[off : off + (last - first + 1)]
        return self._get_range(first, last)

    def readinto(self, b: Any) -> int:
        size = self.size()
        want = len(b)
        if want == 0 or self._pos >= size:
            return 0
        last = min(self._pos + want, size) - 1
        data = self._read_span(self._pos, last)
        n = len(data)
        b[:n] = data
        self._pos += n
        return n

    def readall(self) -> bytes:
        size = self.size()
        if self._pos >= size:
            return b""
        data = self._read_span(self._pos, size - 1)
        self._pos += len(data)
        return data

    def _get_range(self, first: int, last: int) -> bytes:
        from .s3_consistency import with_s3_retry

        def op() -> bytes:
            resp = self._s3.get_object(
                Bucket=self._bucket,
                Key=self._key,
                Range=f"bytes={first}-{last}",
            )
            body = resp["Body"]
            try:
                return bytes(body.read())
            finally:
                body.close()

        return with_s3_retry(op, f"S3 range read: {self._key} [{first}-{last}]")


