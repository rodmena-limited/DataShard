"""Every partition transform, compared against pyiceberg's OWN implementation (#98).

A partition value datashard writes is what DuckDB, pyiceberg, Spark and Trino PRUNE on, so a
transform that disagrees with Iceberg's by one bucket makes those engines skip rows that match.
Checking against this repository's reading of the spec would only prove it is self-consistent;
checking against the reference implementation caught a real defect on the first run (decimal
truncate used the column's declared scale where Iceberg uses the value's own).
"""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from datashard.partitioning import murmur3_32, transform_function

pyiceberg_transforms = pytest.importorskip("pyiceberg.transforms")
pyiceberg_types = pytest.importorskip("pyiceberg.types")


# ---------------------------------------------------------------- against pyiceberg
@pytest.mark.parametrize("n_buckets", [2, 16, 128, 1000])
@pytest.mark.parametrize("itype,ptype,values", [
    ("long", "LongType", [0, 1, -1, 34, 2**40, -(2**40)]),
    ("int", "IntegerType", [0, 1, -1, 7, 2**30]),
    ("string", "StringType", ["", "a", "iceberg", "ZEN-USDT", "unicode ↯ test"]),
    ("date", "DateType", [date(1970, 1, 1), date(2026, 9, 9), date(1900, 3, 4)]),
    ("timestamptz", "TimestamptzType", [datetime(2026, 9, 9, 12, 34, 56, 789012, tzinfo=timezone.utc)]),
    ("timestamp", "TimestampType", [datetime(2026, 9, 9, 12, 34, 56, 789012), datetime(1969, 1, 1)]),
    ("binary", "BinaryType", [b"", b"abc", bytes(range(16))]),
])
def test_bucket_matches_pyiceberg(n_buckets, itype, ptype, values):
    ours = transform_function(f"bucket[{n_buckets}]", itype)
    theirs = pyiceberg_transforms.BucketTransform(num_buckets=n_buckets).transform(
        getattr(pyiceberg_types, ptype)())
    for v in values:
        assert ours(v) == theirs(v), (itype, v)


def test_bucket_on_decimals_matches_pyiceberg():
    ours = transform_function("bucket[16]", "decimal(18,8)")
    theirs = pyiceberg_transforms.BucketTransform(num_buckets=16).transform(
        pyiceberg_types.DecimalType(18, 8))
    for v in (Decimal("1.5"), Decimal("-2.25"), Decimal("0"), Decimal("12345.6789")):
        assert ours(v) == theirs(v), v


@pytest.mark.parametrize("name,cls,itype,ptype,value", [
    ("year", "YearTransform", "date", "DateType", date(2026, 9, 9)),
    ("year", "YearTransform", "date", "DateType", date(1969, 12, 31)),
    ("month", "MonthTransform", "date", "DateType", date(1969, 11, 30)),
    ("month", "MonthTransform", "timestamp", "TimestampType", datetime(2026, 3, 15, 4, 5)),
    ("day", "DayTransform", "date", "DateType", date(1968, 2, 29)),
    ("day", "DayTransform", "timestamptz", "TimestamptzType", datetime(2026, 9, 9, 23, 59, 59, tzinfo=timezone.utc)),
    ("hour", "HourTransform", "timestamptz", "TimestamptzType", datetime(2026, 9, 9, 23, 0, tzinfo=timezone.utc)),
    ("hour", "HourTransform", "timestamp", "TimestampType", datetime(1969, 12, 31, 23, 0)),
])
def test_temporal_transforms_match_pyiceberg(name, cls, itype, ptype, value):
    ours = transform_function(name, itype)(value)
    theirs = getattr(pyiceberg_transforms, cls)().transform(getattr(pyiceberg_types, ptype)())(value)
    assert ours == theirs


@pytest.mark.parametrize("width", [2, 3, 10, 100])
@pytest.mark.parametrize("itype,ptype,values", [
    ("long", "LongType", [0, 1, -1, 17, -17, 12345, -99999]),
    ("string", "StringType", ["", "a", "abcdef", "ZEN-USDT", "↯unicode↯"]),
    ("binary", "BinaryType", [b"", b"abc", bytes(range(20))]),
])
def test_truncate_matches_pyiceberg(width, itype, ptype, values):
    ours = transform_function(f"truncate[{width}]", itype)
    theirs = pyiceberg_transforms.TruncateTransform(width=width).transform(getattr(pyiceberg_types, ptype)())
    for v in values:
        assert ours(v) == theirs(v), (itype, v)


@pytest.mark.parametrize("width", [2, 3, 10, 100])
def test_truncate_on_decimals_uses_the_values_own_scale(width):
    """The defect this comparison caught: truncating at the column's declared scale left
    the value unchanged whenever the trailing zeros made it divisible by W."""
    ours = transform_function(f"truncate[{width}]", "decimal(18,8)")
    theirs = pyiceberg_transforms.TruncateTransform(width=width).transform(pyiceberg_types.DecimalType(18, 8))
    for v in (Decimal("1.5"), Decimal("-2.25"), Decimal("0"), Decimal("1.50000000"), Decimal("12345.6789")):
        assert ours(v) == theirs(v), v


def test_murmur3_matches_the_published_vectors():
    """A known-positive for the hash itself, so a bucket comparison cannot pass vacuously."""
    assert murmur3_32(b"") == 0
    assert murmur3_32(b"a") == 1009084850
    assert murmur3_32(b"hello") == 613153351
