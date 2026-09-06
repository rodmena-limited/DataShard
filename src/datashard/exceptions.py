"""
datashard exception types (split out of metadata_manager.py, #71).
"""


class ConcurrentModificationException(Exception):
    """Exception thrown when concurrent modifications are detected"""

    pass


class TableExistsError(Exception):
    """Raised when initializing a table over an already-initialized table."""

    pass


class SchemaMismatchError(TableExistsError):
    """Raised by create_table() when the table already exists with a different
    persisted schema than the one requested (#64)."""

    pass


class AmbiguousMetadataError(Exception):
    """Raised when the version hint is missing and several metadata files share the
    highest version, so the committed state cannot be told apart from a failed
    committer's leftover (#60). Resolve with MetadataManager.repair_version_hint().
    """

    pass


class AmbiguousCommitError(Exception):
    """Raised when the commit-point write failed in a way that may still have
    become visible (e.g. an S3 PUT that errored client-side after possibly
    succeeding server-side).

    Callers MUST NOT delete files written for this transaction: the commit may
    be durable and referencing them. Orphan cleanup is the garbage collector's
    job once the true outcome is observable.
    """

    pass


