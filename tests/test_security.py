import os
import tempfile

import pytest

from datashard.storage_backend import LocalStorageBackend


def test_path_traversal_prevention():
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = os.path.abspath(temp_dir)
        backend = LocalStorageBackend(base_dir)

        # Attack 1: Relative path traversal
        attack_path = "../../../../etc/passwd"

        with pytest.raises(ValueError, match="Security Error: Path traversal attempt detected"):
            backend._resolve_path(attack_path)

        # Attack 2: Absolute path behavior
        # The system is designed to treat absolute paths starting with / as relative to the table root
        # So /etc/passwd should be resolved to {base_dir}/etc/passwd, effectively sandboxing it.
        if os.name == 'posix':
            attack_path_abs = "/etc/passwd"
            resolved = backend._resolve_path(attack_path_abs)
            # It should NOT point to the real /etc/passwd
            assert resolved != "/etc/passwd"
            # It SHOULD be within base_dir
            assert resolved.startswith(base_dir)
            assert resolved == os.path.join(base_dir, "etc/passwd")

        # Legitimate path should pass
        valid_path = "data/file.parquet"
        resolved = backend._resolve_path(valid_path)
        assert resolved.startswith(base_dir)
        assert resolved == os.path.join(base_dir, "data/file.parquet")
