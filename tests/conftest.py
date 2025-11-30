import pytest
import os

@pytest.fixture(autouse=True)
def force_local_storage_by_default(monkeypatch):
    """
    Ensure that tests default to using local storage, even if the 
    external environment is configured for S3.
    
    Tests that require S3 should explicitly set DATASHARD_STORAGE_TYPE='s3'.
    """
    monkeypatch.setenv("DATASHARD_STORAGE_TYPE", "local")
