"""Package version resolution (see comments)."""

# Single source of truth for the version is pyproject.toml. Installed wheels report
# it through package metadata; a development checkout reads pyproject.toml directly,
# because an editable install's dist-info can lag behind (it reported 0.5.1 at 0.7.2).
_FALLBACK_VERSION = "0.10.2"


def _resolve_version() -> str:
    import os
    import re

    pyproject = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "pyproject.toml",
    )
    try:
        with open(pyproject, encoding="utf-8") as fh:
            text = fh.read()
        if re.search(r'^name = "datashard"$', text, re.M):
            match = re.search(r'^version = "([^"]+)"$', text, re.M)
            if match:
                return match.group(1)
    except OSError:
        pass
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version("datashard")
        except PackageNotFoundError:
            return _FALLBACK_VERSION
    except ImportError:  # pragma: no cover
        return _FALLBACK_VERSION


__version__ = _resolve_version()
