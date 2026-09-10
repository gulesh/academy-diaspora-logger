"""Client construction with recovery from a stale/revoked Globus token."""

from __future__ import annotations

import contextlib
from typing import Any


class DiasporaAuthError(RuntimeError):
    """Cached Globus credentials are stale and must be refreshed."""


def get_client(environment: str | None = None, *, interactive: bool = False) -> Any:
    """Construct a ``diaspora_event_sdk.Client``, recovering from a stale token.

    ``interactive=True`` retries once after clearing the token (only safe for a
    human-run CLI command); otherwise raises ``DiasporaAuthError``.
    """
    from diaspora_event_sdk import Client

    def _construct() -> Any:
        return Client(environment) if environment else Client()

    try:
        return _construct()
    except Exception as exc:  # noqa: BLE001
        if not _is_invalid_grant(exc):
            raise
        _clear_stale_token(environment)
        if interactive:
            return _construct()
        raise DiasporaAuthError(
            "Cached Globus credentials have expired or been revoked (invalid_grant). "
            "Run `diaspora setup` to re-authenticate, then try again.",
        ) from exc


def _is_invalid_grant(exc: BaseException) -> bool:
    try:
        from globus_sdk.exc.api import GlobusAPIError
    except ImportError:
        GlobusAPIError = ()  # noqa: N806
    if GlobusAPIError and not isinstance(exc, GlobusAPIError):
        return False

    raw_json = getattr(exc, "raw_json", None)
    if isinstance(raw_json, dict) and raw_json.get("error") == "invalid_grant":
        return True
    text = getattr(exc, "text", None)
    if isinstance(text, str) and "invalid_grant" in text:
        return True
    if "invalid_grant" in str(exc):
        return True
    return any("invalid_grant" in note for note in getattr(exc, "__notes__", None) or ())


def _clear_stale_token(environment: str | None) -> None:
    from diaspora_event_sdk.sdk.auth.globus_app import get_globus_app

    with contextlib.suppress(Exception):
        # sweep=True: a freshly constructed GlobusApp has no scope
        # requirements registered on it yet, so the default (sweep=False,
        # which only clears scopes it knows about) would silently no-op.
        get_globus_app(environment=environment).logout(sweep=True)


__all__ = ["DiasporaAuthError", "get_client"]
