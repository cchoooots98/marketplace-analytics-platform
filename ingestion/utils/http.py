"""HTTP session helpers for resilient ingestion requests."""

from __future__ import annotations

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_RETRY_TOTAL = 5
DEFAULT_RETRY_BACKOFF_FACTOR = 1.0
DEFAULT_RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)


def build_retry_session(
    *,
    total_retries: int = DEFAULT_RETRY_TOTAL,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    status_forcelist: tuple[int, ...] = DEFAULT_RETRY_STATUS_FORCELIST,
) -> Session:
    """Build a requests session with enterprise-style retry behavior.

    Args:
        total_retries: Maximum retry attempts across transient failures.
        backoff_factor: Exponential backoff multiplier between retries.
        status_forcelist: HTTP status codes treated as transient upstream errors.

    Returns:
        A requests session with retry-enabled adapters mounted for HTTP and HTTPS.
    """
    retry_strategy = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
