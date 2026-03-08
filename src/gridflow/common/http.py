from __future__ import annotations

import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class HttpRateLimitConfig:
    min_interval_seconds: float
    max_retries: int = 5
    backoff_base_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    jitter_seconds: float = 0.25


ELEXON_HTTP_CONFIG = HttpRateLimitConfig(
    min_interval_seconds=0.2,
    max_retries=5,
    backoff_base_seconds=1.0,
    backoff_multiplier=2.0,
    max_backoff_seconds=30.0,
    jitter_seconds=0.2,
)

ENTSOE_HTTP_CONFIG = HttpRateLimitConfig(
    min_interval_seconds=1.0,
    max_retries=6,
    backoff_base_seconds=2.0,
    backoff_multiplier=2.0,
    max_backoff_seconds=60.0,
    jitter_seconds=0.5,
)


_LAST_REQUEST_AT: dict[str, float] = {}


def _sleep_for_rate_limit(source_name: str, config: HttpRateLimitConfig) -> None:
    now = time.monotonic()
    last = _LAST_REQUEST_AT.get(source_name)

    if last is None:
        return

    elapsed = now - last
    remaining = config.min_interval_seconds - elapsed
    if remaining > 0:
        time.sleep(remaining)


def _mark_request(source_name: str) -> None:
    _LAST_REQUEST_AT[source_name] = time.monotonic()


def _retry_after_seconds(response: requests.Response) -> float | None:
    retry_after = response.headers.get("Retry-After")
    if retry_after is None:
        return None

    retry_after = retry_after.strip()
    try:
        return max(0.0, float(retry_after))
    except ValueError:
        return None


def _backoff_seconds(attempt: int, config: HttpRateLimitConfig) -> float:
    raw = config.backoff_base_seconds * (config.backoff_multiplier ** max(0, attempt - 1))
    capped = min(raw, config.max_backoff_seconds)
    jitter = random.uniform(0.0, config.jitter_seconds)
    return capped + jitter


def get_with_retries(
    *,
    source_name: str,
    url: str,
    config: HttpRateLimitConfig,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: int | float | None = None,
    session: requests.Session | None = None,
) -> requests.Response:
    """
    Perform a GET request with:
    - per-source rate limiting
    - retries for 429 and 5xx responses
    - exponential backoff
    """
    client = session or requests

    last_error: Exception | None = None

    for attempt in range(1, config.max_retries + 1):
        _sleep_for_rate_limit(source_name, config)

        try:
            response = client.get(
                url,
                params=dict(params or {}),
                headers=dict(headers or {}),
                timeout=timeout,
            )
            _mark_request(source_name)

            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == config.max_retries:
                    response.raise_for_status()

                sleep_seconds = _retry_after_seconds(response)
                if sleep_seconds is None:
                    sleep_seconds = _backoff_seconds(attempt, config)

                print(
                    f"[HTTP RETRY] source={source_name} "
                    f"status={response.status_code} "
                    f"attempt={attempt}/{config.max_retries} "
                    f"sleep={sleep_seconds:.2f}s"
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            last_error = exc

            if attempt == config.max_retries:
                raise

            sleep_seconds = _backoff_seconds(attempt, config)
            print(
                f"[HTTP RETRY] source={source_name} "
                f"error={exc.__class__.__name__} "
                f"attempt={attempt}/{config.max_retries} "
                f"sleep={sleep_seconds:.2f}s"
            )
            time.sleep(sleep_seconds)

    if last_error is not None:
        raise last_error

    raise RuntimeError("Unexpected HTTP retry state")