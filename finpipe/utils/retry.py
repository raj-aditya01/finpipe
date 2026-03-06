# ==============================================================================
# utils/retry.py — Retry with Exponential Backoff
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Exponential backoff — wait 1s, 2s, 4s, 8s... (not hammering a dead API)
#   2. Jitter — add randomness so 100 clients don't all retry at the same moment
#   3. Decorators — wrap any function with retry logic without changing it
#   4. Transient vs permanent errors — only retry what CAN succeed next time
#
# WHY EXPONENTIAL BACKOFF?
#   Scenario: Yahoo Finance API returns 429 (rate limited).
#
#   BAD (constant retry):
#     retry 1: wait 1s → still rate limited
#     retry 2: wait 1s → still rate limited (you're making it WORSE)
#     retry 3: wait 1s → still rate limited
#
#   GOOD (exponential backoff):
#     retry 1: wait 1s   → gives API time to recover
#     retry 2: wait 2s   → more breathing room
#     retry 3: wait 4s   → API has had 7s total to recover
#     retry 4: wait 8s   → almost certainly back up
#
# WHY JITTER?
#   Imagine 50 pipeline workers all fail at the same time. Without jitter:
#     ALL 50 retry after exactly 1s → "thundering herd" → API crashes again
#   With jitter:
#     Worker 1 retries after 0.8s, Worker 2 after 1.3s, Worker 3 after 0.5s
#     → requests spread out → API survives
#
# PRODUCTION EXAMPLES:
#   - AWS SDK uses exponential backoff by default
#   - Airflow task retries use exponential_backoff=True
#   - gRPC has built-in backoff for connection retries
#   - Kafka producers use retries with backoff.ms
#
# ==============================================================================

import functools
import logging
import random
import time
from typing import Callable, TypeVar, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Decorator: retry a function with exponential backoff on failure.

    WHAT IS A DECORATOR?
      A function that wraps another function to add behavior.

      @retry_with_backoff(max_retries=3)
      def fetch_data():
          ...

      This is equivalent to:
        fetch_data = retry_with_backoff(max_retries=3)(fetch_data)

      Now when you call fetch_data(), the retry logic runs automatically.

    Args:
        max_retries:          How many times to retry (0 = no retries, just run once)
        base_delay:           First retry waits this many seconds
        max_delay:            Never wait longer than this (cap the exponential growth)
        exponential_base:     Multiply delay by this each retry (2.0 = double each time)
        jitter:               Add randomness to prevent thundering herd
        retryable_exceptions: Only retry these exception types. Others fail immediately.
                              Default (Exception,) retries everything — narrow this in production.

    BACKOFF FORMULA:
        delay = min(base_delay * (exponential_base ** attempt), max_delay)
        if jitter: delay = random.uniform(0, delay)

        Example with defaults (base=1, exp=2, max=60):
          Attempt 0: 1 * 2^0 = 1s  (with jitter: 0-1s)
          Attempt 1: 1 * 2^1 = 2s  (with jitter: 0-2s)
          Attempt 2: 1 * 2^2 = 4s  (with jitter: 0-4s)
          Attempt 3: 1 * 2^3 = 8s  (with jitter: 0-8s)

    Usage:
        @retry_with_backoff(max_retries=3, retryable_exceptions=(ConnectionError, TimeoutError))
        def call_api():
            response = requests.get("https://api.example.com/data")
            response.raise_for_status()
            return response.json()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:

        @functools.wraps(func)  # Preserve original function's name and docstring
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):  # +1 because first call is attempt 0
                try:
                    return func(*args, **kwargs)

                except retryable_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        # Exhausted all retries — give up
                        logger.error(
                            f"[retry] {func.__name__} failed after {max_retries + 1} attempts. "
                            f"Last error: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** attempt),
                        max_delay,
                    )

                    # Add jitter: random value between 0 and the calculated delay
                    if jitter:
                        delay = random.uniform(0, delay)

                    logger.warning(
                        f"[retry] {func.__name__} attempt {attempt + 1}/{max_retries + 1} "
                        f"failed: {e}. Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)

                except Exception as e:
                    # Non-retryable exception — fail immediately
                    logger.error(
                        f"[retry] {func.__name__} failed with non-retryable error: {e}"
                    )
                    raise

            # Should never reach here, but satisfy type checker
            raise last_exception  # type: ignore

        return wrapper

    return decorator
