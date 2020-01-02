"""Generic Cell Rate Algorithm

This code is based on an excellent article by Philip Jones, which can be found
at https://smarketshq.com/implementing-gcra-in-python-5df1f11aaa96.

"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict
import logging


logger = logging.getLogger(__file__)
logger.addHandler(logging.NullHandler())


@dataclass
class RateLimit:
    """Describes a rate at which to limit requests.

    For example, to limit the rate to 10 requests per 60 seconds:

        rate = RateLimit(limit=10, period=timedelta(seconds=60))

    """
    limit: int
    period: timedelta

    @property
    def inverse(self) -> float:
        return self.period.total_seconds() / self.limit

    def __str__(self) -> str:
        return f"{self.limit}/{self.period}"


@dataclass
class RateLimiter:
    """Basic implementation of the generic cell rate algorithm.

    Although the algorithm itself is correct, in a production environment, the
    *cache* property should be a shared resource, such as Redis, unless the
    rate limiting is applied to an entire application (e.g. middleware).

    """

    cache: Dict[str, datetime] = field(default_factory=dict)

    def get_tat(self, key: str, now: datetime) -> datetime:
        """Get the arrival time for *key*.

        If *key* does not exist in the cache, *now* will be stored as its value
        and subsequently returned.

        """
        try:
            tat = self.cache[key]
        except KeyError:
            tat = self.cache[key] = now

        return tat

    def set_tat(self, key: str, tat: datetime) -> None:
        """Set the arrival time for *key* to *tat*."""
        self.cache[key] = tat

    def is_rejected(self, key: str, limit: RateLimit) -> bool:
        """Test if *key* has exceeded the *limit*."""
        # If key is missing, 'now' is provided as the initial TAT, rather than
        # calling datetime.now() again in get_tat(), otherwise the second call
        # to now() is 2 or 3 microseconds later which causes the first request
        # for key to be rejected for rate limits such as 1/P.
        now = datetime.now()
        tat = max(self.get_tat(key, now), now)
        separation = (tat - now).total_seconds()
        interval = limit.period.total_seconds() - limit.inverse

        logger.debug(
            "%s: now=%s, tat=%s, separation=%.6f, interval=%.6f",
            key, now, tat, separation, interval
        )

        if separation > interval:
            reject = True
        else:
            tat = max(tat, now) + timedelta(seconds=limit.inverse)
            self.set_tat(key, tat)
            reject = False

        return reject
