from datetime import timedelta
from time import sleep
import logging
import unittest

logging.basicConfig(level="DEBUG", format="%(message)s")

import gcra


class TestCases(unittest.TestCase):
    def setUp(self):
        self.key = "127.0.0.1"
        self.limiter = gcra.RateLimiter()

    def tearDown(self):
        pass

    def is_rejected(self, rate):
        return self.limiter.is_rejected(self.key, rate)

    def assert_rejected(self, rate):
        self.assertTrue(self.is_rejected(rate))

    def assert_not_rejected(self, rate):
        self.assertFalse(self.is_rejected(rate))

    def test_10_per_60s_one_burst(self):
        """Test 10 requests per 60 seconds in one burst."""
        rate = gcra.RateLimit(10, period=timedelta(seconds=60))

        for _ in range(10):
            self.assert_not_rejected(rate)

    def test_10_per_60s_two_bursts(self):
        """Test 10 requests per 60 seconds in two bursts many seconds apart."""
        rate = gcra.RateLimit(10, period=timedelta(seconds=60))

        for _ in range(5):
            self.assert_not_rejected(rate)

        sleep(58);

        for _ in range(10):
            self.assert_not_rejected(rate)

        self.assert_rejected(rate)

    def test_1_per_6s(self):
        """Test 1 request per 6 seconds immediate followed by another request."""
        rate = gcra.RateLimit(1, period=timedelta(seconds=6))

        self.assert_not_rejected(rate)
        self.assert_rejected(rate)

