import logging
import unittest

from src.gtfs_rt_aggregator.utils.redact import RedactingFilter, redact


class TestRedact(unittest.TestCase):
    def test_redact(self):
        self.assertEqual(
            redact("404 for url: https://x.org/feed?key=SECRET and 'headers': {'k': 'SECRET'}"),
            "404 for url: https://x.org/feed?*** and 'headers': {***}",
        )

    def test_filter_on_messages_and_tracebacks(self):
        record = logging.LogRecord("x", logging.ERROR, "f", 1, "GET %s", ("https://x.org/a?t=SECRET",), None)
        try:
            raise ValueError("https://x.org/b?t=SECRET")
        except ValueError:
            import sys

            record.exc_info = sys.exc_info()
        RedactingFilter().filter(record)
        text = logging.Formatter().format(record)
        self.assertNotIn("SECRET", text)
        self.assertIn("https://x.org/a?***", text)


if __name__ == "__main__":
    unittest.main()
