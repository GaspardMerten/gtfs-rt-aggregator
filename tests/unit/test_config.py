import unittest
from io import BytesIO

from src.gtfs_rt_aggregator.config.loader import load_config_from_toml_file
from src.gtfs_rt_aggregator.config.models import ApiConfig, ProviderConfig


def _load(toml: str):
    return load_config_from_toml_file(BytesIO(toml.encode("utf-8")))


STORAGE = """
[storage]
type = "filesystem"
[storage.params]
base_directory = "data"
"""


class TestConfig(unittest.TestCase):
    def test_realtime_and_static(self):
        config = _load(STORAGE + """
[[providers]]
name = "nl"
timezone = "Europe/Amsterdam"

  [[providers.realtime]]
  url = "https://example.org/vp.pb"
  services = ["VehiclePosition"]
  [providers.realtime.headers]
  x-api-key = "secret"

  [[providers.static]]
  url = "https://example.org/gtfs.zip"
  check_minutes = 1440
  [providers.static.headers]
  x-api-key = "secret"
""")
        (provider,) = config.providers
        self.assertEqual(provider.realtime[0].headers, {"x-api-key": "secret"})
        self.assertEqual(provider.static[0].name, "static")
        self.assertEqual(provider.static[0].check_minutes, 1440)
        self.assertEqual(provider.static[0].headers, {"x-api-key": "secret"})

    def test_apis_still_accepted(self):
        config = _load(STORAGE + """
[[providers]]
name = "nl"
  [[providers.apis]]
  url = "https://example.org/vp.pb"
  services = ["VehiclePosition"]
""")
        provider = config.providers[0]
        self.assertEqual(len(provider.realtime), 1)
        self.assertIs(provider.apis, provider.realtime)

        # Also from Python
        provider = ProviderConfig(
            name="nl",
            apis=[ApiConfig(url="https://example.org/vp.pb", services=["Alert"])],
        )
        self.assertEqual(len(provider.realtime), 1)

    def test_apis_deprecation_warning(self):
        with self.assertLogs(level="WARNING") as logs:
            _load(STORAGE + """
[[providers]]
name = "nl"
  [[providers.apis]]
  url = "https://example.org/vp.pb"
  services = ["VehiclePosition"]
""")
        self.assertTrue(any("deprecated" in line for line in logs.output))

    def test_static_typo_rejected(self):
        with self.assertRaises(ValueError):
            _load(STORAGE + """
[[providers]]
name = "nl"
  [[providers.static]]
  url = "https://example.org/gtfs.zip"
  check_minute = 5
""")

    def test_realtime_and_apis_together_rejected(self):
        with self.assertRaises(ValueError):
            _load(STORAGE + """
[[providers]]
name = "nl"
  [[providers.apis]]
  url = "https://example.org/a.pb"
  services = ["Alert"]
  [[providers.realtime]]
  url = "https://example.org/b.pb"
  services = ["Alert"]
""")

    def test_static_only_provider(self):
        config = _load(STORAGE + """
[[providers]]
name = "at"
timezone = "Europe/Vienna"
  [[providers.static]]
  url = "https://example.org/gtfs.zip"
""")
        self.assertEqual(config.providers[0].realtime, [])
        self.assertEqual(len(config.providers[0].static), 1)

    def test_provider_without_feeds_rejected(self):
        with self.assertRaises(ValueError):
            _load(STORAGE + '\n[[providers]]\nname = "nl"\n')

    def test_duplicate_static_names_rejected(self):
        with self.assertRaises(ValueError):
            _load(STORAGE + """
[[providers]]
name = "nl"
  [[providers.static]]
  url = "https://example.org/a.zip"
  [[providers.static]]
  url = "https://example.org/b.zip"
""")


if __name__ == "__main__":
    unittest.main()
