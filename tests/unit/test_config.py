import unittest
from io import BytesIO

from src.gtfs_rt_aggregator.config.loader import load_config_from_toml_file
from pydantic import ValidationError

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    StorageConfig,
)


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


class TestValidators(unittest.TestCase):
    URL = "https://example.org/vp.pb"

    def test_invalid_values_rejected(self):
        cases = {
            "service": lambda: ApiConfig(url=self.URL, services=["Bus"]),
            "no service": lambda: ApiConfig(url=self.URL, services=[]),
            "refresh": lambda: ApiConfig(
                url=self.URL, services=["Alert"], refresh_seconds=0
            ),
            "frequency": lambda: ApiConfig(
                url=self.URL, services=["Alert"], frequency_minutes=-5
            ),
            "timezone": lambda: ProviderConfig(
                name="nl",
                timezone="Europe/Amsterdm",
                realtime=[ApiConfig(url=self.URL, services=["Alert"])],
            ),
            "provider frequency": lambda: ProviderConfig(
                name="nl",
                frequency_minutes=0,
                realtime=[ApiConfig(url=self.URL, services=["Alert"])],
            ),
            "gcs bucket": lambda: StorageConfig(type="gcs", params={}),
            "minio params": lambda: StorageConfig(
                type="minio", params={"endpoint": "e"}
            ),
            "duplicate providers": lambda: GtfsRtConfig(
                storage=StorageConfig(type="filesystem"),
                providers=[
                    ProviderConfig(
                        name="nl",
                        realtime=[ApiConfig(url=self.URL, services=["Alert"])],
                    )
                ]
                * 2,
            ),
        }
        for name, build in cases.items():
            with self.subTest(name), self.assertRaises(ValidationError):
                build()

    def test_valid_values_kept(self):
        provider = ProviderConfig(
            name="nl",
            timezone="Europe/Amsterdam",
            realtime=[ApiConfig(url=self.URL, services=["TripModifications"])],
        )
        self.assertEqual(provider.timezone, "Europe/Amsterdam")
        self.assertEqual(StorageConfig(type="FileSystem").type, "filesystem")


class TestRawConfig(unittest.TestCase):
    def test_retention_and_exclude(self):
        config = _load(
            STORAGE
            + """
[raw]
enabled = true
retention_days = 3
exclude = ["de-gtfsde", "TripUpdate-0123abcd"]

[[providers]]
name = "de-gtfsde"
[[providers.realtime]]
url = "https://example.org/de.pb"
services = ["TripUpdate"]
"""
        )
        raw = config.raw
        self.assertEqual(raw.retention_days, 3)
        self.assertFalse(raw.archives("de-gtfsde", "TripUpdate-ffffffff"))
        self.assertFalse(raw.archives("nl", "TripUpdate-0123abcd"))
        self.assertTrue(raw.archives("nl", "TripUpdate-ffffffff"))
        raw.enabled = False
        self.assertFalse(raw.archives("nl", "TripUpdate-ffffffff"))

    def test_defaults_keep_everything(self):
        raw = _load(
            STORAGE
            + """
[raw]
enabled = true

[[providers]]
name = "nl"
[[providers.realtime]]
url = "https://example.org/nl.pb"
services = ["TripUpdate"]
"""
        ).raw
        self.assertEqual((raw.retention_days, raw.exclude), (0, []))
        self.assertTrue(raw.archives("nl", "TripUpdate-ffffffff"))


class TestProviderChecks(unittest.TestCase):
    def test_reserved_or_unsafe_names(self):
        for name in ("global", "__global__", "a/b", "..", ""):
            with self.assertRaises(ValidationError, msg=name):
                ProviderConfig(
                    name=name, realtime=[ApiConfig(url="u", services=["Alert"])]
                )

    def test_same_feed_twice(self):
        with self.assertRaisesRegex(ValidationError, "twice"):
            ProviderConfig(
                name="p",
                realtime=[
                    ApiConfig(url="u", services=["Alert"]),
                    ApiConfig(url="u", services=["TripUpdate"]),
                ],
            )

    def test_feeds_of_a_service_share_its_settings(self):
        with self.assertRaisesRegex(ValidationError, "frequency_minutes"):
            ProviderConfig(
                name="p",
                realtime=[
                    ApiConfig(url="a", services=["Alert"]),
                    ApiConfig(url="b", services=["Alert"], frequency_minutes=15),
                ],
            )

    def test_provider_frequency_checked_with_accumulate_minutes(self):
        with self.assertRaisesRegex(ValidationError, "accumulate_minutes"):
            ProviderConfig(
                name="p",
                frequency_minutes=45,
                realtime=[
                    ApiConfig(url="a", services=["Alert"], accumulate_minutes=30)
                ],
            )

    def test_provider_defaults(self):
        provider = ProviderConfig(
            name="p",
            frequency_minutes=15,
            realtime=[
                ApiConfig(url="a", services=["Alert"]),
                ApiConfig(url="b", services=["TripUpdate"], frequency_minutes=30),
            ],
        )
        self.assertEqual([a.frequency_minutes for a in provider.realtime], [15, 30])

    def test_secrets_not_in_errors(self):
        with self.assertRaises(ValidationError) as error:
            ApiConfig(url="https://x.org/?key=s3cret", services=["Nope"])
        self.assertNotIn("s3cret", str(error.exception))

    def test_accumulate_concatenate_ignored(self):
        with self.assertLogs("src.gtfs_rt_aggregator.config.models", "WARNING"):
            api = ApiConfig(url="u", services=["Alert"], accumulate_concatenate=False)
        self.assertFalse(hasattr(api, "accumulate_concatenate"))

    def test_unknown_storage_type(self):
        with self.assertRaises(ValidationError):
            StorageConfig(type="ftp")


if __name__ == "__main__":
    unittest.main()
