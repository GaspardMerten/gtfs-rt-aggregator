import os
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from unittest.mock import patch

from src.gtfs_rt_aggregator.aggregator.compaction import (
    compact_files,
    sorted_by,
    write_sorted,
)
from src.gtfs_rt_aggregator.aggregator.dedup import deduplicate

TS = pa.timestamp("us", tz="UTC")


def _hour(start: datetime, entities: int, fetches: int, rng: random.Random) -> pa.Table:
    """Fetches every 30 s from start; entity content changes now and then."""
    rows = {"entityId": [], "contentHash": [], "fetchTime": [], "value": []}
    for f in range(fetches):
        time = start + timedelta(seconds=60 * f)
        for e in range(entities):
            if rng.random() < 0.1:
                continue  # missing from this fetch
            state = (e * 7 + f // rng.choice([1, 5, 50])) % 4
            rows["entityId"].append(f"e{e:03}")
            rows["contentHash"].append(f"h{state}")
            rows["fetchTime"].append(time)
            rows["value"].append(state)
    table = pa.table(rows)
    return table.set_column(2, "fetchTime", pa.array(rows["fetchTime"], TS))


class TestStreamingCompaction(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        rng = random.Random(1)
        # Hours around the night clocks go back, in UTC
        start = datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc)
        self.hours = [_hour(start + timedelta(hours=h), 30, 60, rng) for h in range(4)]
        self.paths = []
        for index, table in enumerate(self.hours):
            # Hourly files are written in fetch order, not sorted
            path = os.path.join(self.tmp, f"{index}.parquet")
            pq.write_table(table, path)
            self.paths.append(path)

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmp)

    def _run(self, keys, dedup, batch_rows, fan_in=8):
        output = os.path.join(self.tmp, "out.parquet")
        times = None
        if dedup:
            times = pc.unique(
                pa.concat_tables(self.hours)["fetchTime"].combine_chunks()
            )
        inputs = self.paths
        if dedup:
            # Hourly files are deduplicated by the aggregator before compaction
            inputs = []
            for index, table in enumerate(self.hours):
                path = os.path.join(self.tmp, f"d{index}.parquet")
                pq.write_table(deduplicate(table), path)
                inputs.append(path)
        prepared = []
        for index, path in enumerate(inputs):
            sorted_path = os.path.join(self.tmp, f"s{index}.parquet")
            write_sorted(
                pq.read_table(path), keys, sorted_path, row_group_rows=batch_rows
            )
            prepared.append(sorted_path)
        module = "src.gtfs_rt_aggregator.aggregator.compaction"
        with (
            patch(f"{module}.ROW_BUDGET", batch_rows),
            patch(f"{module}.MIN_BATCH_ROWS", 1),
            patch(f"{module}.MAX_FAN_IN", fan_in),
        ):
            rows = compact_files(prepared, keys, output, dedup, times)
        result = pq.read_table(output)
        self.assertEqual(rows, result.num_rows)
        return result

    def test_same_as_in_memory_sort(self):
        keys = ["entityId", "fetchTime"]
        expected = pa.concat_tables(self.hours)
        expected = expected.take(
            pc.sort_indices(expected, [(k, "ascending") for k in keys])
        )
        for batch_rows in (37, 100_000):
            with self.subTest(batch_rows=batch_rows):
                result = self._run(keys, False, batch_rows)
                self.assertEqual(
                    result.select(expected.column_names).to_pylist(),
                    expected.to_pylist(),
                )

    def test_merge_in_rounds(self):
        keys = ["entityId", "fetchTime"]
        expected = pa.concat_tables(self.hours)
        expected = expected.take(
            pc.sort_indices(expected, [(k, "ascending") for k in keys])
        )
        # 4 inputs, 2 at a time: two rounds
        result = self._run(keys, False, 50, fan_in=2)
        self.assertEqual(
            result.select(expected.column_names).to_pylist(), expected.to_pylist()
        )
        self.assertEqual(sorted_by(os.path.join(self.tmp, "out.parquet")), keys)

    def test_null_sort_keys(self):
        # value is null in some rows: nulls sort last, nothing crashes
        for index, table in enumerate(self.hours):
            values = [
                None if i % 3 == 0 else v
                for i, v in enumerate(table["value"].to_pylist())
            ]
            self.hours[index] = table.set_column(
                3, "value", pa.array(values, pa.int64())
            )
            pq.write_table(self.hours[index], self.paths[index])
        keys = ["value", "entityId", "fetchTime"]
        expected = pa.concat_tables(self.hours)
        expected = expected.take(
            pc.sort_indices(
                expected, [(k, "ascending") for k in keys], null_placement="at_end"
            )
        )
        for batch_rows in (37, 100_000):
            with self.subTest(batch_rows=batch_rows):
                result = self._run(keys, False, batch_rows)
                self.assertEqual(
                    result.select(expected.column_names).to_pylist(),
                    expected.to_pylist(),
                )

    def test_deduplicate_across_hours(self):
        expected = deduplicate(pa.concat_tables(self.hours))
        expected = expected.take(
            pc.sort_indices(
                expected, [("entityId", "ascending"), ("firstSeen", "ascending")]
            )
        )
        for batch_rows in (37, 100_000):
            with self.subTest(batch_rows=batch_rows):
                result = self._run(["entityId", "firstSeen"], True, batch_rows)
                columns = ["entityId", "contentHash", "firstSeen", "lastSeen"]
                self.assertEqual(
                    result.select(columns).to_pylist(),
                    expected.select(columns).to_pylist(),
                )


if __name__ == "__main__":
    unittest.main()
