"""
trip_stop_events: one row per (provider, service date, trip, stop), from the
TripUpdate files and the static timetable. Needs Polars (static extra).

For a service date D, trip updates are read from the local days D-1, D and
D+1 (predictions made the evening before, trips running past midnight).

1. Each trip is resolved, from its trip descriptor only: its service date
   (trip.startDate; otherwise the day whose scheduled run, widened by
   WINDOW_BEFORE / WINDOW_AFTER, contains the updates), its static trip (by
   id, or by route and start time) and the static version to use (the last
   one its updates carried).
2. File by file, the stop time updates of trips running on D are exploded and
   aggregated per trip and stop (first and last prediction, when they were
   made, how many). Each file's result is folded into the running result, so
   memory stays around the day's distinct trip stops.
3. The static stop_times of the trips seen are joined: scheduled times (from
   "noon minus 12 h" of D, so right on DST days and past 24:00), delays from
   times and times from delays, delays carried from earlier stops, observed.
"""

import os
from datetime import date, datetime, timedelta
from datetime import timezone as dt_timezone
from typing import Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytz

# Rows read at once from a TripUpdate file
BATCH_ROWS = 200_000
# Marks a NO_DATA stop while carrying delays forward
NO_DATA_MARK = -(2**62)
# Stops are told apart by stop_sequence, or by stop_id when there is none
STOP_KEY = ["trip_index", "stop_sequence", "match_id"]
PREDICTED = ["arrival_delay", "arrival_time", "departure_delay", "departure_time"]
# Without startDate, updates belong to the run whose schedule, widened by
# these margins (and at most 12 h from its start), contains them
WINDOW_BEFORE = timedelta(hours=3)
WINDOW_AFTER = timedelta(hours=6)
WINDOW_MAX = timedelta(hours=12)
WEEKDAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]

STOP_FIELDS = {
    "stopSequence",
    "stopId",
    "arrival.delay",
    "arrival.time",
    "departure.delay",
    "departure.time",
    "scheduleRelationship",
}

TRIP_COLUMNS = [
    "fetchTime",
    "firstSeen",
    "lastSeen",
    "staticVersion",
    "trip_tripId",
    "trip_routeId",
    "trip_startTime",
    "trip_startDate",
    "trip_scheduleRelationship",
]


def service_day_base(day: date, tz) -> datetime:
    """GTFS times count from "noon minus 12 h" of the service day (not midnight on DST days)."""
    noon = tz.localize(datetime(day.year, day.month, day.day, 12))
    return (noon - timedelta(hours=12)).astimezone(dt_timezone.utc)


def read_trips(path: str) -> Iterator:
    """
    The trip columns of a TripUpdate file (missing ones as nulls), BATCH_ROWS
    rows at a time: a compacted day can be larger than memory.
    """
    import polars as pl

    file = pq.ParquetFile(path, pre_buffer=False)
    columns = [c for c in TRIP_COLUMNS if c in file.schema_arrow.names]
    for batch in file.iter_batches(batch_size=BATCH_ROWS, columns=columns):
        yield _with_keys(pl.from_arrow(pa.Table.from_batches([batch])), TRIP_COLUMNS)


def read_stop_updates(path: str, keys) -> Iterator:
    """
    The stop time updates of a TripUpdate file belonging to the trips of keys
    (raw_key, trip_index, window_start, window_end), one row per update, with
    the trip_index and the first_seen / last_seen of their trip update.
    """
    import polars as pl

    file = pq.ParquetFile(path, pre_buffer=False)
    if "stopTimeUpdate" not in file.schema_arrow.names:
        return
    columns = [c for c in TRIP_COLUMNS if c in file.schema_arrow.names]
    # Only the stop time update fields used: decoding the whole nested column
    # (uncertainty, occupancy, properties...) takes 3 times longer
    for index in range(len(file.schema)):
        parts = file.schema.column(index).path.split(".")
        if parts[0] == "stopTimeUpdate" and ".".join(parts[3:]) in STOP_FIELDS:
            columns.append(file.schema.column(index).path)
    for batch in file.iter_batches(batch_size=BATCH_ROWS, columns=columns):
        table = pa.Table.from_batches([batch])
        rows = (
            _with_keys(
                pl.from_arrow(table.drop_columns(["stopTimeUpdate"])), TRIP_COLUMNS
            )
            .with_row_index("row")
            .join(keys, on="raw_key", how="inner")
            # Trips without startDate: only the updates of D's run
            # (rows merged by deduplication overlap it)
            .filter(
                pl.col("window_start").is_null()
                | (
                    (pl.col("first_seen") <= pl.col("window_end"))
                    & (pl.col("last_seen") >= pl.col("window_start"))
                )
            )
            .select("row", "trip_index", "first_seen", "last_seen")
        )
        if rows.is_empty():
            continue
        # Flattened without copying the nested values, then joined to their
        # trip update by row number (much cheaper than exploding in Polars)
        updates = table.column("stopTimeUpdate").combine_chunks()
        flat = pc.list_flatten(updates)
        parents = pc.list_parent_indices(updates).cast(pa.uint32())
        yield _stop_fields(
            pl.from_arrow(pa.table({"row": parents, "update": flat}), rechunk=False)
        ).join(rows, on="row", how="inner")


def _stop_fields(frame):
    """Columns of the stop time update struct (nulls for fields a file does not have)."""
    import polars as pl

    fields = {f.name: f.dtype for f in frame.schema["update"].fields}
    update = pl.col("update")

    def field(name, kind, alias):
        outer, _, inner = name.partition(".")
        if outer not in fields or (
            inner and inner not in [f.name for f in fields[outer].fields]
        ):
            return pl.lit(None, kind).alias(alias)
        expr = update.struct.field(outer)
        if inner:
            expr = expr.struct.field(inner)
        return expr.cast(kind).alias(alias)

    return frame.select(
        "row",
        field("stopSequence", pl.Int64, "stop_sequence"),
        field("stopId", pl.Utf8, "stop_id"),
        *[
            field(f"{kind}.{value}", pl.Int64, f"{kind}_{value}")
            for kind in ("arrival", "departure")
            for value in ("delay", "time")
        ],
        field("scheduleRelationship", pl.Utf8, "stop_schedule_relationship"),
    )


def _with_keys(frame, wanted):
    import polars as pl

    timestamp = pl.Datetime("us", "UTC")
    for name in wanted:
        if name not in frame.columns:
            kind = timestamp if name in ("firstSeen", "lastSeen") else pl.Utf8
            frame = frame.with_columns(pl.lit(None, kind).alias(name))
    return frame.with_columns(
        pl.coalesce("firstSeen", "fetchTime").cast(timestamp).alias("first_seen"),
        pl.coalesce("lastSeen", "fetchTime").cast(timestamp).alias("last_seen"),
        # How the feed names a trip: its id, start date and start time (runs
        # of frequency-based trips), or (no id) its start date, route and
        # start time
        pl.when(pl.col("trip_tripId").is_not_null())
        .then(
            pl.concat_str(
                [
                    pl.col("trip_tripId"),
                    pl.col("trip_startDate").fill_null(""),
                    pl.col("trip_startTime").fill_null(""),
                ],
                separator="|",
            )
        )
        .otherwise(
            pl.concat_str(
                [
                    pl.lit(""),
                    pl.col("trip_startDate").fill_null(""),
                    pl.col("trip_routeId").fill_null(""),
                    pl.col("trip_startTime").fill_null(""),
                ],
                separator="|",
            )
        )
        .alias("raw_key"),
    )


def parse_gtfs_time(value: Optional[str]) -> Optional[timedelta]:
    """HH:MM:SS (hours may pass 24) as a duration, None if invalid."""
    try:
        hours, minutes, seconds = (int(x) for x in value.split(":"))
    except (AttributeError, ValueError):
        return None
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


class StaticTimetable:
    """The static tables of one version, from a local folder of Parquet files."""

    def __init__(self, folder: str):
        import polars as pl

        def table(name, columns):
            path = os.path.join(folder, f"{name}.parquet")
            if not os.path.exists(path):
                return None
            available = pq.read_schema(path).names
            return pl.read_parquet(path, columns=[c for c in columns if c in available])

        self.folder = folder
        self.trips = table("trips", ["trip_id", "route_id", "service_id"])
        self.calendar = table(
            "calendar", ["service_id", *WEEKDAYS, "start_date", "end_date"]
        )
        self.calendar_dates = table(
            "calendar_dates", ["service_id", "date", "exception_type"]
        )
        frequencies = table("frequencies", ["trip_id"])
        # Trips whose runs are given by frequencies: told apart by start time
        self.frequency_trips = (
            set(frequencies["trip_id"].to_list()) if frequencies is not None else set()
        )
        self._active: Dict[date, set] = {}
        self._starts = None

    def stop_times(self, trip_ids: List[str]):
        """Stop times of some trips only (stop_times can hold tens of millions of rows)."""
        import polars as pl

        return (
            pl.scan_parquet(os.path.join(self.folder, "stop_times.parquet"))
            .select(
                "trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"
            )
            .filter(pl.col("trip_id").is_in(list(trip_ids)))
            .collect()
        )

    def spans(self, trip_ids: List[str]):
        """First departure, last arrival and service of some trips."""
        import polars as pl

        spans = (
            self.stop_times(trip_ids)
            .group_by("trip_id")
            .agg(
                pl.coalesce("departure_time", "arrival_time")
                .sort_by("stop_sequence")
                .first()
                .alias("start"),
                pl.coalesce("arrival_time", "departure_time")
                .sort_by("stop_sequence")
                .last()
                .alias("end"),
            )
        )
        if self.trips is not None:
            spans = spans.join(
                self.trips.select("trip_id", "service_id"), on="trip_id", how="left"
            )
        return spans

    def trip_starts(self):
        """First departure of every trip, computed once (to match trips without id)."""
        import polars as pl

        if self._starts is None:
            self._starts = (
                pl.scan_parquet(os.path.join(self.folder, "stop_times.parquet"))
                .group_by("trip_id")
                .agg(
                    pl.coalesce("departure_time", "arrival_time")
                    .sort_by("stop_sequence")
                    .first()
                    .alias("start")
                )
                .collect()
            )
        return self._starts

    def active_services(self, day: date) -> set:
        """Service ids running on day (calendar, then calendar_dates exceptions)."""
        import polars as pl

        if day in self._active:
            return self._active[day]
        active = set()
        if self.calendar is not None:
            active = set(
                self.calendar.filter(
                    (pl.col(WEEKDAYS[day.weekday()]) == 1)
                    & (pl.col("start_date") <= day)
                    & (pl.col("end_date") >= day)
                )["service_id"].to_list()
            )
        if self.calendar_dates is not None:
            exceptions = self.calendar_dates.filter(pl.col("date") == day)
            active |= set(
                exceptions.filter(pl.col("exception_type") == 1)["service_id"].to_list()
            )
            active -= set(
                exceptions.filter(pl.col("exception_type") == 2)["service_id"].to_list()
            )
        self._active[day] = active
        return active


def build_trip_stop_events(
    files: List[str],
    service_date: date,
    timezone: str,
    provider: str,
    timetables: Dict[str, StaticTimetable],
):
    """
    Compute the trip stop events of one provider and service date.

    @param files: Local TripUpdate files of the local days around service_date
    @param service_date: The service date D
    @param timezone: Provider timezone
    @param provider: Provider name (stored in every row)
    @param timetables: Static timetable by static version, oldest first (the
        last one is used for trips whose version is unknown)
    @return A Polars DataFrame, one row per trip and stop (None if no trip ran)
    """
    import polars as pl

    tz = pytz.timezone(timezone)
    trips = resolve_trips(files, service_date, tz, timetables)
    if trips is None:
        return None

    # Aggregated by an integer per trip_key (faster than strings)
    trips = trips.with_columns(
        pl.col("trip_key").rank("dense").cast(pl.UInt32).alias("trip_index")
    )
    keys = trips.select("raw_key", "trip_index", "window_start", "window_end")
    running = None
    for path in files:
        for updates in read_stop_updates(path, keys):
            partial = _stop_updates(updates)
            running = (
                partial if running is None else _fold(pl.concat([running, partial]))
            )
    if running is not None:
        running = running.join(
            trips.select("trip_index", "trip_key").unique("trip_index"),
            on="trip_index",
        ).drop("trip_index", "match_id")
    return _events(
        trips,
        running,
        timetables,
        service_day_base(service_date, tz),
        provider,
        service_date,
    )


def _trip_aggregates():
    import polars as pl

    # Rows sorted by last_seen
    return [
        pl.col("trip_tripId").first(),
        pl.col("trip_startDate").first(),
        pl.col("trip_routeId").drop_nulls().last(),
        pl.col("trip_startTime").drop_nulls().last(),
        pl.col("trip_scheduleRelationship").last(),
        pl.col("staticVersion").drop_nulls().last(),
        pl.col("first_seen").min(),
        pl.col("last_seen").max(),
    ]


def resolve_trips(files, service_date, tz, timetables):
    """
    The trips running on service_date: one row per raw_key (how the feed names
    the trip), with trip_key (the static trip id, or the raw key), route,
    start time, last trip schedule relationship, static version and, for trips
    without startDate, the window of D's run (None if no trip ran).
    """
    import polars as pl

    # Trips without startDate are summarised per hour: the same raw key
    # names the runs of every day
    hour = (
        pl.when(pl.col("trip_startDate").is_null())
        .then(pl.col("first_seen").dt.truncate("1h"))
        .alias("hour")
    )

    def summarise(frame):
        return (
            frame.sort("last_seen").group_by("raw_key", "hour").agg(*_trip_aggregates())
        )

    seen = None
    for path in files:
        for frame in read_trips(path):
            part = summarise(frame.with_columns(hour))
            seen = (
                part
                if seen is None
                else summarise(pl.concat([seen, part], how="diagonal_relaxed"))
            )
    if seen is None:
        return None
    versions = list(timetables)
    default_version = versions[-1] if versions else None
    seen = seen.with_columns(
        pl.when(pl.col("staticVersion").is_in(versions))
        .then(pl.col("staticVersion"))
        .otherwise(pl.lit(default_version, pl.Utf8))
        .alias("static_version")
    )

    dated = seen.filter(
        pl.col("trip_startDate") == service_date.strftime("%Y%m%d")
    ).with_columns(
        pl.lit(None, pl.Datetime("us", "UTC")).alias("window_start"),
        pl.lit(None, pl.Datetime("us", "UTC")).alias("window_end"),
    )
    undated = _undated_on(
        seen.filter(pl.col("trip_startDate").is_null()), service_date, tz, timetables
    )
    trips = pl.concat([dated, undated], how="diagonal_relaxed").drop("hour")
    if trips.is_empty():
        return None

    # Static trip of the updates without trip id
    matched = [
        _match_trips(group, service_date, timetables[version])
        for (version,), group in trips.filter(pl.col("trip_tripId").is_null()).group_by(
            "static_version"
        )
        if version in timetables
    ]
    trips = trips.join(
        (
            pl.concat(matched)
            if matched
            else pl.DataFrame(schema={"raw_key": pl.Utf8, "matched_id": pl.Utf8})
        ),
        on="raw_key",
        how="left",
    ).with_columns(pl.coalesce("trip_tripId", "matched_id").alias("trip_id"))
    trips = _trip_keys(trips, timetables)

    # Route from the timetable when the feed gives only the trip id
    routes = [
        timetable.trips.select(
            pl.lit(version, pl.Utf8).alias("static_version"),
            "trip_id",
            pl.col("route_id").alias("static_route_id"),
        )
        for version, timetable in timetables.items()
        if timetable.trips is not None and "route_id" in timetable.trips.columns
    ]
    if routes:
        trips = trips.join(
            pl.concat(routes), on=["static_version", "trip_id"], how="left"
        ).with_columns(pl.coalesce("trip_routeId", "static_route_id"))
    return trips.select(
        "raw_key",
        "trip_key",
        "trip_id",
        pl.col("trip_routeId").alias("route_id"),
        pl.col("trip_startTime").alias("trip_start_time"),
        pl.col("trip_scheduleRelationship").alias("trip_schedule_relationship"),
        "static_version",
        "time_shift",
        pl.col("last_seen").alias("trip_last_seen"),
        "window_start",
        "window_end",
    )


def _trip_keys(trips, timetables):
    """
    trip_key: the static trip id; for frequency-based trips, the id and start
    time, with time_shift the seconds between the run's start and the
    template's (stop_times of such trips hold one run for all).
    """
    import polars as pl

    shifts = []
    for (version,), group in trips.filter(pl.col("trip_id").is_not_null()).group_by(
        "static_version"
    ):
        timetable = timetables.get(version)
        if timetable is None or not timetable.frequency_trips:
            continue
        runs = group.filter(
            pl.col("trip_id").is_in(list(timetable.frequency_trips))
            & pl.col("trip_startTime").is_not_null()
        )
        if runs.is_empty():
            continue
        starts = {
            row["trip_id"]: row["start"]
            for row in timetable.spans(runs["trip_id"].unique().to_list()).iter_rows(
                named=True
            )
        }
        for row in runs.iter_rows(named=True):
            start = parse_gtfs_time(row["trip_startTime"])
            template = starts.get(row["trip_id"])
            if start is not None and template is not None:
                shifts.append(
                    {
                        "raw_key": row["raw_key"],
                        "time_shift": int((start - template).total_seconds()),
                    }
                )
    trips = trips.join(
        pl.DataFrame(shifts, schema={"raw_key": pl.Utf8, "time_shift": pl.Int64}),
        on="raw_key",
        how="left",
    )
    return trips.with_columns(
        pl.when(pl.col("time_shift").is_not_null())
        .then(pl.concat_str(["trip_id", "trip_startTime"], separator="|"))
        .otherwise(pl.coalesce("trip_id", "raw_key"))
        .alias("trip_key"),
        pl.col("time_shift").fill_null(0),
    )


def _run_window(base: datetime, start: timedelta, end: timedelta):
    """Updates of a run starting at base + start: from WINDOW_BEFORE before its start to WINDOW_AFTER after its end, at most WINDOW_MAX from its start."""
    departure = base + start
    return (
        departure - min(WINDOW_BEFORE, WINDOW_MAX),
        min(base + end + WINDOW_AFTER, departure + WINDOW_MAX),
    )


def _undated_on(hours, service_date, tz, timetables):
    """
    Trips without startDate (summarised per hour) that ran on service_date:
    those with updates inside the window of D's run. The run's schedule comes
    from the static trip, else the trip's start time, else the local day.
    """
    import polars as pl

    if hours.is_empty():
        return hours
    spans: Dict[Optional[str], Dict[str, dict]] = {}
    for (version,), group in hours.group_by("static_version"):
        timetable = timetables.get(version)
        ids = group["trip_tripId"].drop_nulls().unique().to_list()
        if timetable is not None and ids:
            spans[version] = {
                row["trip_id"]: row
                for row in timetable.spans(ids).iter_rows(named=True)
            }

    base = service_day_base(service_date, tz)
    next_base = service_day_base(service_date + timedelta(days=1), tz)
    kept = []
    for record in hours.iter_rows(named=True):
        span = spans.get(record["static_version"], {}).get(record["trip_tripId"])
        timetable = timetables.get(record["static_version"])
        start_time = parse_gtfs_time(record["trip_startTime"])
        frequency = (
            timetable is not None and record["trip_tripId"] in timetable.frequency_trips
        )
        if span is not None and span["start"] is not None:
            service = span.get("service_id")
            if (
                timetable is not None
                and service is not None
                and service not in timetable.active_services(service_date)
            ):
                continue
            start, end = span["start"], span["end"] or span["start"]
            if frequency and start_time is not None:
                start, end = start_time, start_time + (end - start)
            window = _run_window(base, start, end)
        elif start_time is not None:
            window = _run_window(base, start_time, start_time)
        else:
            window = (base, next_base)
        # The hour's updates overlap the window
        if record["hour"] <= window[1] and record["last_seen"] >= window[0]:
            kept.append({**record, "window_start": window[0], "window_end": window[1]})
    if not kept:
        return hours.clear().with_columns(
            pl.lit(None, pl.Datetime("us", "UTC")).alias("window_start"),
            pl.lit(None, pl.Datetime("us", "UTC")).alias("window_end"),
        )
    frame = pl.DataFrame(
        kept,
        schema={
            **hours.schema,
            "window_start": pl.Datetime("us", "UTC"),
            "window_end": pl.Datetime("us", "UTC"),
        },
    )
    # One row per trip again
    return (
        frame.sort("last_seen")
        .group_by("raw_key")
        .agg(
            *_trip_aggregates(),
            pl.col("hour").first(),
            pl.col("static_version").last(),
            pl.col("window_start").first(),
            pl.col("window_end").first(),
        )
    )


def _match_trips(records, service_date, timetable):
    """
    Static trips of updates without trip id: same route and start time,
    running that day, when exactly one trip matches. Returns raw_key and
    matched_id.
    """
    import polars as pl

    empty = pl.DataFrame(schema={"raw_key": pl.Utf8, "matched_id": pl.Utf8})
    if timetable.trips is None or "route_id" not in timetable.trips.columns:
        return empty
    wanted = records.select(
        "raw_key",
        pl.col("trip_routeId").alias("route_id"),
        pl.col("trip_startTime")
        .map_elements(parse_gtfs_time, return_dtype=pl.Duration("us"))
        .cast(pl.Duration("ms"))
        .alias("start"),
    ).drop_nulls()
    if wanted.is_empty():
        return empty
    running = timetable.trips.filter(
        pl.col("service_id").is_in(list(timetable.active_services(service_date)))
        & pl.col("route_id").is_in(wanted["route_id"].unique().to_list())
    ).select("trip_id", "route_id")
    starts = timetable.trip_starts().select(
        "trip_id", pl.col("start").cast(pl.Duration("ms"))
    )
    return (
        wanted.join(running, on="route_id")
        .join(starts, on=["trip_id", "start"])
        .group_by("raw_key")
        .agg(pl.col("trip_id").first().alias("matched_id"), pl.len().alias("n"))
        .filter(pl.col("n") == 1)
        .select("raw_key", "matched_id")
    )


def _stop_updates(updates):
    """Aggregate stop time updates per trip and stop."""
    import polars as pl

    updates = updates.with_columns(
        pl.when(pl.col("stop_sequence").is_null())
        .then(pl.col("stop_id"))
        .alias("match_id")
    )
    return updates.group_by(STOP_KEY).agg(
        pl.col("stop_id").sort_by("last_seen").drop_nulls().last(),
        *[
            pl.col(c).sort_by("first_seen").first().alias(f"first_{c}")
            for c in PREDICTED
        ],
        *[pl.col(c).sort_by("last_seen").last().alias(f"last_{c}") for c in PREDICTED],
        pl.col("stop_schedule_relationship").sort_by("last_seen").last(),
        pl.col("first_seen").min(),
        pl.col("last_seen").max(),
        pl.len().cast(pl.Int64).alias("prediction_count"),
    )


def _fold(frame):
    """Merge partial aggregates of the same stops (from different files)."""
    import polars as pl

    return frame.group_by(STOP_KEY).agg(
        pl.col("stop_id").sort_by("last_seen").drop_nulls().last(),
        *[pl.col(f"first_{c}").sort_by("first_seen").first() for c in PREDICTED],
        *[pl.col(f"last_{c}").sort_by("last_seen").last() for c in PREDICTED],
        pl.col("stop_schedule_relationship").sort_by("last_seen").last(),
        pl.col("first_seen").min(),
        pl.col("last_seen").max(),
        pl.col("prediction_count").sum(),
    )


def _empty_updates():
    import polars as pl

    return pl.DataFrame(
        schema={
            "trip_key": pl.Utf8,
            "stop_sequence": pl.Int64,
            "stop_id": pl.Utf8,
            **{f"{w}_{c}": pl.Int64 for w in ("first", "last") for c in PREDICTED},
            "stop_schedule_relationship": pl.Utf8,
            "first_seen": pl.Datetime("us", "UTC"),
            "last_seen": pl.Datetime("us", "UTC"),
            "prediction_count": pl.Int64,
        }
    )


def _static_stops(trips, timetables, base_seconds):
    """Scheduled stops (UTC Unix time) of the trips seen, from each trip's static version."""
    import polars as pl

    parts = []
    for (version,), group in trips.filter(pl.col("trip_id").is_not_null()).group_by(
        "static_version"
    ):
        timetable = timetables.get(version)
        if timetable is None:
            continue
        runs = group.select("trip_key", "trip_id", "time_shift").unique("trip_key")
        parts.append(
            timetable.stop_times(runs["trip_id"].unique().to_list())
            .join(runs, on="trip_id")
            .select(
                "trip_key",
                pl.col("stop_sequence").cast(pl.Int64),
                "stop_id",
                *[
                    (
                        pl.col(f"{kind}_time").dt.total_seconds().cast(pl.Int64)
                        + pl.col("time_shift")
                        + base_seconds
                    ).alias(f"scheduled_{kind}")
                    for kind in ("arrival", "departure")
                ],
            )
        )
    if parts:
        return pl.concat(parts)
    return pl.DataFrame(
        schema={
            "trip_key": pl.Utf8,
            "stop_sequence": pl.Int64,
            "stop_id": pl.Utf8,
            "scheduled_arrival": pl.Int64,
            "scheduled_departure": pl.Int64,
        }
    )


def _events(trips, updates, timetables, base, provider, service_date):
    import polars as pl

    if updates is None:
        updates = _empty_updates()
    static = _static_stops(trips, timetables, int(base.timestamp()))

    # Match updates to static stops: by stop_sequence, else by stop_id
    by_sequence = static.join(
        updates.filter(pl.col("stop_sequence").is_not_null()).drop("stop_id"),
        on=["trip_key", "stop_sequence"],
        how="left",
    )
    matched = by_sequence.filter(pl.col("prediction_count").is_not_null())
    by_stop = (
        by_sequence.filter(pl.col("prediction_count").is_null())
        .select(static.columns)
        .join(
            updates.filter(pl.col("stop_sequence").is_null())
            .drop("stop_sequence")
            .unique(["trip_key", "stop_id"], keep="first"),
            on=["trip_key", "stop_id"],
            how="left",
        )
    )
    # Updates of trips the timetable does not know (e.g. added trips)
    unknown = updates.join(
        static.select("trip_key").unique(), on="trip_key", how="anti"
    )
    events = pl.concat([matched, by_stop, unknown], how="diagonal_relaxed")
    # Trips without any stop update (e.g. canceled): their static stops
    no_updates = trips.select("trip_key").join(
        events.select("trip_key").unique(), on="trip_key", how="anti"
    )
    events = pl.concat(
        [events, static.join(no_updates, on="trip_key", how="semi")],
        how="diagonal_relaxed",
    )

    events = events.join(
        trips.select(
            "trip_key",
            "trip_id",
            "route_id",
            "trip_start_time",
            "trip_schedule_relationship",
            "static_version",
            "trip_last_seen",
        ).unique("trip_key", keep="last"),
        on="trip_key",
        how="left",
    ).sort("trip_key", "stop_sequence", nulls_last=True)

    # Delays from times, times from delays
    for kind in ("arrival", "departure"):
        scheduled = pl.col(f"scheduled_{kind}")
        for which in ("first", "last"):
            delay = pl.col(f"{which}_{kind}_delay")
            time = pl.col(f"{which}_{kind}_time")
            events = events.with_columns(
                pl.coalesce(delay, time - scheduled).alias(f"{which}_{kind}_delay"),
                pl.coalesce(time, scheduled + delay).alias(f"{which}_{kind}_time"),
            )

    # A stop without an update of its own takes the delay of the stop before
    # it (its departure delay, else its arrival delay), up to a NO_DATA stop
    # (the stops after it have no data either); skipped stops and canceled or
    # deleted trips have no times
    own = pl.col("prediction_count").is_not_null()
    no_data = own & (pl.col("stop_schedule_relationship") == "NO_DATA").fill_null(False)
    no_times = (pl.col("stop_schedule_relationship") == "SKIPPED").fill_null(False) | (
        pl.col("trip_schedule_relationship").is_in(["CANCELED", "DELETED"])
    ).fill_null(False)
    stop = pl.lit(NO_DATA_MARK, pl.Int64)
    carried = []
    for which in ("first", "last"):
        value = (
            pl.when(no_data)
            .then(stop)
            .when(own & ~no_times)
            .then(pl.coalesce(f"{which}_departure_delay", f"{which}_arrival_delay"))
            .forward_fill()
            .over("trip_key")
        )
        carried.append(
            pl.when(value == stop).then(None).otherwise(value).alias(f"{which}_carried")
        )
    events = events.with_columns(carried)
    propagated = ~own & ~no_times & pl.col("last_carried").is_not_null()
    columns = []
    for which in ("first", "last"):
        for kind in ("arrival", "departure"):
            delay = f"{which}_{kind}_delay"
            columns.append(
                pl.when(no_times)
                .then(None)
                .when(own)
                .then(pl.col(delay))
                .otherwise(pl.col(f"{which}_carried"))
                .alias(delay)
            )
    events = events.with_columns(*columns, propagated.alias("delay_propagated"))
    columns = []
    for which in ("first", "last"):
        for kind in ("arrival", "departure"):
            time = f"{which}_{kind}_time"
            columns.append(
                pl.when(no_times)
                .then(None)
                .when(own)
                .then(pl.col(time))
                .otherwise(
                    pl.col(f"scheduled_{kind}") + pl.col(f"{which}_{kind}_delay")
                )
                .alias(time)
            )
    events = events.with_columns(columns)

    # Observed: the last prediction was made at or after the predicted
    # event, or the stop left the feed (feeds listing upcoming stops only)
    # while the trip was still reported after the predicted event
    event_time = pl.coalesce("last_departure_time", "last_arrival_time")
    trip_seen = pl.col("trip_last_seen").dt.epoch("s")
    events = events.with_columns(
        (
            own
            & (
                (pl.col("last_seen").dt.epoch("s") >= event_time)
                | (
                    (pl.col("trip_last_seen") > pl.col("last_seen"))
                    & (trip_seen >= event_time)
                )
            )
        )
        .fill_null(False)
        .alias("observed"),
        pl.col("prediction_count").fill_null(0),
    )
    return events.select(
        pl.lit(provider).alias("provider"),
        pl.lit(service_date).alias("service_date"),
        "trip_id",
        "route_id",
        "trip_start_time",
        "stop_sequence",
        "stop_id",
        "scheduled_arrival",
        "scheduled_departure",
        "first_arrival_delay",
        "last_arrival_delay",
        "first_departure_delay",
        "last_departure_delay",
        pl.col("last_arrival_time").alias("last_predicted_arrival"),
        pl.col("last_departure_time").alias("last_predicted_departure"),
        "first_seen",
        "last_seen",
        "prediction_count",
        "observed",
        "delay_propagated",
        "trip_schedule_relationship",
        "stop_schedule_relationship",
        "static_version",
        # Partition column shared with the other services (Iceberg)
        pl.lit(service_date).alias("date"),
    )
