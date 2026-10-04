"""
Final delays: one row per trip and planned call of a service day, with the last delay the feed gave,
matched to the GTFS timetable. Needs DuckDB (pip install 'gtfs_rt_aggregator[punctuality]').

Built for real feeds, which rarely follow the spec to the letter:

- Each trip is matched to the timetable version its rows name (staticVersion), as trip ids change between
  versions for some feeds; rows without one use default_version. A trip missing from its version takes the
  newest version that has it (feeds that keep sending a dropped id).
- A trip whose id ends in a date (…:1813:20271210) and is missing from the timetable takes the only trip
  with the same id up to that date that runs on the day (feeds naming trains after the next timetable year).
- Each stop update is matched to its planned call by stop: the same stop, or another platform of the same
  station (parent_station), nearest scheduled time first, so a train calling twice at one stop keeps both
  calls. The rest by stop_sequence, read as the timetable's stop_sequence, the position in the trip, or the
  rank among commercial calls (pickup or drop-off allowed), whichever fits the live times best.
- Feeds sending only predicted times get delays from the schedule ("noon minus 12 h" of the service day, so
  right on DST days and past 24:00). A predicted time dated a whole day off moves to the planned time's day,
  and a delay beyond ±12 h loses its whole days.
- Planned commercial calls the feed never listed, after its first listed call, take the delay of the nearest
  earlier call (GTFS-RT propagation): delay_source "propagated" instead of "feed".
- A CANCELED (or DELETED) trip gets all its planned commercial calls without times; one without a
  timetable entry or updates still gets one row. Updates that match no call (ADDED trips) are kept as they
  are, without a schedule.

Input: local TripUpdate Parquet files as the aggregator writes them (trip columns flattened, e.g.
trip_tripId; stopTimeUpdate a list of structs), including the next local day's files for trips running past
midnight. The timetable comes through a callback, so any storage works (see storage_timetable).

Limits: trips are keyed by trip_id and service date, so frequency-based trips (one trip_id, several runs)
and trips sent without a trip_id are not supported, and the service date is trip.startDate or, when the
feed sends none, the local date of the fetch (a trip without startDate running past midnight is split in
two). Rail feeds fit; for frequency-based or undated feeds, TripStopEvent (aggregator/trip_stop_events.py)
resolves runs from the schedule.
"""

import logging

import datetime as dt
import json
import re
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Writes a timetable table (e.g. "stop_times") of a static version to dest as Parquet; False if there is
# no such version or table
Timetable = Callable[[str, str, Path], bool]

logger = logging.getLogger(__name__)

# Updates of one stop are told apart by their scheduled time in windows of this many seconds (a train
# calling twice at one stop)
CALL_WINDOW_S = 600
# A stop_sequence numbering fits an update when the call's scheduled time is within this many seconds of the
# update's; the numbering kept must fit at least MIN_FIT of a sample of SAMPLE_ROWS updates
FIT_S = 1800
MIN_FIT = 0.5
SAMPLE_ROWS = 20_000

# Ties between updates of one fetch: the later row of the files, then the values (one list can repeat a stop)
LATER = ("src DESC, rn DESC, arr_t DESC NULLS LAST, dep_t DESC NULLS LAST, arr_d DESC NULLS LAST, "
         "dep_d DESC NULLS LAST, stop_status NULLS LAST")

# Columns of the result, in order
COLUMNS = [
    "feed_id",  # "" unless the files hold several feeds (feedId)
    "service_date",  # YYYYMMDD
    "trip_id",
    "stop_sequence",  # the timetable's; the feed's for updates that match no call
    "stop_id",
    "scheduled_arrival",  # Unix seconds
    "scheduled_departure",
    "arrival_delay",  # seconds
    "departure_delay",
    "predicted_arrival",  # Unix seconds
    "predicted_departure",
    "observed_at",  # Unix seconds of the fetch the final value comes from
    "stop_schedule_relationship",  # as in the feed, e.g. SKIPPED
    "delay_source",  # "feed" or "propagated"
    "trip_schedule_relationship",  # as in the feed, e.g. CANCELED, ADDED
    "route_id",
    "trip_start_time",
]


_EMPTY = [("feed_id", pa.string()), ("service_date", pa.string()), ("trip_id", pa.string()), ("stop_sequence", pa.int64()),
          ("stop_id", pa.string()), ("scheduled_arrival", pa.float64()), ("scheduled_departure", pa.float64()),
          ("arrival_delay", pa.int64()), ("departure_delay", pa.int64()), ("predicted_arrival", pa.float64()),
          ("predicted_departure", pa.float64()), ("observed_at", pa.float64()), ("stop_schedule_relationship", pa.string()),
          ("delay_source", pa.string()), ("trip_schedule_relationship", pa.string()), ("route_id", pa.string()),
          ("trip_start_time", pa.string())]


def storage_timetable(storage, provider_name: str, feed_name: str = "static") -> Timetable:
    """A Timetable reading the static versions the aggregator stored (StaticService's layout)."""
    from .static.service import manifest_tables, static_base

    base = static_base(provider_name, feed_name)
    manifests: Dict[str, Optional[dict]] = {}

    def fetch(version: str, table: str, dest: Path) -> bool:
        if version not in manifests:
            try:
                manifest = json.loads(storage.read_bytes(f"{base}/{version}/manifest.json"))
                manifests[version] = manifest_tables(manifest, base)
            except FileNotFoundError:
                manifests[version] = None
        path = (manifests[version] or {}).get(table)
        if not path:
            return False
        storage.read_to_file(path, str(dest))
        return True

    return fetch


def final_calls(
    files: Iterable[Union[str, Path]],
    service_date: str,
    timezone: str,
    timetable: Timetable,
    work_dir: Union[str, Path],
    default_version: Optional[str] = None,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
) -> Tuple[pa.Table, dict]:
    """
    The final delay of every trip at every planned call of a service day (see the module docstring).

    @param files: TripUpdate Parquet files: the service day's local day and the next one
    @param service_date: "YYYY-MM-DD" or "YYYYMMDD"
    @param timezone: The timetable's timezone, e.g. "Europe/Brussels"
    @param timetable: Fetches timetable tables per static version (see storage_timetable)
    @param work_dir: Folder for the timetable tables and DuckDB's spill files, one per concurrent call
    @param default_version: Static version for rows without staticVersion (e.g. the latest). Without it,
        files written without staticVersion get no schedule
    @param memory_limit: DuckDB's memory limit, e.g. "650MB" (default: DuckDB's, 80% of RAM)
    @param threads: DuckDB's threads (default: DuckDB's, one per core)
    @return The calls (columns COLUMNS, sorted by trip and stop_sequence) and figures:
        timetable_version (default_version), timetable_versions (the versions used, most rows first),
        stop_sequence_mode ("full", "position", "commercial" or None) and stop_sequence_scores,
        trips_matched_by_id_before_date, updates_matched and trips_in_timetable (shares, None
        without data), trips_added (share), propagated_calls
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("final_calls needs DuckDB: pip install 'gtfs_rt_aggregator[punctuality]'")

    try:
        day = dt.datetime.strptime(service_date.strip().replace("-", ""), "%Y%m%d").strftime("%Y%m%d")
    except ValueError:
        raise ValueError(f"service_date must be YYYY-MM-DD or YYYYMMDD, not {service_date!r}")
    if memory_limit is not None and not re.fullmatch(r"\d+(\.\d+)?\s*[KMGT]?i?B", memory_limit, re.IGNORECASE):
        raise ValueError(f"memory_limit must look like '650MB' or '2GB', not {memory_limit!r}")
    files = [Path(f) for f in files]
    if not files:
        return pa.table({name: pa.array([], type) for name, type in _EMPTY}), {}
    tmp = Path(work_dir)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        schema = pa.unify_schemas([pq.read_schema(p) for p in files], promote_options="permissive")
    except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
        raise ValueError(f"final_calls: the files' schemas differ (files written by different versions?), "
                         f"rewrite them to one schema first: {e}") from e
    con = duckdb.connect()
    try:
        return _final_calls(con, files, schema, day, timezone, timetable, tmp, default_version, memory_limit, threads)
    finally:
        con.close()
        # Timetable tables left by an error
        for path in tmp.glob("static-*.parquet"):
            path.unlink(missing_ok=True)


def _sql_col(schema: pa.Schema, name: str, sql_type: str = "VARCHAR") -> str:
    """A column of the files, or NULL when no file has it (optional fields come and go)."""
    if name not in schema.names:
        return f"CAST(NULL AS {sql_type})"
    if sql_type == "DOUBLE" and pa.types.is_timestamp(schema.field(name).type):
        return f'epoch("{name}")'
    return f'CAST("{name}" AS {sql_type})'


def _update_fields(schema: pa.Schema) -> set:
    """Paths available inside stopTimeUpdate items, e.g. {"stopId", "arrival.delay"}."""
    if "stopTimeUpdate" not in schema.names:
        return set()
    item = schema.field("stopTimeUpdate").type.value_type
    paths = set()
    for f in item:
        paths.add(f.name)
        if pa.types.is_struct(f.type):
            paths.update(f"{f.name}.{g.name}" for g in f.type)
    return paths


def _local_noon_minus_12h(sd: str, tz: str) -> float:
    """GTFS times count from "noon minus 12 h" of the service day (midnight except on DST days)."""
    day = dt.date(int(sd[:4]), int(sd[4:6]), int(sd[6:8]))
    return (dt.datetime.combine(day, dt.time(12), ZoneInfo(tz)) - dt.timedelta(hours=12)).timestamp()


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _final_calls(con, files: List[Path], schema: pa.Schema, service_day: str, tz: str, timetable: Timetable,
                 tmp: Path, default_version: Optional[str], memory_limit: Optional[str], threads: Optional[int]):
    if memory_limit is not None:
        con.execute(f"SET memory_limit = {_quote(memory_limit)}")
    if threads is not None:
        con.execute(f"SET threads = {int(threads)}")
    con.execute(f"SET preserve_insertion_order = false; SET temp_directory = {_quote(str(tmp / 'duckdb'))}")
    con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet([{', '.join(_quote(str(p)) for p in files)}], union_by_name = true, filename = true, file_row_number = true)")
    c = lambda name, t="VARCHAR": _sql_col(schema, name, t)
    observed = f"coalesce({c('lastSeen', 'DOUBLE')}, {c('fetchTime', 'DOUBLE')})"
    # Several rows can hold the same fetch (a trip sent twice): ties go to the later row of the files, so
    # a day always gives the same result
    # (rows without a fetch time never win, as with arg_max on observed alone)
    last = "CASE WHEN observed IS NOT NULL THEN struct_pack(o := observed, f := src, r := rn) END"
    # The service day: trip_startDate (either format), else the local day of the fetch
    fetch_day = f"strftime({c('date', 'DATE')}, '%Y%m%d')" if "date" in schema.names else f"'{service_day}'"
    # One feed per source (most): ignore feedId, which files written before 0.7.3 lack (a trip would be split in two)
    feeds = con.execute(f"SELECT count(DISTINCT {c('feedId')}) FROM raw").fetchone()[0] if "feedId" in schema.names else 0
    feed = f"coalesce({c('feedId')}, '')" if feeds > 1 else "''"
    trip_keys = (f"{feed} AS feed, replace(coalesce({c('trip_startDate')}, {fetch_day}), '-', '') AS sd, "
                 f"{c('trip_tripId')} AS trip")
    fallback_version = _quote(default_version) if default_version else "NULL"
    # Each trip is matched to the timetable version its rows name (trip ids change between versions for
    # some feeds); rows without one use default_version
    con.execute(f"""CREATE TABLE t AS
        SELECT feed, sd, trip, arg_max(status, {last}) AS status, arg_max(route_id, {last}) FILTER (route_id IS NOT NULL) AS route_id,
               max(start_time) AS start_time, coalesce(arg_max(version, {last}) FILTER (version IS NOT NULL), {fallback_version}) AS version
        FROM (SELECT {trip_keys}, {c('trip_scheduleRelationship')} AS status, {c('trip_routeId')} AS route_id,
                     {c('trip_startTime')} AS start_time, {c('staticVersion')} AS version, {observed} AS observed,
                     filename AS src, file_row_number AS rn FROM raw)
        WHERE trip IS NOT NULL AND sd = '{service_day}' GROUP BY ALL""")
    fields = _update_fields(schema)
    f = lambda path, t: f"CAST(s.{path} AS {t})" if path in fields else f"CAST(NULL AS {t})"
    if "stopTimeUpdate" in schema.names:
        # Every version repeats the trip's stops: keep the last value per stop (and 10-minute window of its
        # scheduled time, so a train calling twice at one stop keeps both) before matching
        con.execute(f"""CREATE TABLE u AS
            SELECT row_number() OVER () AS id, * FROM (
              SELECT * FROM (
                SELECT {trip_keys}, {observed} AS observed, filename AS src, file_row_number AS rn, {f('stopSequence', 'BIGINT')} AS seq, trim({f('stopId', 'VARCHAR')}) AS stop_id,
                       {f('arrival.delay', 'BIGINT')} AS arr_d, {f('arrival.time', 'BIGINT')} AS arr_t,
                       {f('departure.delay', 'BIGINT')} AS dep_d, {f('departure.time', 'BIGINT')} AS dep_t,
                       {f('scheduleRelationship', 'VARCHAR')} AS stop_status
                FROM raw, UNNEST(raw.stopTimeUpdate) AS x(s))
              WHERE trip IS NOT NULL AND sd = '{service_day}'
              QUALIFY row_number() OVER (PARTITION BY feed, sd, trip, seq, stop_id,
                CAST(floor(coalesce(arr_t - arr_d, dep_t - dep_d, arr_t, dep_t, 0) / {CALL_WINDOW_S}) AS BIGINT)
                ORDER BY observed DESC, {LATER}) = 1)""")
    else:
        con.execute("CREATE TABLE u (id BIGINT, feed VARCHAR, sd VARCHAR, trip VARCHAR, observed DOUBLE, src VARCHAR, rn BIGINT, seq BIGINT, stop_id VARCHAR, "
                    "arr_d BIGINT, arr_t BIGINT, dep_d BIGINT, dep_t BIGINT, stop_status VARCHAR)")

    # Planned calls (stop_times) and stations (stops: a platform's parent_station) of every version in use
    stats = {"timetable_version": default_version, "stop_sequence_mode": None}
    con.execute("CREATE TABLE p (version VARCHAR, trip VARCHAR, seq BIGINT, stop_id VARCHAR, arr_ms BIGINT, dep_ms BIGINT, commercial BOOLEAN)")
    con.execute("CREATE TABLE s (version VARCHAR, stop_id VARCHAR, station VARCHAR)")
    con.execute("CREATE TABLE alias (version VARCHAR, trip VARCHAR, static VARCHAR, route_id VARCHAR)")
    versions = [r[0] for r in con.execute("SELECT version FROM t WHERE version IS NOT NULL GROUP BY 1 ORDER BY count(*) DESC, 1").fetchall()]
    stats["timetable_versions"] = versions
    undated = con.execute(f"SELECT count(*) FROM (SELECT {trip_keys} FROM raw) WHERE trip IS NOT NULL AND sd IS NULL").fetchone()[0]
    if undated:
        logger.warning("final_calls: %d rows without trip.startDate nor a date column are left out", undated)
    if not versions:
        logger.warning("final_calls: no static version for the trips (no staticVersion in the files and no "
                       "default_version): calls have no schedule")
    # One file name per version: DuckDB caches what it read by file name, and a file rewritten within the
    # same second (several versions a day) could be read from that stale cache ("ZSTD Decompression failure")
    for i, version in enumerate(versions):
        def get(table: str) -> Optional[Path]:
            dest = tmp / f"static-{i}-{table}.parquet"
            return dest if timetable(version, table, dest) else None

        # A live trip id ending in a date (…:1813:20271210) missing from the timetable, which lists the train
        # under another last part (…:1813:20261211): it takes the only trip with the same id up to that date
        # that runs on the day
        trips_path = get("trips")
        if trips_path:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE missing AS
                SELECT DISTINCT trip, regexp_replace(trip, ':[0-9]{{8}}$', '') AS prefix FROM t
                WHERE version = ? AND regexp_matches(trip, ':[0-9]{{8}}$')
                  AND trip NOT IN (SELECT trim(CAST(trip_id AS VARCHAR)) FROM read_parquet({_quote(str(trips_path))}))""", [version])
            if con.execute("SELECT count(*) FROM missing").fetchone()[0]:
                runs = []
                cd_path = get("calendar_dates")
                if cd_path:
                    con.execute(f"""CREATE OR REPLACE TEMP TABLE cd AS SELECT trim(CAST(service_id AS VARCHAR)) AS service,
                        CAST(exception_type AS INTEGER) AS kind FROM read_parquet({_quote(str(cd_path))})
                        WHERE replace(CAST(date AS VARCHAR), '-', '') = '{service_day}'""")
                    cd_path.unlink()
                    runs.append("SELECT service FROM cd WHERE kind = 1")
                cal_path = get("calendar")
                if cal_path:
                    weekday = dt.datetime.strptime(service_day, "%Y%m%d").strftime("%A").lower()
                    removed = "AND trim(CAST(service_id AS VARCHAR)) NOT IN (SELECT service FROM cd WHERE kind = 2)" if cd_path else ""
                    con.execute(f"""CREATE OR REPLACE TEMP TABLE cal AS SELECT trim(CAST(service_id AS VARCHAR)) AS service
                        FROM read_parquet({_quote(str(cal_path))}) WHERE CAST({weekday} AS INTEGER) = 1 {removed}
                          AND '{service_day}' BETWEEN replace(CAST(start_date AS VARCHAR), '-', '') AND replace(CAST(end_date AS VARCHAR), '-', '')""")
                    cal_path.unlink()
                    runs.append("SELECT service FROM cal")
                if runs:
                    con.execute(f"""INSERT INTO alias
                        SELECT ?, m.trip, any_value(s.trip), any_value(s.route_id) FROM missing m
                        JOIN (SELECT trim(CAST(trip_id AS VARCHAR)) AS trip, trim(CAST(route_id AS VARCHAR)) AS route_id,
                                     regexp_replace(trim(CAST(trip_id AS VARCHAR)), ':[0-9]{{8}}$', '') AS prefix
                              FROM read_parquet({_quote(str(trips_path))})
                              WHERE trim(CAST(service_id AS VARCHAR)) IN ({' UNION '.join(runs)})) s USING (prefix)
                        GROUP BY m.trip HAVING count(*) = 1""", [version])
            trips_path.unlink()
        path = get("stop_times")
        if path:
            st = pq.read_schema(path)
            # gtfs-parquet stores times as Duration(ms): DuckDB reads BIGINT milliseconds, or INTERVAL in some versions
            kind = con.execute(f"SELECT typeof(arrival_time) FROM read_parquet({_quote(str(path))}) LIMIT 1").fetchone() if "arrival_time" in st.names else None
            interval = bool(kind) and kind[0].startswith("INTERVAL")
            ms = lambda n: ("CAST(NULL AS BIGINT)" if n not in st.names else
                            f'CAST(epoch("{n}") * 1000 AS BIGINT)' if interval else f'CAST("{n}" AS BIGINT)')
            opt = lambda n: f'coalesce(CAST("{n}" AS INTEGER), 0)' if n in st.names else "0"
            con.execute(f"""INSERT INTO p
                SELECT ?, ids.trip, st.seq, st.stop_id, st.arr_ms, st.dep_ms, st.commercial FROM (
                  SELECT trim(CAST(trip_id AS VARCHAR)) AS static, CAST(stop_sequence AS BIGINT) AS seq, trim(CAST(stop_id AS VARCHAR)) AS stop_id,
                         {ms('arrival_time')} AS arr_ms, {ms('departure_time')} AS dep_ms,
                         NOT ({opt('pickup_type')} = 1 AND {opt('drop_off_type')} = 1) AS commercial
                  FROM read_parquet({_quote(str(path))})
                  WHERE trim(CAST(trip_id AS VARCHAR)) IN (SELECT trip FROM t UNION ALL SELECT static FROM alias WHERE version = ?)) st
                -- Filtered while reading (stop_times has tens of millions of rows for some sources), then each
                -- live id takes its timetable trip's calls: the feed can send a train under both ids
                JOIN (SELECT DISTINCT trip AS static, trip FROM t UNION ALL SELECT static, trip FROM alias WHERE version = ?) ids
                  USING (static)""", [version, version, version])
            path.unlink()
        path = get("stops")
        if path:
            parent = "nullif(trim(CAST(parent_station AS VARCHAR)), '')" if "parent_station" in pq.read_schema(path).names else "NULL"
            con.execute(f"""INSERT INTO s SELECT ?, trim(CAST(stop_id AS VARCHAR)), coalesce({parent}, trim(CAST(stop_id AS VARCHAR)))
                            FROM read_parquet({_quote(str(path))})""", [version])
            path.unlink()
    con.execute("""UPDATE t SET route_id = a.route_id FROM alias a
                   WHERE a.trip = t.trip AND a.version = t.version AND t.route_id IS NULL""")
    stats["trips_matched_by_id_before_date"] = con.execute("SELECT count(DISTINCT trip) FROM alias").fetchone()[0]
    # Some feeds keep sending a trip's old id after a new version dropped it: use the newest version that has the trip
    con.execute("""UPDATE t SET version = (SELECT max(p.version) FROM p WHERE p.trip = t.trip)
                   WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.trip = t.trip AND p.version = t.version)
                     AND EXISTS (SELECT 1 FROM p WHERE p.trip = t.trip)""")
    sds = [r[0] for r in con.execute("SELECT DISTINCT sd FROM t WHERE length(sd) = 8 AND sd ~ '^[0-9]+$'").fetchall()]
    con.execute("CREATE TABLE days (sd VARCHAR, base DOUBLE)")
    if sds:
        con.executemany("INSERT INTO days VALUES (?, ?)", [(sd, _local_noon_minus_12h(sd, tz)) for sd in sds])
    # pos: 1-based position in the trip; crank: rank among commercial calls (some feeds number only those)
    con.execute("""CREATE TABLE pc AS
        SELECT t.feed, t.sd, t.trip, t.version, p.seq, p.stop_id, coalesce(s.station, p.stop_id) AS station, p.commercial,
               row_number() OVER (PARTITION BY t.feed, t.sd, t.trip ORDER BY p.seq) AS pos,
               CASE WHEN p.commercial THEN count(*) FILTER (WHERE p.commercial)
                 OVER (PARTITION BY t.feed, t.sd, t.trip ORDER BY p.seq ROWS UNBOUNDED PRECEDING) END AS crank,
               d.base + p.arr_ms / 1000.0 AS sched_arr, d.base + p.dep_ms / 1000.0 AS sched_dep,
               d.base + coalesce(p.arr_ms, p.dep_ms) / 1000.0 AS sched
        FROM t JOIN p ON p.trip = t.trip AND p.version = t.version JOIN days d ON d.sd = t.sd
        LEFT JOIN s ON s.version = p.version AND s.stop_id = p.stop_id""")

    # Match each update to its planned call. By stop: the same stop, or another platform of the same station
    # (parent_station), nearest scheduled time first
    est = "coalesce(u.arr_t - u.arr_d, u.dep_t - u.dep_d, u.arr_t, u.dep_t)"
    con.execute(f"""CREATE TABLE m1 AS
        SELECT u.id, pc.seq AS call_seq FROM u
        JOIN pc ON pc.feed = u.feed AND pc.sd = u.sd AND pc.trip = u.trip
        LEFT JOIN s ON s.version = pc.version AND s.stop_id = u.stop_id
        WHERE u.stop_id IS NOT NULL AND (pc.stop_id = u.stop_id OR pc.station = coalesce(s.station, u.stop_id))
        QUALIFY row_number() OVER (PARTITION BY u.id ORDER BY pc.stop_id = u.stop_id DESC,
          abs(coalesce({est}, pc.sched) - pc.sched), pc.seq) = 1""")
    # By stop_sequence, for the rest: feeds number calls as the timetable does, by position, or by commercial
    # rank; the numbering kept is the one whose scheduled times fit the live ones best
    scores = {}
    for mode, key in (("full", "pc.seq"), ("position", "pc.pos"), ("commercial", "pc.crank")):
        scores[mode] = con.execute(f"""SELECT avg(CAST(EXISTS (SELECT 1 FROM pc WHERE pc.feed = u.feed AND pc.sd = u.sd AND pc.trip = u.trip
                AND {key} = u.seq AND ({est} IS NULL OR abs({est} - pc.sched) < {FIT_S})) AS INTEGER))
            FROM (SELECT * FROM u WHERE seq IS NOT NULL AND id NOT IN (SELECT id FROM m1)
                  ORDER BY hash(feed, sd, trip, seq, stop_id, observed, arr_t, dep_t, arr_d, dep_d, src, rn) LIMIT {SAMPLE_ROWS}) u""").fetchone()[0] or 0
    mode = max(scores, key=lambda m: scores[m]) if max(scores.values()) >= MIN_FIT else None
    stats["stop_sequence_mode"] = mode
    stats["stop_sequence_scores"] = {m: round(v, 3) for m, v in scores.items()}
    if mode:
        key = {"full": "pc.seq", "position": "pc.pos", "commercial": "pc.crank"}[mode]
        con.execute(f"""CREATE TABLE m2 AS SELECT u.id, pc.seq AS call_seq FROM u JOIN pc
            ON pc.feed = u.feed AND pc.sd = u.sd AND pc.trip = u.trip AND {key} = u.seq
            WHERE u.seq IS NOT NULL AND u.id NOT IN (SELECT id FROM m1)""")
    else:
        con.execute("CREATE TABLE m2 (id BIGINT, call_seq BIGINT)")
    con.execute(f"""CREATE TABLE f AS
        SELECT u.*, m.call_seq FROM u LEFT JOIN (SELECT id, call_seq, true AS by_stop FROM m1 UNION ALL SELECT id, call_seq, false FROM m2) m USING (id)
        QUALIFY row_number() OVER (PARTITION BY feed, sd, trip,
          coalesce(CAST(m.call_seq AS VARCHAR), 'x' || coalesce(CAST(u.seq AS VARCHAR), u.stop_id, ''))
          -- Of one fetch's updates for a call, the one naming its stop first, then the one numbered as the call
          ORDER BY observed DESC, m.by_stop DESC NULLS LAST, (u.seq = m.call_seq) DESC NULLS LAST, {LATER}) = 1""")
    # Of the trips found in the timetable, the updates that carry a delay or a time and found their call
    # (feeds also send en-route points with nothing in them: those are left out)
    informative = "coalesce(f.arr_d, f.dep_d, f.arr_t, f.dep_t) IS NOT NULL"
    stats["updates_matched"] = con.execute(f"""SELECT avg(CAST(call_seq IS NOT NULL AS DOUBLE)) FROM f
        WHERE {informative} AND EXISTS (SELECT 1 FROM pc WHERE pc.feed = f.feed AND pc.sd = f.sd AND pc.trip = f.trip)""").fetchone()[0]
    # Trips the feed marks ADDED (extra trains) are not in any timetable: counted apart
    added = "upper(coalesce(t.status, '')) IN ('ADDED', 'NEW')"
    stats["trips_in_timetable"], stats["trips_added"] = con.execute(f"""
        SELECT avg(CAST(EXISTS (SELECT 1 FROM pc WHERE pc.trip = t.trip AND pc.sd = t.sd) AS DOUBLE)) FILTER (NOT {added}),
               avg(CAST({added} AS DOUBLE)) FROM t""").fetchone()

    cancelled = "upper(coalesce(t.status, '')) IN ('CANCELED', 'CANCELLED', 'DELETED')"
    table = con.execute(f"""
        WITH dated AS (  -- a predicted time on the wrong day (some feeds date stops after midnight on the day
                         -- before) moves by whole days to the planned time's day
          SELECT pc.*, f.observed, f.stop_status, f.id IS NOT NULL AS listed, f.call_seq, f.arr_d AS feed_arr_d,
                 f.dep_d AS feed_dep_d,
                 f.arr_t + 86400 * coalesce(CAST(round((coalesce(pc.sched_arr, pc.sched_dep) - f.arr_t) / 86400.0) AS BIGINT), 0) AS arr_t,
                 f.dep_t + 86400 * coalesce(CAST(round((coalesce(pc.sched_dep, pc.sched_arr) - f.dep_t) / 86400.0) AS BIGINT), 0) AS dep_t
          FROM pc LEFT JOIN f ON f.feed = pc.feed AND f.sd = pc.sd AND f.trip = pc.trip AND f.call_seq = pc.seq),
        timed AS (  -- feeds that send only predicted times: the delay is the time minus the planned time. A
                    -- delay beyond ±12 h is the same day error: it loses its whole days
          SELECT * EXCLUDE (feed_arr_d, feed_dep_d),
                 coalesce(CASE WHEN abs(feed_arr_d) > 43200 THEN ((feed_arr_d % 86400) + 86400 + 43200) % 86400 - 43200
                               ELSE feed_arr_d END,
                          CAST(round(arr_t - coalesce(sched_arr, sched_dep)) AS BIGINT)) AS arr_d,
                 coalesce(CASE WHEN abs(feed_dep_d) > 43200 THEN ((feed_dep_d % 86400) + 86400 + 43200) % 86400 - 43200
                               ELSE feed_dep_d END,
                          CAST(round(dep_t - coalesce(sched_dep, sched_arr)) AS BIGINT)) AS dep_d
          FROM dated),
        planned AS (
          SELECT timed.*, min(call_seq) OVER (PARTITION BY feed, sd, trip) AS first_listed,
                 last_value(coalesce(dep_d, arr_d) IGNORE NULLS) OVER (PARTITION BY feed, sd, trip ORDER BY seq
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS carried
          FROM timed),
        running AS (
          SELECT p.feed, p.sd, p.trip, p.seq AS stop_sequence, p.stop_id, p.sched_arr, p.sched_dep,
                 CASE WHEN listed THEN arr_d ELSE carried END AS arr_d, CASE WHEN listed THEN dep_d ELSE carried END AS dep_d,
                 CASE WHEN listed THEN coalesce(arr_t, sched_arr + arr_d) ELSE sched_arr + carried END AS pred_arr,
                 CASE WHEN listed THEN coalesce(dep_t, sched_dep + dep_d) ELSE sched_dep + carried END AS pred_dep,
                 p.observed, p.stop_status, CASE WHEN listed THEN 'feed' ELSE 'propagated' END AS delay_source
          FROM planned p JOIN t USING (feed, sd, trip)
          WHERE NOT {cancelled} AND (listed OR (p.commercial AND p.seq > p.first_listed AND p.carried IS NOT NULL))),
        stopped AS (
          SELECT pc.feed, pc.sd, pc.trip, pc.seq, pc.stop_id, pc.sched_arr, pc.sched_dep, NULL, NULL, NULL, NULL, NULL, NULL, NULL
          FROM pc JOIN t USING (feed, sd, trip) WHERE {cancelled} AND pc.commercial),
        unmatched AS (
          SELECT f.feed, f.sd, f.trip, f.seq, f.stop_id, NULL, NULL, f.arr_d, f.dep_d, coalesce(f.arr_t, NULL), coalesce(f.dep_t, NULL),
                 f.observed, f.stop_status, 'feed'
          FROM f JOIN t USING (feed, sd, trip) WHERE f.call_seq IS NULL
            AND NOT EXISTS (SELECT 1 FROM pc WHERE pc.feed = f.feed AND pc.sd = f.sd AND pc.trip = f.trip AND ({cancelled} OR NOT ({informative})))),
        bare AS (  -- cancelled trains without planned calls or stop updates: one row, so they still show
          SELECT t.feed, t.sd, t.trip, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM t
          WHERE {cancelled} AND NOT EXISTS (SELECT 1 FROM pc WHERE pc.trip = t.trip AND pc.sd = t.sd)
            AND NOT EXISTS (SELECT 1 FROM f WHERE f.trip = t.trip AND f.sd = t.sd))
        SELECT x.feed AS feed_id, x.sd AS service_date, x.trip AS trip_id, x.stop_sequence, x.stop_id,
               x.sched_arr AS scheduled_arrival, x.sched_dep AS scheduled_departure, x.arr_d AS arrival_delay,
               x.dep_d AS departure_delay, x.pred_arr AS predicted_arrival, x.pred_dep AS predicted_departure,
               x.observed AS observed_at, x.stop_status AS stop_schedule_relationship, x.delay_source,
               t.status AS trip_schedule_relationship, t.route_id, t.start_time AS trip_start_time FROM (
          SELECT * FROM running UNION ALL SELECT * FROM stopped UNION ALL SELECT * FROM unmatched UNION ALL SELECT * FROM bare) x
        JOIN t USING (feed, sd, trip)
        ORDER BY feed_id, trip_id, stop_sequence NULLS LAST, stop_id, scheduled_arrival""")
    table = table.to_arrow_table() if hasattr(table, "to_arrow_table") else table.fetch_arrow_table()
    stats["propagated_calls"] = pc.sum(pc.equal(table["delay_source"], "propagated")).as_py() or 0
    return table, stats
