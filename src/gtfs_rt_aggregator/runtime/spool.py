"""
On-disk queue between the fetch threads, the worker processes and the upload
thread. Every step renames or replaces files, so a crash at any point leaves
either the old or the new state, never a half-written file.

    incoming/<feed>/<fetch time>.pb (+ .json)   fetched, waiting for a worker
    processing/<feed>/<fetch time>.pb (+ .json)  being processed
    quarantine/<feed>/...                        failed max_attempts times
    windows/<feed>/<service>/<window>/part-*.parquet
                                                 accumulate_minutes blocks being filled
    ready/<provider>/<storage path>              waiting to be uploaded
    static/<feed>/<fetch time>/feed.zip (+ meta.json)
                                                 static downloads, waiting for a heavy worker
    raw/<feed>/<date>/<hour>/...                 processed fetches waiting to be archived
    state/<feed>.json                            per-feed state (previous snapshot)
"""

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

# Written by a shutdown that went to the end (see recover)
CLEAN_SHUTDOWN = "clean-shutdown"

SUBDIRS = (
    "incoming",
    "processing",
    "quarantine",
    "windows",
    "ready",
    "static",
    "raw",
    "state",
)


def item_name(fetch_time: datetime) -> str:
    """File name of a fetch: UTC with microseconds, so names sort by time."""
    return fetch_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def write_atomic(path: Path, data: bytes):
    """Write through a temporary file, then rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def write_json(path: Path, data: dict):
    write_atomic(path, json.dumps(data, indent=2, default=str).encode("utf-8"))


def read_json(path: Path) -> Optional[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


class Spool:
    def __init__(self, root: str):
        self.root = Path(root)
        for sub in SUBDIRS:
            (self.root / sub).mkdir(parents=True, exist_ok=True)

    def path(self, *parts: str) -> Path:
        return self.root.joinpath(*parts)

    # Realtime fetches ---------------------------------------------------

    def new_item(self, feed: str, fetch_time: datetime) -> Path:
        """Path to download a fetch to (then call commit_item)."""
        path = self.path("incoming", feed, item_name(fetch_time) + ".pb")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def commit_item(self, item: Path, tmp: Path, meta: dict):
        """Make a downloaded fetch visible to the workers: sidecar first."""
        write_json(item.with_suffix(".json"), meta)
        os.replace(tmp, item)

    def claim(self, item: Path) -> Path:
        """
        Move a fetch to processing/. Returns its new path. The .pb moves
        first: if the process dies in between, recover() finds it in
        processing/ and fetches its sidecar from incoming/.
        """
        target = self.path("processing", item.parent.name, item.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(item, target)
        os.replace(item.with_suffix(".json"), target.with_suffix(".json"))
        return target

    def meta(self, item: Path) -> dict:
        return read_json(item.with_suffix(".json")) or {}

    def release(
        self,
        item: Path,
        error: str,
        max_attempts: int,
        count_attempt: bool = True,
    ) -> bool:
        """
        A fetch failed: back to incoming/ for another try, or to quarantine/
        after max_attempts. With count_attempt False (not its fault), no
        attempt is counted. Returns True if quarantined.
        """
        meta = self.meta(item)
        if count_attempt:
            meta["attempt"] = meta.get("attempt", 1) + 1
        meta["last_error"] = error
        quarantined = meta.get("attempt", 1) > max_attempts
        target = self.path(
            "quarantine" if quarantined else "incoming", item.parent.name, item.name
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        write_json(target.with_suffix(".json"), meta)
        os.replace(item, target)
        item.with_suffix(".json").unlink(missing_ok=True)
        return quarantined

    def done(self, item: Path, archive: bool):
        """A fetch was processed: delete it, or keep it for the raw archive."""
        if archive:
            meta = self.meta(item)
            hour = item.name[:11]  # YYYYMMDDTHH
            target = self.path("raw", item.parent.name, hour[:8], hour[9:11], item.name)
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(item, target)
            write_json(target.with_suffix(".json"), meta)
            item.with_suffix(".json").unlink(missing_ok=True)
        else:
            item.unlink(missing_ok=True)
            item.with_suffix(".json").unlink(missing_ok=True)

    # State --------------------------------------------------------------

    def state(self, feed: str) -> dict:
        return read_json(self.path("state", feed + ".json")) or {}

    def save_state(self, feed: str, state: dict):
        write_json(self.path("state", feed + ".json"), state)

    # Results to upload ----------------------------------------------------

    def ready_path(self, provider: str, storage_path: str) -> Path:
        return self.path("ready", provider, *storage_path.split("/"))

    def put_ready(self, provider: str, storage_path: str, data: bytes):
        write_atomic(self.ready_path(provider, storage_path), data)

    def ready_files(self) -> Iterator[Path]:
        """Files waiting to be uploaded (names hold their time: sorted by name)."""
        found = []
        for dirpath, _, filenames in os.walk(self.path("ready")):
            found.extend(
                os.path.join(dirpath, name) for name in filenames if ".tmp-" not in name
            )
        return iter(Path(p) for p in sorted(found))

    def queued_items(self) -> Dict[str, List[Path]]:
        """Fetches waiting in incoming/, per feed, oldest first (scanned once at start)."""
        result: Dict[str, List[Path]] = {}
        for folder in self.path("incoming").iterdir():
            if folder.is_dir():
                items = sorted(
                    p for p in folder.glob("*.pb") if p.with_suffix(".json").exists()
                )
                if items:
                    result[folder.name] = items
        return result

    # Recovery and size ------------------------------------------------------

    def mark_clean_shutdown(self):
        (self.root / CLEAN_SHUTDOWN).touch()

    def recover(self, max_attempts: int) -> Dict[str, int]:
        """
        After a crash or restart: fetches left in processing/ go back to
        incoming/ (counting an attempt after a crash, not after a clean
        shutdown), temporary files are deleted.
        """
        marker = self.root / CLEAN_SHUTDOWN
        clean = marker.exists()
        marker.unlink(missing_ok=True)
        counts = {"requeued": 0, "quarantined": 0, "tmp_removed": 0}
        # A claim interrupted between its two renames: sidecar still in incoming/
        for item in self.path("processing").glob("*/*.pb"):
            sidecar = self.path("incoming", item.parent.name, item.stem + ".json")
            if not item.with_suffix(".json").exists() and sidecar.exists():
                os.replace(sidecar, item.with_suffix(".json"))
        # Sidecars whose fetch is gone
        for sidecar in self.path("incoming").glob("*/*.json"):
            if not sidecar.with_suffix(".pb").exists():
                sidecar.unlink(missing_ok=True)
        for item in sorted(self.path("processing").glob("*/*.pb")):
            if self.release(
                item,
                "interrupted (restart)",
                max_attempts,
                count_attempt=not clean,
            ):
                counts["quarantined"] += 1
            else:
                counts["requeued"] += 1
        for tmp in list(self.root.rglob("*.tmp-*")) + list(
            self.root.rglob("*.pb.part")
        ):
            if tmp.is_file():
                tmp.unlink(missing_ok=True)
                counts["tmp_removed"] += 1
        for folder in self.path("static").glob("*/*"):
            if folder.is_dir() and not (folder / "meta.json").exists():
                shutil.rmtree(folder, ignore_errors=True)
                counts["tmp_removed"] += 1
        return counts

    def size_bytes(self) -> int:
        """Size of the spool, quarantine included: it only drains by hand."""
        total = 0
        for dirpath, dirnames, filenames in os.walk(self.root):
            for name in filenames:
                try:
                    total += os.stat(os.path.join(dirpath, name)).st_size
                except OSError:
                    pass
        return total

    def quarantined(self) -> int:
        """Fetches and static downloads in quarantine/."""
        quarantine = self.path("quarantine")
        return sum(1 for _ in quarantine.glob("*/*.pb")) + sum(
            1 for _ in quarantine.glob("static/*/*/meta.json")
        )
