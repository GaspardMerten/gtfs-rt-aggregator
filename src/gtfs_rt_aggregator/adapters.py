"""
Adapters: Python functions as the source of a feed that is not a GTFS-RT or
GTFS URL (e.g. a JSON API polled with its own change ids).

    [[providers.realtime]]
    adapter = "adapters/trafikverket.py:fetch"    # file (relative to the config file) or module:function

    def fetch(state: dict, env: Mapping[str, str]) -> FeedMessage | bytes

    [[providers.static]]
    adapter = "adapters/trafikverket.py:timetable"

    def timetable(state: dict, env: Mapping[str, str], out_dir: Path) -> Path   # a GTFS zip or folder

state is the adapter's own JSON-serialisable dict, kept between calls and
across restarts (spool/state/<feed>.adapter.json), and saved only after a
successful call. env is os.environ. Adapters run in the worker processes;
a file is loaded once per process, and again when it changes.
"""

import copy
import hashlib
import importlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

# Loaded adapter files: path -> (modification time, module)
_LOADED: Dict[str, Tuple[int, Any]] = {}


def parse_spec(spec: str) -> Tuple[str, str]:
    """(file or module, function) of "path/to/file.py:function" or "module:function"."""
    target, sep, function = spec.rpartition(":")
    if not sep or not target or not function or not function.isidentifier():
        raise ValueError(
            f"Invalid adapter {spec!r}: expected 'path/to/file.py:function' or 'module:function'"
        )
    return target, function


def load(spec: str, base_dir: Optional[str] = None) -> Callable:
    """The adapter function; a relative file path starts from base_dir."""
    target, function = parse_spec(spec)
    if target.endswith(".py"):
        path = Path(target)
        if not path.is_absolute():
            path = Path(base_dir or os.getcwd()) / path
        path = path.resolve()
        mtime = path.stat().st_mtime_ns
        loaded = _LOADED.get(str(path))
        if loaded is None or loaded[0] != mtime:
            name = (
                "gtfs_rt_aggregator_adapter_"
                + hashlib.sha1(str(path).encode()).hexdigest()[:12]
            )
            module_spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(module_spec)
            # Its folder, for the helper modules it imports
            if str(path.parent) not in sys.path:
                sys.path.insert(0, str(path.parent))
            sys.modules[name] = module
            module_spec.loader.exec_module(module)
            loaded = _LOADED[str(path)] = (mtime, module)
        module = loaded[1]
    else:
        module = importlib.import_module(target)
    try:
        return getattr(module, function)
    except AttributeError:
        raise ValueError(f"Adapter {spec!r}: no function {function} in {target}")


class AdapterState:
    """An adapter's state file: its own dict, and when it last ran."""

    def __init__(self, path: Path):
        self.path = Path(path)
        try:
            saved = json.loads(self.path.read_text())
        except (OSError, ValueError):
            saved = {}
        self.state: Dict[str, Any] = saved.get("state", {})
        self.last_run: Optional[datetime] = (
            datetime.fromisoformat(saved["last_run"]) if saved.get("last_run") else None
        )

    def working_copy(self) -> Dict[str, Any]:
        """A copy for the adapter: a call that fails leaves the saved state as it was."""
        return copy.deepcopy(self.state)

    def save(self, state: Dict[str, Any]):
        """Keep state after a successful call (written atomically)."""
        try:
            data = json.dumps(
                {"state": state, "last_run": datetime.now(timezone.utc).isoformat()}
            )
        except (TypeError, ValueError) as e:
            raise TypeError(f"Adapter state is not JSON-serialisable: {e}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + f".tmp-{os.getpid()}")
        tmp.write_text(data)
        os.replace(tmp, self.path)
        self.state = state
        self.last_run = datetime.now(timezone.utc)


def fetch_realtime(spec: str, base_dir: Optional[str], state: Dict) -> bytes:
    """One poll of a realtime adapter, as GTFS-RT bytes."""
    result = load(spec, base_dir)(state, os.environ)
    if isinstance(result, (bytes, bytearray)):
        return bytes(result)
    if hasattr(result, "SerializeToString"):
        # Deterministic: the same content gives the same bytes (skip_unchanged)
        return result.SerializeToString(deterministic=True)
    raise TypeError(
        f"Adapter {spec!r} returned {type(result).__name__}, expected a FeedMessage or bytes"
    )


def build_static(
    spec: str, base_dir: Optional[str], state: Dict, out_dir: Path
) -> Path:
    """A static adapter's GTFS, as a zip in out_dir (a folder it returns is zipped)."""
    import zipfile

    produced = load(spec, base_dir)(state, os.environ, Path(out_dir))
    if produced is None:
        raise TypeError(
            f"Adapter {spec!r} returned nothing, expected a GTFS zip or folder"
        )
    produced = Path(produced)
    if produced.is_dir():
        zip_path = Path(out_dir) / "feed.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
            for file in sorted(produced.glob("*.txt")):
                archive.write(file, file.name)
        return zip_path
    if not produced.is_file():
        raise FileNotFoundError(
            f"Adapter {spec!r} returned {produced}, which does not exist"
        )
    if produced.resolve().parent != Path(out_dir).resolve():
        # Work files are written next to the zip: keep them in out_dir
        import shutil

        shutil.copyfile(produced, Path(out_dir) / "feed.zip")
        return Path(out_dir) / "feed.zip"
    return produced
