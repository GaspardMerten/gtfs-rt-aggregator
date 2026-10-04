from typing import TYPE_CHECKING

# Loaded on first use: importing a light module (e.g. punctuality) then skips the pipeline's dependencies
_PIPELINE = ("run_pipeline", "run_pipeline_from_toml", "load_config_from_toml")
__all__ = list(_PIPELINE)

if TYPE_CHECKING:
    from .pipeline import load_config_from_toml, run_pipeline, run_pipeline_from_toml


def __getattr__(name):
    if name in _PIPELINE:
        from . import pipeline

        return getattr(pipeline, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_PIPELINE))
