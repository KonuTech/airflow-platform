"""``load_config`` — merge a dataset YAML over platform defaults, then validate.

Mirrors ``tools/corpus/manifest.py``'s load -> validate -> fail-loudly shape
(`03-PATTERNS.md` Cluster D): name the file, name the field. Validation
itself is delegated to Pydantic (``DatasetConfig.model_validate``) rather
than hand-rolled ``_require_*`` helpers, but a ``pydantic.ValidationError``
is never allowed to escape this module — it is always re-raised as a
``dataplat.errors.ConfigurationError`` naming the offending path, so every
caller catches exactly one exception type for "this config is bad"
(CONTEXT.md D-06).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# PyYAML 6.0.3 ships no `py.typed`, and `types-PyYAML` is not in the dev group
# (this plan's declared file scope does not include `pyproject.toml`/`uv.lock`
# — `tools/corpus/manifest.py:45` sets the same precedent). The suppression is
# narrowed to this one import rather than a project-wide `ignore_missing_
# imports` override, so it stays visible here and disappears the moment the
# stubs are installed. Everything `yaml` returns is re-validated below by
# `DatasetConfig.model_validate` before it reaches typed code.
import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from dataplat.config.model import DatasetConfig
from dataplat.errors import ConfigurationError

if TYPE_CHECKING:
    from pathlib import Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return document or {}


def load_config(path: Path, *, defaults_path: Path) -> DatasetConfig:
    """Load, merge and validate one dataset config.

    Reads ``defaults_path`` and ``path``, shallow-merges ``path``'s document
    over ``defaults_path``'s (dataset keys win on any collision —
    ARCHITECTURE.md lines 592-594), and validates the merged mapping against
    ``DatasetConfig``.

    Args:
        path: The dataset-specific YAML file, e.g.
            ``configs/datasets/customers.yaml``.
        defaults_path: The platform-defaults YAML file merged under
            ``path``, e.g. ``configs/defaults.yaml``.

    Returns:
        The validated, frozen ``DatasetConfig``.

    Raises:
        ConfigurationError: The merged document fails ``DatasetConfig``
            validation. The error's ``context`` carries ``path`` (as
            ``str``) and Pydantic's own structured ``errors()`` list.
    """
    defaults = _load_yaml_mapping(defaults_path)
    dataset = _load_yaml_mapping(path)
    merged = {**defaults, **dataset}
    try:
        return DatasetConfig.model_validate(merged)
    except ValidationError as exc:
        msg = f"invalid dataset config at {path}: {exc}"
        raise ConfigurationError(
            msg,
            context={"path": str(path), "errors": exc.errors()},
        ) from exc
