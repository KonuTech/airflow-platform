"""``DatasetConfig`` — the pydantic model every ``configs/datasets/*.yaml`` validates against.

Every model in this module is ``ConfigDict(extra="forbid", frozen=True)``
(CLAUDE.md's "Data modelling / validation" table, `03-PATTERNS.md` Cluster
E): ``extra="forbid"`` turns a config typo into a validation-time error
instead of a silently-ignored key, and ``frozen=True`` supports the
determinism constraint (PROJECT.md Constraints) by making a resolved config
immutable for the lifetime of a run.

Strategy/source fields (``SourceConfig.type``, ``DeduplicationConfig.strategy``,
``LoadConfig.strategy``) are plain ``str``, resolved through string-keyed
registries elsewhere (ARCHITECTURE.md Q4.4 line 523) — never a Python
``Enum`` baked into this model. That indirection is what makes "config not
code" (README §65) literally true: a new source or publisher strategy is a
registry entry, not a change to this file.

This model intentionally carries no ``delimiter``/``decimal_separator``
collision validator: no such fields exist on ``DatasetConfig`` today, and a
``model_validator`` with no reachable raise site is dead code wearing a
design decision's clothes (CONTEXT.md D-06's reasoning, applied here too).
Phase 6, which introduces ``delimiter``/``decimal_separator`` fields, must
add the STACK.md §15 "do not confuse CSV delimiters with decimal separators"
collision check at that point — not before.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SourceConfig(BaseModel):
    """Where a dataset's source files live and how change is signaled.

    Attributes:
        type: Source engine key resolved through ``SOURCE_REGISTRY``, e.g.
            ``"csv"``. A string, never an enum — see the module docstring.
        bucket: Object-store bucket the source files arrive in.
        path: Prefix within ``bucket`` the source discovers files under.
        change_semantics: How the source signals change, e.g.
            ``"snapshot"`` or ``"cdc"``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: str
    bucket: str
    path: str
    change_semantics: str


class DeduplicationConfig(BaseModel):
    """How a dataset's deduplication stage collapses duplicate records.

    Attributes:
        strategy: Deduplication strategy key resolved through
            ``DEDUP_REGISTRY``, e.g. ``"business_key_latest"``.
        keys: Business-key column names that identify one logical record.
        order_by: Column expressions (e.g. ``"event_ts desc"``) that decide
            which duplicate wins.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: str
    keys: list[str]
    order_by: list[str]


class LoadConfig(BaseModel):
    """How a dataset's records are published to their target table.

    Attributes:
        strategy: Publisher strategy key resolved through
            ``PUBLISHER_REGISTRY``, e.g. ``"merge"``.
        target: Fully-qualified target table, e.g.
            ``"normalized.customers"``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: str
    target: str


class DatasetConfig(BaseModel):
    """The complete, validated configuration for one dataset.

    Every ``configs/datasets/<name>.yaml``, merged over
    ``configs/defaults.yaml``, must validate against this model with zero
    errors (``dataplat.config.loader.load_config``). ``extra="forbid"``
    rejects an unknown top-level key at validation time; ``frozen=True``
    rejects any post-construction mutation.

    Attributes:
        dataset: The dataset's unique name, matching ``meta.datasets.dataset_name``.
        config_schema_version: Version of this model's *shape* — lets
            ``loader.py`` migrate an older document into a newer model when
            replaying a historical run (ARCHITECTURE.md Q5.2).
        source: Where and how source files arrive.
        deduplication: How duplicate records are collapsed.
        load: How records are published to their target table.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str
    config_schema_version: int
    source: SourceConfig
    deduplication: DeduplicationConfig
    load: LoadConfig
