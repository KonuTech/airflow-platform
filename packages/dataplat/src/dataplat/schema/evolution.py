"""``classify_schema_change`` -- the dlt 3x4-matrix compatible/breaking classifier (SCHEMA-04/05).

Adopts dlt's 3x4 schema-contract matrix (``{tables, columns, data_type}`` x
``{evolve, freeze, discard_row, discard_value}``) rather than a boolean
evolve/freeze flag -- ROADMAP.md's Phase 6 plan guidance. This module
implements the ``columns``/``data_type`` half of that matrix as a single
pure, DB-independent function comparing two column lists: the dataset's
currently-recorded/contract schema (``old_columns``) against one file's
observed schema (``new_columns``).

Per this phase's locked decisions (``06-CONTEXT.md``):

- **D-01**: a COMPATIBLE change (a new column appears) is detect + record
  only, never auto-DDL -- classified compatible and returned as a
  ``SchemaChangeFinding`` value, never raised. The new column's values are
  not persisted anywhere until a human adds a real Alembic migration and
  updates the contract; this function only classifies, it never migrates.
- **D-02**: a BREAKING change (business-key rename, column disappearance,
  or data-type retype) makes the whole file fail, nothing loads -- raises
  ``IncompatibleSchemaError`` (``dataplat.errors``) before any row is
  staged. "Reported... never silently adapted to" is taken literally: no
  row from a breaking file reaches the target table under a guessed
  mapping.
- **D-04**: a column disappearing and a column's data type changing are
  both classified breaking (freeze) by default, the same treatment as a
  rename -- a rename is structurally indistinguishable from "the old name
  disappeared AND a new name coincidentally appeared", so disappearance
  dominates and the coincidental appearance never rescues the file. Only
  "a genuinely new column appears" is evolve; every other structural
  change freezes the file.

SCHEMA-05 ("drift... detected and reported, never silently adapted to") is
satisfied by construction: every BREAKING classification raises before any
row is staged, and every raise carries a ``diagnostic_code`` plus the
offending column name (and both types, for a retype) in ``context`` --
T-06-31's mitigation -- so a breaking classification is diagnosable without
reading source code.

**D-05** (a breaking classification in one file's task never blocks sibling
files in the same batch/run) holds structurally, not by any run-level
gating logic: ``classify_schema_change`` is called PER FILE and is pure --
no globals, no caching, no I/O, no shared mutable state across calls. Two
back-to-back calls, one breaking and one compatible, can never contaminate
each other; this is a property to document and test, not code to write --
see ``tests/unit/schema/test_evolution.py``'s dedicated D-05 proof test.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataplat.errors import IncompatibleSchemaError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class SchemaChangeFinding:
    """One COMPATIBLE schema change: a proposal, never an exception (D-01).

    Mirrors ``dataplat.models.record.RejectedRecord``'s exact
    ``@dataclass(frozen=True, slots=True)`` shape -- a compatible schema
    change is data, not an exception, matching QUAL-03's errors-as-values
    convention for row-level problems, applied here at schema level instead.

    Attributes:
        change_type: A short, stable, machine-readable reason code. Always
            ``"column_added"`` this phase -- D-04's "only a genuinely new
            column appears is evolve" leaves no other compatible outcome to
            name yet.
        column: The name of the column this finding is about.
        message: A human-readable description of the change.
    """

    change_type: str
    column: str
    message: str


def classify_schema_change(
    old_columns: Sequence[Mapping[str, object]],
    new_columns: Sequence[Mapping[str, object]],
) -> list[SchemaChangeFinding]:
    """Classify a dataset's schema drift as COMPATIBLE findings or a BREAKING raise.

    Compares ``old_columns`` (the dataset's currently-recorded/contract
    schema) against ``new_columns`` (this file's observed schema) by column
    ``name`` -- never by position, so a column list that is merely
    reordered, with every name and type otherwise unchanged, is correctly
    treated as no change AT THE CLASSIFICATION LEVEL this function owns.

    This is deliberately not the whole story end to end: the platform's
    loader (``dataplat.load.staging.StagingLoader``) maps a row's fields to
    its target columns by POSITION alone, with no header-to-contract name
    remapping anywhere in the codebase, so a reordered file cannot actually
    be loaded correctly even though this classifier alone would call it
    COMPATIBLE. The caller that owns both facts --
    ``csv_processor.source.CsvSource._resolve_schema`` -- is responsible for
    rejecting a reordered file itself (``diagnostic_code
    == "schema-columns-reordered"``) before ever reaching this function's
    "no change" outcome for that case in practice; this function's own
    contract and tests are unchanged.

    Breaking conditions are checked for every column in ``old_columns``, in
    the given order, BEFORE any compatible finding is ever returned (D-02's
    dominance rule: a compatible addition never partially rescues a
    breaking file). Because a disappearance and a retype are checked
    per-column and a column cannot be both, the only real ordering question
    is which of two *different* old columns raises first when more than one
    is breaking -- that is decided by ``old_columns``' own iteration order,
    a deterministic, caller-controlled tie-break, documented here rather
    than left implicit.

    Args:
        old_columns: The dataset's currently-recorded/contract column list.
            Each mapping must carry a ``"name"`` key; a ``"type"`` key, when
            present, is compared for the retype check below.
        new_columns: This file's observed column list, same shape as
            ``old_columns``.

    Returns:
        A list of ``SchemaChangeFinding`` values, one per genuinely new
        column present in ``new_columns`` but absent from ``old_columns``.
        Empty when there is no change at all. This return only happens
        after every ``old_columns`` entry has cleared the breaking check
        below -- a findings list is never returned on a path that also
        raises.

    Raises:
        IncompatibleSchemaError: A column present in ``old_columns`` is
            absent from ``new_columns`` (``context["diagnostic_code"] ==
            "schema-column-disappeared"``, ``context["column"]`` names it),
            or a column present in both changed its ``"type"``
            (``context["diagnostic_code"] == "schema-column-retyped"``,
            with ``context["column"]``, ``context["old_type"]`` and
            ``context["new_type"]``).
    """
    new_by_name: dict[str, Mapping[str, object]] = {
        str(column["name"]): column for column in new_columns
    }

    for old_column in old_columns:
        name = str(old_column["name"])
        new_column = new_by_name.get(name)
        if new_column is None:
            msg = f"column {name!r} present in the recorded schema disappeared from this file"
            raise IncompatibleSchemaError(
                msg,
                context={"diagnostic_code": "schema-column-disappeared", "column": name},
            )
        old_type = old_column.get("type")
        new_type = new_column.get("type")
        if old_type != new_type:
            msg = f"column {name!r} retyped from {old_type!r} to {new_type!r}"
            raise IncompatibleSchemaError(
                msg,
                context={
                    "diagnostic_code": "schema-column-retyped",
                    "column": name,
                    "old_type": old_type,
                    "new_type": new_type,
                },
            )

    old_names = {str(column["name"]) for column in old_columns}
    return [
        SchemaChangeFinding(
            change_type="column_added",
            column=str(column["name"]),
            message=f"column {column['name']!r} is new (D-01: detect + record only)",
        )
        for column in new_columns
        if str(column["name"]) not in old_names
    ]
