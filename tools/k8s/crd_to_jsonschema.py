"""Convert rendered CloudNativePG CRDs into per-kind, per-version JSON Schemas.

kubeconform (CICD-07) cannot validate a `CustomResourceDefinition`-defined kind
like `Cluster` without a JSON Schema for it — CRDs are, by definition, invisible
to any built-in Kubernetes schema catalogue. This module performs the missing
half of the pipeline offline: given a rendered stream of
`CustomResourceDefinition` documents (as produced by
`helm template cnpg cnpg/cloudnative-pg`), it lifts each version's
`spec.versions[].schema.openAPIV3Schema`, strips the Kubernetes-specific
`x-kubernetes-*` vendor-extension keys that are not valid JSON Schema keywords,
declares the same `$schema` kubeconform's own upstream catalogue uses, and
writes one file per kind-and-version.

## Why the filename is lowercase

kubeconform's `-schema-location` Go-template variable `{{.ResourceKind}}` is the
resource's `kind` field **lowercased** — `pkg/registry/registry.go`'s
`schemaPath` builds its template data with `ResourceKind: strings.ToLower(
resourceKind)`, confirmed by reading that source at pin time (kubeconform
0.8.0) and by running the exact pipeline this module feeds: a schema file
named `Cluster_v1.json` is silently never found (`could not find schema for
Cluster`, no hint that case is the problem); `cluster_v1.json` is found and
validates. `{kind.lower()}_{version}.json` is therefore not a style choice —
it is the one filename kubeconform will actually request.

## Why this is a hand-written converter, not a vendored one

`02-07-PLAN.md` Task 1 records the deliberation: vendoring
`openapi2jsonschema.py` from the kubeconform repository would add an unlinted
third-party script to a `mypy --strict` / `ruff select=["ALL"]` tree, buying a
permanent `pyproject.toml` exclusion. The property that actually matters —
schema and CRD staying in lockstep — comes from regenerating from the *pinned
chart* (`scripts/vendor-crd-schemas.sh`), not from the identity of the
converter. This module is well under the plan's forty-line budget for real
logic and passes the repository's own gates unmodified.

Usage:
    python3 tools/k8s/crd_to_jsonschema.py <crds.yaml> <output-dir>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

# The suppression is deliberately narrowed to this one import rather than added
# as a project-wide `ignore_missing_imports` override (mirrors
# tools/corpus/manifest.py) — everything `yaml` returns is re-validated below
# (schemas_for_crd's required-field checks) before it reaches a written file,
# so the untyped boundary is one line wide.
import yaml  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

logger = logging.getLogger(__name__)

# kubeconform's own upstream catalogue (yannh/kubernetes-json-schema) declares
# exactly this draft string on every schema file it serves — matched here so a
# vendored schema and a catalogue-downloaded one are declared identically.
_JSON_SCHEMA_DRAFT = "http://json-schema.org/schema#"

# Not valid JSON Schema keywords — Kubernetes/OpenAPI vendor extensions
# (x-kubernetes-preserve-unknown-fields, x-kubernetes-int-or-string,
# x-kubernetes-list-type, x-kubernetes-validations, ...) that a strict JSON
# Schema consumer has no reason to see.
_VENDOR_EXTENSION_PREFIX = "x-kubernetes-"


class CrdConversionError(ValueError):
    """A `CustomResourceDefinition` document could not be converted."""


def _strip_vendor_extensions(node: Any) -> Any:
    """Recursively drop `x-kubernetes-*` keys from a parsed JSON-like structure.

    Args:
        node: A parsed YAML/JSON value — mapping, sequence, or scalar.

    Returns:
        The same structure with every `x-kubernetes-*` mapping key removed.
    """
    if isinstance(node, dict):
        return {
            key: _strip_vendor_extensions(value)
            for key, value in node.items()
            if not key.startswith(_VENDOR_EXTENSION_PREFIX)
        }
    if isinstance(node, list):
        return [_strip_vendor_extensions(item) for item in node]
    return node


def iter_crds(documents: Iterable[Any]) -> Iterator[dict[str, Any]]:
    """Filter a parsed multi-document YAML stream down to CRDs.

    Args:
        documents: Parsed YAML documents, as produced by `yaml.safe_load_all`.

    Yields:
        Every document whose `kind` is `CustomResourceDefinition`. A blank
        document (a bare `---` separator, or a `# comment`-only document)
        parses to `None` and is silently skipped.
    """
    for document in documents:
        if document and document.get("kind") == "CustomResourceDefinition":
            yield document


def schemas_for_crd(crd: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build one JSON Schema per declared version of a single CRD.

    Args:
        crd: A parsed `CustomResourceDefinition` document.

    Returns:
        Output filename (`{kind}_{version}.json`, lowercase kind — see the
        module docstring) mapped to the converted JSON Schema document.

    Raises:
        CrdConversionError: If the CRD names no kind/versions, or a version
            declares no `schema.openAPIV3Schema`. Silently emitting nothing
            for a broken version would leave a schema directory that looks
            complete but validates nothing for it.
    """
    kind = crd.get("spec", {}).get("names", {}).get("kind")
    versions = crd.get("spec", {}).get("versions") or []
    crd_name = crd.get("metadata", {}).get("name", "<unnamed>")
    if not kind or not versions:
        msg = f"{crd_name}: CRD document has no spec.names.kind or spec.versions"
        raise CrdConversionError(msg)

    schemas: dict[str, dict[str, Any]] = {}
    for version in versions:
        version_name = version.get("name")
        openapi_schema = (version.get("schema") or {}).get("openAPIV3Schema")
        if not version_name or not openapi_schema:
            msg = (
                f"{crd_name}: {kind} version {version_name!r} has no "
                "schema.openAPIV3Schema — refusing to write a schema file that "
                "would silently validate nothing for it"
            )
            raise CrdConversionError(msg)

        converted = _strip_vendor_extensions(openapi_schema)
        converted["$schema"] = _JSON_SCHEMA_DRAFT
        schemas[f"{kind.lower()}_{version_name}.json"] = converted

    return schemas


def convert(input_path: Path, output_dir: Path) -> list[Path]:
    """Convert every CRD in a multi-document YAML stream into JSON Schema files.

    Args:
        input_path: A YAML file of one or more Kubernetes documents — typically
            the full `helm template` output of a CRD-bearing chart; non-CRD
            documents are silently skipped.
        output_dir: Directory to write `{kind}_{version}.json` files into.
            Created if it does not already exist.

    Returns:
        The sorted list of files written, for the caller to report or verify.

    Raises:
        CrdConversionError: If the input contains no CRD documents at all.
    """
    documents = yaml.safe_load_all(input_path.read_text(encoding="utf-8"))
    crds = list(iter_crds(documents))
    if not crds:
        msg = f"{input_path}: no CustomResourceDefinition documents found"
        raise CrdConversionError(msg)

    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for crd in crds:
        for filename, schema in schemas_for_crd(crd).items():
            destination = output_dir / filename
            # Sorted keys + a single trailing newline: deterministic,
            # byte-identical output on every regeneration — the property
            # scripts/vendor-crd-schemas.sh's `git status --porcelain` check
            # depends on.
            destination.write_text(
                json.dumps(schema, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            written.append(destination)

    return sorted(written)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser.

    Returns:
        The configured parser for the CRD-to-JSON-Schema conversion.
    """
    parser = argparse.ArgumentParser(
        prog="crd_to_jsonschema",
        description=(
            "Convert rendered CustomResourceDefinition documents into per-kind, "
            "per-version JSON Schema files for kubeconform's -schema-location."
        ),
    )
    parser.add_argument("input", type=Path, help="YAML file of CRD documents")
    parser.add_argument("output_dir", type=Path, help="directory to write schema files into")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CRD-to-JSON-Schema conversion as a script.

    Args:
        argv: Argument vector, defaulting to `sys.argv[1:]`.

    Returns:
        Process exit status: 0 on success, 1 if conversion failed.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)
    try:
        written = convert(args.input, args.output_dir)
    except CrdConversionError:
        logger.exception("failed to convert %s", args.input)
        return 1
    logger.info("wrote %d schema file(s) to %s", len(written), args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
