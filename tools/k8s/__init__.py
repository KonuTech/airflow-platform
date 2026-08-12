"""Kubernetes-tooling helpers: pinned installers and the offline manifest gate.

Modules here are linted and type-checked exactly like library code, the same
convention `tools/security/__init__.py` established: `crd_to_jsonschema.py`
implements a real control (CICD-07's manifest-validation gate cannot see a
CustomResourceDefinition-defined kind without it) rather than being a loose,
exempted script.
"""
