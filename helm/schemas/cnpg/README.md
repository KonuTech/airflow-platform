# CloudNativePG CRD JSON Schemas — generated, not hand-written

Every `*.json` file in this directory is **generated** from the pinned
`cloudnative-pg` operator chart (`CNPG_OPERATOR_CHART_VERSION` in
`helm/versions.env`) by `scripts/vendor-crd-schemas.sh`, which delegates the
actual OpenAPI-to-JSON-Schema conversion to `tools/k8s/crd_to_jsonschema.py`.

They exist because `kubeconform` (CICD-07's offline manifest-validation gate)
cannot validate a `CustomResourceDefinition`-defined kind — such as CNPG's
`Cluster` — without a JSON Schema for it: CRDs are, by definition, invisible to
any built-in Kubernetes schema catalogue. `ingress-nginx` 4.15.1 ships no CRDs
at all, so CNPG is the only chart this phase vendors schemas for.

## Regenerating

```bash
scripts/vendor-crd-schemas.sh
```

Idempotent and deterministic — re-running with no chart-pin change produces
byte-identical output (`git status --porcelain helm/schemas` stays empty).

**Regenerate whenever `CNPG_OPERATOR_CHART_VERSION` moves.** The schema and
the CRD it describes must stay in lockstep; a stale schema here would let
`make manifests` silently validate a `Cluster` CR against an outdated shape.

## Filename convention

`{kind}_{version}.json`, with `{kind}` **lowercase** — e.g. `cluster_v1.json`
for `kind: Cluster`, `apiVersion: postgresql.cnpg.io/v1`. This is not a style
choice: kubeconform's `-schema-location` Go-template variable
`{{.ResourceKind}}` is the resource's `kind` field lowercased
(`pkg/registry/registry.go`'s `schemaPath`, confirmed by reading kubeconform
0.8.0's source and by running the pipeline — a schema named `Cluster_v1.json`
is silently never found).

## Do not hand-edit

Any manual edit here is overwritten the next time
`scripts/vendor-crd-schemas.sh` runs. If a schema looks wrong, fix the chart
pin or the converter, not the JSON.
