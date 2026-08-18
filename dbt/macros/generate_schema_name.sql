{#
  generate_schema_name.sql -- unconditionally return target.schema (Pitfall 3,
  08.1-RESEARCH.md).

  dbt's own default `generate_schema_name` macro concatenates
  `{{ target.schema }}_{{ custom_schema_name }}` whenever a model/folder
  declares a `+schema:` config override -- a reasonable default for dbt's
  typical multi-developer use case, but wrong for this project's single
  deliberate schema name: it would silently produce `silver_silver`
  instead of `silver`. This override makes that concatenation structurally
  impossible: every model lands directly in `target.schema` (`silver`,
  set once in `profiles.yml`'s target) no matter what any model or
  `dbt_project.yml` entry configures.

  `custom_schema_name`/`node` are accepted (dbt always calls this macro with
  exactly this two-argument signature) but deliberately never referenced in
  the returned value -- ignoring them, not merely defaulting them, is the
  fix.
#}
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ target.schema }}
{% endmacro %}
