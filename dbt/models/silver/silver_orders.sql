{#
  silver_orders.sql -- D-15's identical mechanism to silver_customers.sql,
  substituted for orders (order_id business key; orders carries no
  event_ts column at all, so the tie-break's secondary consideration is
  order_date, per orders.yaml's own deduplication.order_by: [order_date
  desc] and merge_orders.py's own `ORDER BY order_id, order_date DESC,
  _source_row_number DESC` precedent, mirrored here verbatim).

  See silver_customers.sql's own header comment for the full design
  rationale (is_incremental()/_run_id watermark, D-06/D-07 layering,
  late-arrival-vs-existing-silver ranking, _dbt_loaded_at) -- identical
  here, dataset-substituted only.
#}

{% set watermark_floor = 0 %}
{% if is_incremental() %}
  {% set watermark_floor_query %}
    select coalesce(max(_run_id), 0) from {{ this }}
  {% endset %}
  {% set watermark_floor = run_query(watermark_floor_query).columns[0].values()[0] | int %}
{% endif %}

{#-
  `post_hook_sql` is pre-rendered here, eagerly, in the model's own primary
  render pass, and captured as a fully-substituted static SQL string
  before being handed to `config(post_hook=...)` -- see
  silver_customers.sql's own header comment for the full, empirically-
  verified rationale (both the `{% set %}`-locals-not-carried-into-a-
  deferred-hook-render finding and the `this`-not-yet-alias-resolved
  finding).
-#}
{% set post_hook_sql %}
{{ dedup_audit_post_hook(
    dataset_name='orders',
    business_key_column='order_id',
    source_schema='staging',
    source_identifier='orders',
    target_schema=target.schema,
    target_identifier='orders'
) }}
{% endset %}

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    contract={'enforced': True},
    alias='orders',
    post_hook=post_hook_sql
) }}

with new_bronze as (
    select
        order_id, customer_id, order_date, amount,
        _run_id, _file_id, _batch_id, _source_row_number,
        _record_hash, _record_hash_version
    from {{ source('bronze', 'orders') }}
    {% if is_incremental() %}
    where _run_id > {{ watermark_floor }}
    {% endif %}
),

existing_silver_contenders as (
    {% if is_incremental() %}
    select
        order_id, customer_id, order_date, amount,
        _run_id, _file_id, _batch_id, _source_row_number,
        _record_hash, _record_hash_version
    from {{ this }}
    where order_id in (select order_id from new_bronze)
    {% else %}
    select
        null::text as order_id, null::text as customer_id, null::text as order_date,
        null::text as amount,
        null::bigint as _run_id, null::bigint as _file_id, null::bigint as _batch_id,
        null::bigint as _source_row_number, null::bytea as _record_hash,
        null::smallint as _record_hash_version
    where false
    {% endif %}
),

all_contenders as (
    select * from new_bronze
    union all
    select * from existing_silver_contenders
),

business_key_ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by order_date::date desc nulls last, _source_row_number desc, _file_id desc
        ) as rn
    from all_contenders
)

select
    order_id, customer_id, order_date, amount,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version,
    now() as _dbt_loaded_at
from business_key_ranked
where rn = 1
