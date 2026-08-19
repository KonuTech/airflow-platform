{#
  reconciliation_orders.sql -- D-15's identical treatment as
  `reconciliation_customers.sql`, substituted for orders. See that file's
  own header comment for the full rationale (identical here,
  dataset-substituted only), including the `-- depends_on:` explicit DAG
  edge below (this test never literally calls `{{ ref('silver_orders') }}`
  in its own SQL body).
#}
-- depends_on: {{ ref('silver_orders') }}
{{ config(severity = 'warn') }}

with latest as (
    select rr.*
    from meta.reconciliation_results rr
    join meta.datasets d on d.dataset_id = rr.dataset_id
    where d.dataset_name = 'orders'
      and rr.hop = 'bronze_silver'
    order by rr.checked_at desc, rr.reconciliation_id desc
    limit 1
)

select *
from latest
where input_count != (output_count + dedup_count)
