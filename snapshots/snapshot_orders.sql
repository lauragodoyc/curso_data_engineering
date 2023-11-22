{% snapshot orders_snapshot %}
{{ 
    config(
      target_schema='snapshots',
      unique_key='id_order',
      strategy='check',
      check_cols=['id_order', 'shipping_service', 'shipping_cost', 'id_address'],
      invalidate_hard_deletes=True,
    ) 
    }}


select * from {{ ref('prueba_incremental_orders') }}

{% endsnapshot %}



