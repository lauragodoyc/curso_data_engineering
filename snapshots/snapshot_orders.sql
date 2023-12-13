{% snapshot snapshot_orders %}
{{ 
    config(
      target_schema='snapshots',
      unique_key='id_order',
      strategy='timestamp',
      updated_at='_fivetran_synced',
      invalidate_hard_deletes=True,
    ) 
    }}


select * from {{ ref('prueba_incremental_orders') }}

{% endsnapshot %}



