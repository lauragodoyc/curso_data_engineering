{% snapshot snapshot_users %}

{{
    config(
      target_schema='snapshots',
      unique_key='id_user',
      strategy='timestamp',
      updated_at='_fivetran_synced',
      invalidate_hard_deletes=True,
    ) 
}}

select *
 from {{ ref('prueba_incremental_users') }}

{% endsnapshot %}