{% snapshot users_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'phone_number', 'address_id'],
      invalidate_hard_deletes=True,
    )
}}

select *
 from {{ ref('prueba_incremental') }}

{% endsnapshot %}