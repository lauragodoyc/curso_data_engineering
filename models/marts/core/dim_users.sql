{{ config(
    materialized='incremental'
    ) 
    }}

WITH stg_users AS (
    SELECT * 
    FROM {{ ref ('stg_users') }}
{% if is_incremental() %}

  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
{% endif %}
    ),

total_orders as(
    select id_user,
    count(distinct(id_order)) as total_orders
    from {{ ref('stg_orders') }}
   group by id_user 
    
),
renamed as (

    select
        users.id_user,
        updated_at_time_utc,
        updated_at_date_utc,
        users.id_address,
        last_name,
        created_at_time_utc,
        created_at_date_utc,
        phone_number,
        total_orders.total_orders,
        first_name,
        email,
    
        _fivetran_synced  

    from stg_users users
    left join total_orders 
    on users.id_user=total_orders.id_user
    
   

)

select * from renamed
