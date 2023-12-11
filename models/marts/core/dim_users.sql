with 

    source as (

    select * from {{ ref('stg_users') }}

    ),
total_orders as(
    select id_user,
    count(distinct(id_order)) as total_orders
    from {{ ref('stg_orders') }}
   group by id_user 
    
),
renamed as (

    select
        distinct(users.id_user),
        updated_at_time_utc,
        updated_at_date_utc,
        users.id_address,
        last_name,
        created_at_time_utc,
        created_at_date_utc,
        phone_number,
       orders.total_orders,
        first_name,
        email,
    
        users._fivetran_synced  as _fivetran_synced_utc

    from {{ ref('stg_users') }} users
    left join total_orders orders
    on orders.id_user=users.id_user
)

select * from renamed
