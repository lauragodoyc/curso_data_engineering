with 

    source as (

    select * from {{ ref('stg_orders') }}

    ),

renamed as (

    select
        orders.id_tracking,
        orders.id_status,
        orders.delivered_at_date_utc,
        orders.delivered_at_time_utc,
        orders.estimated_delivery_at_time_utc,
        orders.estimated_delivery_at_date_utc,
        addresses.id_address,
        orders.shipping_service,
        orders.shipping_cost_euro,
        orders.id_order,
        decode(users.id_user, null, 'Not user', users.id_user) as id_user

    from {{ ref('stg_orders') }} orders
    left join {{ ref('stg_addresses') }} addresses
    on orders.id_address=addresses.id_address
    left join {{ ref('stg_users') }} users
    on users.id_address=orders.id_address
    left join {{ref('dim_status')}} status 
    on status.id_status=orders.id_status


)

select * from renamed
