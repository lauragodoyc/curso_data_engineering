with

        
        source as (
        select * from {{ ref('stg_orders')}}
        
        ),


    renamed as (

        select
            orders.id_order,
            order_items.id_product_order,
            decode(addresses.id_address, null, '0', addresses.id_address) as id_address,
            orders.created_at_time_utc,
            orders.created_at_date_utc,
            decode(promos.id_promo,null, 'Not promo', promos.id_promo) as id_promo,
            order_cost_euro,
            users.id_user,
            order_total_euro,
            decode(orders._fivetran_synced, null, '0', orders._fivetran_synced) as _fivetran_synced_utc

            from {{ ref('stg_orders') }} orders
            left join {{ ref('stg_order_items') }} order_items
            on orders.id_order=order_items.id_order
            left join {{ref('stg_addresses')}} addresses 
            on addresses.id_address=orders.id_address
            left join {{ref('stg_users')}} users 
            on users.id_user=orders.id_user
            left join {{ref('stg_promos')}} promos 
            on promos.id_promo=orders.id_promo
    )

select * 
from renamed
 