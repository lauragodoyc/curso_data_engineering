with

        
        source as (
        select * from {{ ref('stg_orders')}}
        
        ),


    renamed as (

        select
            orders.id_order,
            id_product,
            order_items.id_product_order,
            shipping_service_euro,
            shipping_cost_euro,
            id_address,
            created_at_utz,
            id_promo,
            promo_description,
            estimated_delivery_at_utz,
            order_cost_euro,
            id_user,
            order_total_euro,
            delivered_at_utz,
            id_tracking,
            id_status,
            orders._fivetran_deleted_utz,
            orders._fivetran_synced

            from {{ ref('stg_orders') }} orders
            left join {{ ref('stg_order_items') }} order_items
            on orders.id_order=order_items.id_order
    )

select * 
from renamed
