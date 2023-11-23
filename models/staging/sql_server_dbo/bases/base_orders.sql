with

    source as (
        select * from {{ source("sql_server_dbo", "orders") }}
        ),


    renamed as (

        select
            order_id,
            shipping_service,
            shipping_cost,
            address_id,
            created_at,
            decode(promo_id,'', '999', promo_id)::varchar as id_promo,
            promo_id as promo_description,
            cast(estimated_delivery_at as timestamp_ntz) as estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            cast(delivered_at as timestamp_ntz) as delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
        from source
    )   




select * 
from renamed
