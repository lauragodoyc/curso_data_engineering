with 

    source as (
         select * from {{ source('sql_server_dbo', 'orders') }}
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
            estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
        from source
    ),   


    renamed2 as (

        select
            order_id,
            shipping_service,
            shipping_cost,
            address_id,
            created_at,
            {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }} as id_promo,
            promo_description,
            estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
        from renamed
    )

select * 
from renamed2
