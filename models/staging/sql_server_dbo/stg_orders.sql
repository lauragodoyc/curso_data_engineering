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
            cast(created_at as timestamp_ntz) as created_at,
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
    ),   


    renamed2 as (

        select
            order_id as id_order,
            decode(shipping_service, '', 'no datos', shipping_service) as shipping_service,
            shipping_cost,
            address_id as id_address,
            created_at,
            cast( case
                    when id_promo='999' then '999'
                    else {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }}
                    end as varchar(50)) as id_promo,
            decode(promo_description, '', 'no promo', promo_description) as promo_description,
            decode(estimated_delivery_at, NULL, '0', estimated_delivery_at) as estimated_delivery_at,
            order_cost,
            user_id as id_user,
            order_total,
            decode(delivered_at, null, '0', delivered_at) as delivered_at,
            decode(tracking_id, '', 'no tracking', tracking_id) as id_tracking,
            status,
            decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
            _fivetran_synced
        from renamed
    )

select * 
from renamed2
