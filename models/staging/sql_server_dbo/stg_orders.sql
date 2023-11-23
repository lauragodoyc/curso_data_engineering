with

        
        source as (
        select * from {{ ref('base_orders')}}
        
        ),


    renamed as (

        select
            order_id as id_order,
            decode(shipping_service, '', 'no datos', shipping_service) as shipping_service_euro,
            shipping_cost as shipping_cost_euro,
            address_id as id_address,
            cast(created_at as timestamp_ntz) as created_at_utz,
            cast( case
                    when id_promo='999' then '999'
                    else {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }}
                    end as varchar(50)) as id_promo,
            decode(promo_description, '', 'no promo', promo_description) as promo_description,
            decode(estimated_delivery_at, NULL, '0', estimated_delivery_at) as estimated_delivery_at_utz,
            order_cost as order_cost_euro,
            user_id as id_user,
            order_total as order_total_euro,
            decode(delivered_at, null, '0', delivered_at) as delivered_at_utz,
            decode(tracking_id, '', 'no tracking', tracking_id) as id_tracking,
            {{ dbt_utils.surrogate_key(['status'])}} as id_status,
            decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted_utz,
            _fivetran_synced
        from source
    )

select * 
from renamed
