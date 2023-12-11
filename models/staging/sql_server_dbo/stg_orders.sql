with

        
        source as (
        select * from {{ ref('base_orders')}}
        
        ),


    renamed as (

        select
            order_id as id_order,
            decode(shipping_service, '', 'not shipping service asigned', shipping_service) as shipping_service,
            shipping_cost as shipping_cost_euro,
            address_id as id_address,
            cast(created_at as time) as created_at_time_utc,
            cast(created_at as date) as created_at_date_utc,
            decode(promo_id,'', '999', promo_id)::varchar as id_promo,
            decode(promo_id,'', 'not promo', promo_id) as promo_description,
            cast(estimated_delivery_at as date) as  estimated_delivery_at_date_utc,
            cast(estimated_delivery_at as time) as  estimated_delivery_at_time_utc,
            cast(delivered_at as time) as delivered_at_time_utc,
            cast(delivered_at as date) as delivered_at_date_utc,
            order_cost as order_cost_euro,
            user_id as id_user,
            order_total as order_total_euro,
            {{ dbt_utils.surrogate_key(['tracking_id'])}} as id_tracking,
            {{ dbt_utils.surrogate_key(['status'])}} as id_status,
            _fivetran_deleted,
            _fivetran_synced
        from source
    ),

    renamed2 as (
        select
            id_order,
            shipping_service,
            shipping_cost_euro,
            id_address,
            created_at_time_utc,
            created_at_date_utc,
            promo_description,
            cast( case
                    when id_promo='999' then '999'
                    else {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }}
                    end as varchar(50)) as id_promo,
            estimated_delivery_at_time_utc,
            estimated_delivery_at_date_utc,
            delivered_at_time_utc,
            delivered_at_date_utc,
            order_cost_euro,
            id_user,
            order_total_euro,
            id_tracking,
            id_status,
            _fivetran_deleted,
            _fivetran_synced

        from renamed
    )

select * 
from renamed2
