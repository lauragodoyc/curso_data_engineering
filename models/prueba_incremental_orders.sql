{{ config(
    materialized='incremental'
   
    ) 
    }}


WITH stg_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo','orders') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}
) , 



    renamed as (

        select
            order_id,
            shipping_service,
            shipping_cost,
            address_id,
            created_at,

            estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
        from {{ source('sql_server_dbo','orders') }}
    )

select * 
from renamed
