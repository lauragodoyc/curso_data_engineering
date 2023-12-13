{{ config(
    materialized='incremental'
   
    ) 
    }}


WITH orders AS (
    SELECT * 
    FROM {{ ref('stg_orders') }} 
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}
) , 



    renamed as (

        select *

        from  orders
    )

select * 
from renamed
