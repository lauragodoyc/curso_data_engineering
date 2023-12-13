{{ config(
    materialized='incremental'
   
    ) 
    }}


WITH users AS (
    SELECT * 
    FROM {{ ref('stg_users') }} 
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}
) , 



    renamed as (

        select *

        from  users
    )

select * 
from renamed