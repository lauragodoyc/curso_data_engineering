{{ config(
    materialized='incremental',
    ) 
    }}

WITH stg_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
{% if is_incremental() %}

  where _fivetran_synced > (select max(f_carga) from {{ this }})
{% endif %}
    ),

renamed_casted AS (
    SELECT
        user_id,
        address_id,
        last_name,
        cast(replace(phone_number ,'-','' ) as int) as phone_number,
        first_name,
        _fivetran_synced as f_carga

FROM {{ source('sql_server_dbo', 'users') }}
)
SELECT * FROM renamed_casted