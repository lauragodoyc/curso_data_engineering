{{ config(
    materialized='incremental',
    unique_key = 'user_id'
    ) 
    }}


WITH stg_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo','users') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(f_carga) from {{ this }})

{% endif %}
) , 

renamed_casted as (
    SELECT
        user_id,
        first_name,
        last_name,
        address_id,
        cast(replace(phone_number, '-', '') as int) as phone_number,
        _fivetran_synced as f_carga

    FROM stg_users
    )




SELECT * FROM renamed_casted