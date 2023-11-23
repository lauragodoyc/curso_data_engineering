with 

source as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),




renamed as (

    select
        address_id as id_address,
        zipcode,
        country,
        address,
        state,
        decode(_fivetran_deleted, null, '0', _fivetran_deleted) as _fivetran_deleted,
        _fivetran_synced

from source

)

select * from renamed