with 

source as (

    select * from {{ ref('base_addresses') }}

),




renamed as (

    select
        id_address,
        zipcode,
        country,
        address,
        state,
        _fivetran_deleted,
        _fivetran_synced

from {{ ref('base_addresses') }}

)

select * from renamed