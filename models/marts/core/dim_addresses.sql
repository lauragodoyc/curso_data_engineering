with 



renamed as (

    select
        id_address,
        zipcode,
        country,
        address,
        state,
        _fivetran_synced as _fivetran_synced_utc

    from {{ ref('stg_addresses') }} 


)


select * from renamed
    