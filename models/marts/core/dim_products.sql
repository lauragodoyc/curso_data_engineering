with 

source as (

    select * from {{ ref('stg_products') }}

),

renamed as (

    select
        id_product,
        price_euro,
        name,
        inventory,
        _fivetran_synced as _fivetran_synced_utc

    from source

)

select * from renamed
