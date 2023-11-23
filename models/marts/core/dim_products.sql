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
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
