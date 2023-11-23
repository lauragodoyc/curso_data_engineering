with 

source as (

    select * from {{ ref('base_products') }}

),

renamed as (

    select
        id_product,
        price_euro,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced

    from {{ref('base_products')}}

)

select * from renamed
