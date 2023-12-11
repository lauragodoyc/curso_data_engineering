with 

source as (

    select * from {{ ref('base_products') }}

),

renamed as (

    select
        product_id as id_product,
        price as price_euro,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced

    from {{ref('base_products')}}

)

select * from renamed
