with 

source as (

    select * from {{ ref('stg_order_items') }}

),

renamed as (

    select
        id_order,
        id_product,
        id_product_order, 
        quantity,
        _fivetran_synced as _fivetran_synced_utc

    from source

)

select * from renamed
