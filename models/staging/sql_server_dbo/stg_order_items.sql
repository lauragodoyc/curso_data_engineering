with 

source as (

    select * from {{ ref('base_order_items') }}

),

renamed as (

    select
        id_order,
        id_product,
        id_product_order, 
        quantity,
        _fivetran_deleted,
        _fivetran_synced

    from {{ ref('base_order_items') }}

)

select * from renamed
