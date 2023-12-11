with 

source as (

    select * from {{ ref('base_order_items') }}

),

renamed as (

    select
        order_id as id_order,
        product_id as id_product,
        concat(order_id,'-',product_id) as id_product_order, 
        quantity,
        _fivetran_deleted,
        _fivetran_synced

    from {{ ref('base_order_items') }}

)

select * from renamed
