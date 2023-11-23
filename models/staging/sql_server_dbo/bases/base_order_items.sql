with 

source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),


renamed as (

    select
        order_id as id_order,
        product_id as id_product,
        concat(order_id,'-',product_id) as id_product_order, 
        quantity,
        decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
