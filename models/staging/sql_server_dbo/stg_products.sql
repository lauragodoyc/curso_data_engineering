with 

source as (

    select * from {{ source('sql_server_dbo', 'products') }}

),

renamed as (

    select
        product_id as id_product,
        price as price_euro,
        name,
        inventory,
        decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
