with 

    source as (

    select * from {{ source('sql_server_dbo', 'users') }}

    ),

renamed as (

    select
        user_id as id_user,
        cast(updated_at as timestamp_ntz) as updated_at_utz,
        address_id as id_address,
        last_name,
        cast(created_at as timestamp_ntz) as created_at_utz,
        phone_number,
        decode(total_orders,null, '0',total_orders) as total_orders_euro,
        first_name,
        email,
        decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
