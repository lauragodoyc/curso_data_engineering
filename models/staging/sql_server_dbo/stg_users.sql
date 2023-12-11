with 

source as (

    select * from {{ ref('base_users') }}

),


renamed as (

    select
        user_id as id_user,
        cast(updated_at as time) as updated_at_time_utc,
        cast(updated_at as date) as updated_at_date_utc,
        address_id as id_address,
        last_name,
        cast(created_at as time) as created_at_time_utc,
        cast(created_at as date) as created_at_date_utc,
        phone_number,
        decode(total_orders,null, '0',total_orders) as total_orders,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced

    from {{ ref('base_users') }}

)


select * from renamed
