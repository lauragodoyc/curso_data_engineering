with 

source as (

    select * from {{ ref('base_users') }}

),

renamed as (

    select
        id_user,
        updated_at_utz,
        id_address,
        last_name,
        created_at_utz,
        phone_number,
        total_orders_euro,
        first_name,
        email,
         _fivetran_deleted,
        _fivetran_synced

    from {{ ref('base_users') }}

)

select * from renamed
