with 



renamed as (

    select
        addresses.id_address,
        users.id_user,
        zipcode,
        country,
        address,
        state,
        addresses._fivetran_deleted,
        addresses._fivetran_synced

    from {{ ref('stg_addresses') }} addresses
    left join {{ ref('stg_users') }} users
    on addresses.id_address=users.id_address


)

select * from renamed
    