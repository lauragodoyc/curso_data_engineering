with 



renamed as (

    select
        event_id as id_event,
        page_url,
        event_type,
        users.user_id as id_user,
        session_id as id_session,
        decode(order_items.order_id,null, 'no id', order_items.order_id) as  id_order,
        decode(products.product_id, null, 'no id', products.product_id) as id_product,
        cast(events.created_at as timestamp_ntz) as created_at_utz,
        concat(id_order,'',id_product) as id_product_order,
        decode(events._fivetran_deleted,null, '0',events. _fivetran_deleted) as _fivetran_deleted,
        events._fivetran_synced

    from {{ source("sql_server_dbo", "events") }} events
    left join {{ source("sql_server_dbo", "users") }} users
    on events.user_id=users.user_id
    left join {{ source("sql_server_dbo", "products") }} products
    on events.product_id=products.product_id
    left join {{ source("sql_server_dbo", "order_items") }} order_items
    on events.order_id=order_items.order_id

)

select * from renamed
