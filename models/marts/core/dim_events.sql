with 
source as (

    select * from {{ ref('stg_events') }}

),

renamed as (

    select
        id_event,
        page_url,
        event_type,
        users.id_user,
        id_session,
        decode(order_items.id_order,null, 'no id', order_items.id_order) as  id_order,
        decode(products.id_product, null, 'no id', products.id_product) as id_product,
        events.created_at_utz,
        concat(order_items.id_order,'',order_items.id_product) as id_product_order,
        events. _fivetran_deleted,
        events._fivetran_synced



    from {{ ref('stg_events') }} events
    left join {{ ref('stg_users') }} users
    on events.id_user=users.id_user
    left join {{ ref('stg_products') }} products
    on events.id_product=products.id_product
    left join {{ ref('stg_order_items') }} order_items
    on events.id_order=order_items.id_order

)

select * from renamed
