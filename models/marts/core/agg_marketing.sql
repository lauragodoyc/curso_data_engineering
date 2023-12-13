with
source as (select * from {{ref("dim_users")}}),


total_quantity_products as(
    select 
    
    users.id_user,
    sum(items.quantity) as total_quantity_products

    from {{ ref('dim_order_items') }} items
    left join {{ref('fact_orders') }} orders
    on items.id_order=orders.id_order
    left join {{ref('dim_users')}} users 
    on orders.id_user=users.id_user
   group by users.id_user 
    
),


renamed as(
    select
    users.id_user,
    first_name,
    last_name,
    email,
    phone_number,
    users.created_at_date_utc,
    users.created_at_time_utc,
    users.updated_at_date_utc,
    users.updated_at_time_utc,
    addresses.address,
    addresses.zipcode,
    addresses.state,
    addresses.country,
    users.total_orders as total_number_orders,
    orders.order_total_euro as total_order_cost_usd,
    orders.order_total_euro - orders.order_cost_euro as total_shipping_cost_uds,
    total_quantity_products
    


    from {{ ref('dim_users') }} users
    left join {{ref('dim_addresses')}} addresses
    on users.id_address=addresses.id_address
    left join {{ref('fact_orders')}} orders 
    on users.id_user=orders.id_user
    left join total_quantity_products 
    on  users.id_user=total_quantity_products.id_user
)

select* from renamed