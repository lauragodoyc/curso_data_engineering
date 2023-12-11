with
source as (select * from {{ref("stg_users")}}),

page_view as(
    select id_user,
    count(distinct(page_url)) as page_view
    from {{ ref('stg_events') }}
   group by id_user 
    
),

checkout as(
    select id_user,
     count(event_type) as checkout
    from {{ ref('stg_events') }}
   
    where event_type='checkout'
    group by id_user 
),
package_shipped as(
    select id_user,
    count(event_type) as package_shipped
    from {{ ref('stg_events') }}
   
    where event_type='package_shipped'
    group by id_user )
,
add_to_cart as(
    select id_user,
    count(event_type) as add_to_cart
    from {{ ref('stg_events') }}
   
    where event_type='add_to_cart'
    group by id_user 
),



renamed as(
    select
    distinct(id_session),
    users.id_user,
    first_name,
    email,
    --events.first_event_time_utc,
    --events.first_event_date_utc,
    --events.last_event_time_utc,
    --events.last_event_date_utc,
    --events.session_length_minutes,
    page_view,
    add_to_cart,
    checkout,
    package_shipped

    from {{ ref('stg_users') }} users
    left join {{ref('stg_events')}} events
    on users.id_user=events.id_user
    left join page_view 
    on events.id_user=page_view.id_user
    left join add_to_cart 
    on users.id_user=add_to_cart.id_user
    left join checkout 
    on users.id_user=checkout.id_user
    left join package_shipped
    on users.id_user=package_shipped.id_user
    


 )

select*from renamed
