with 

source as (

    select * from {{ ref('base_events') }}

),

renamed as (

    select
        event_id as id_event,
        page_url,
        event_type,
        user_id as id_user,
        session_id as id_session,
        decode(order_id,null, 'no id', order_id) as  id_order,
        decode(product_id, null, 'no id', product_id) as id_product,
        cast(created_at as time) as created_at_time_utc,
        cast(created_at as date) as created_at_date_utc,        
        _fivetran_deleted,
        _fivetran_synced
from {{ ref('base_events') }}

)

select * from renamed

