with 

source as (

    select * from {{ source('sql_server_dbo', 'events') }}

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
        cast(created_at as timestamp_ntz) as created_at_utz,
        decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
        _fivetran_synced
from source

)

select * from renamed
