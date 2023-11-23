with 

source as (

    select * from {{ ref('base_events') }}

),

renamed as (

    select
        id_event,
        page_url,
        event_type,
        id_user,
        id_session,
        id_order,
        id_product,
        created_at_utz,
        _fivetran_deleted,
        _fivetran_synced
from {{ ref('base_events') }}

)

select * from renamed

