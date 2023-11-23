with

    source as ( select * from {{ source("sql_server_dbo", "promos") }}),


    renamed as (

        select
            promo_id,
            discount,
            status,
            _fivetran_deleted,
            _fivetran_synced

        from source

    )



select *
from renamed
union all
(
select 
'999' as id_promo,  '0' as discount, '0' as status, '0' as _fivetran_deleted, '0' as _fivetran_synced)