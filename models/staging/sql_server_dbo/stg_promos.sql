with

    source as (
        select * from {{ source("sql_server_dbo", "promos") }}
        ),

    renamed as (

        select
            promo_id,
            promo_id as promo_description,
            discount,
            status,
            _fivetran_deleted,
            _fivetran_synced

        from source

    ),

    renamed2 as (

        select
           {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }} as id_promo,
            upper(promo_description) as promo_description,
            discount,
            status,
            decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
            _fivetran_synced

        from renamed

    ) 

select *
from renamed2
union all
(
select 
'999' as id_promo, 'Not promo' as promo_description, '0' as discount, 'inactive' as status, '0' as _fivetran_deleted, '0' as _fivetran_synced)

