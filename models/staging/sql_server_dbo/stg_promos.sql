with

    source as ( select * from {{ ref('base_promos') }}),




    renamed2 as (

        select
           {{ dbt_utils.surrogate_key(['id_promo'],['promo_description'],[' discount'],['status'], [' _fivetran_deleted'], ['_fivetran_synced']) }} as id_promo,
            upper(promo_id) as promo_description,
            discount,
            {{ dbt_utils.surrogate_key(['status'])}} as id_status,
            decode(_fivetran_deleted,null, '0', _fivetran_deleted) as _fivetran_deleted,
            _fivetran_synced

        from source
    ) 

select *
from renamed2
union all
(
select 
'999' as id_promo, 'Not promo' as promo_description, '0' as discount, '0' as status, '0' as _fivetran_deleted, '0' as _fivetran_synced)

