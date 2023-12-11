with

    source as ( select * from {{ ref('base_promos') }}),




    renamed2 as (

        select
           {{ dbt_utils.surrogate_key(['promo_id'],['promo_description'],[' discount'],['status']) }} as id_promo,
            promo_id as promo_description,
            discount as discount_euro,
            {{ dbt_utils.surrogate_key(['status'])}} as id_status,
            _fivetran_deleted,
            _fivetran_synced

        from source
    ) 

select *
from renamed2
union all
(
select 
'Not promo' as id_promo, 'Not promo' as promo_description, '0' as discount, '0' as status,'0' as _fivetran_deleted,  '0' as _fivetran_synced)

