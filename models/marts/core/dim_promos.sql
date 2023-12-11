with

    source as ( select * from {{ ref('stg_promos') }}),




    renamed2 as (

        select
            id_promo,
            promo_description,
            discount_euro,
            id_status,
            decode(_fivetran_synced, null, '0', _fivetran_synced) as _fivetran_synced_utc

        from source
    ) 

select *
from renamed2
union all
(
select 
'999' as id_promo, 'Not promo' as promo_description, '0' as discount, '0' as status,  '0' as _fivetran_synced_utc)
