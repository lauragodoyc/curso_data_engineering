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

