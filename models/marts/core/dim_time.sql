with 

    source as (

    select * from {{ ref('stg_time') }}

    ),

renamed as (

    select
         date_second as time,
         hour_time

    from {{ ref('stg_time') }} time
    
)

select * from renamed
