with
source as (select * from {{ref("stg_date")}}),

renamed as(
    select
    fecha_forecast,
    id_date,
    anio,
    mes,
    desc_mes,
    id_anio_mes,
    dia_previo,
    anio_semana_dia,
    semana

    from {{ ref("stg_date") }} date
 )

select*from renamed
