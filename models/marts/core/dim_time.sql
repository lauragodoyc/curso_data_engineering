{{ config(
  materialized='table'
) }}

WITH dim_time AS 
(
    {{ dbt_utils.date_spine(
    datepart="second",
    start_date="cast('00:00:00' as time)",
    end_date="cast('23:59:59' as time)"
   )
}}
)

SELECT
      date_second ,
      extract (hour from date_second) as hour_time

FROM dim_time
