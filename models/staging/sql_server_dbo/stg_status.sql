
select 
distinct ({{ dbt_utils.surrogate_key(['status'])}}) as id_status,
status
from {{ source("sql_server_dbo", "promos") }}

union all

select 
distinct ({{ dbt_utils.surrogate_key(['status'])}}) as id_status,
status
from {{ source("sql_server_dbo", "orders") }}