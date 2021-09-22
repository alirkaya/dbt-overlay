select row_number() over (order by date::date) as rownum, *
from {{ ref('etl_working_capital') }}
qualify rownum > 2920
