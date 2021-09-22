select *
from {{ ref('etl_working_capital') }}
where round(daily_balance::float, 2) <> 258069.78 AND
        account = 'Account3' AND
        date = '2019-04-27'
