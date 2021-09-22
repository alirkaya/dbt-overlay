--truncate table if exists {{ source('evaluation', 'working_capital') }};
--insert into {{ source('evaluation', 'working_capital') }} (date, daily_balance, account)

with table_generated_date as
(
    select  -1 + row_number() over(order by 0) as i,
            start_date + i as generated_date
    from    (
                select  min(date::date) as start_date,
                        max(date::date) as end_date
                from {{ source('evaluation', 'accounts_receivable') }}
            )
    join    table(generator(rowcount => 10000)) as x
    qualify i < 1 + end_date - start_date
),

accounts as
(
    select      distinct account
    from        {{ source('evaluation', 'accounts_receivable')}}
    order by    account
),

total_daily_trans as
(
    select      date::date as date,
                sum(debit - credit) as daily_transaction,
                account
    from        {{ source('evaluation', 'accounts_receivable') }}
    group by    date,
                account
),

final_version as
(
    select      generated_date as date,
                t2.account as account,
                sum(ifnull(t3.daily_transaction,0)) over (partition by t2.account order by generated_date, t2.account) as daily_balance
    from        table_generated_date t1
    cross join  accounts t2
    left join   total_daily_trans t3
    on          t1.generated_date = t3.date and t2.account = t3.account
    order by    date, account
),

table_completed as
(
  select  date,
          daily_balance + 47742.37 as daily_balance,
          account
  from    final_version
  where   account = 'Account1'

  union

  select  date,
          daily_balance + 422.75 as daily_balance,
          account
  from    final_version
  where   account = 'Account2'

  union

  select  date,
          daily_balance + 5181.13 as daily_balance,
          account
  from    final_version
  where   account = 'Account3'

  union

  select      date,
              daily_balance + 7108874.05 as daily_balance,
              account
  from        final_version
  where       account = 'Account4'
  order by    date, account
)
select *
from table_completed

--select * from {{ source('evaluation', 'working_capital') }}
--order by date::date, account
