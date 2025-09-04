{{ config(
    materialized='table',
    tags=['marts', 'finance', 'financial_statements'])
}}


select
    month_year,
    
    -- Revenue
    sum(case when account_type = 'Revenue' then -net_amount else 0 end) as revenue,
    
    -- Cost of Goods Sold  
    sum(case when account_id = '5000' then net_amount else 0 end) as cost_of_goods_sold,
    
    -- Gross Profit
    sum(case when account_type = 'Revenue' then -net_amount else 0 end) - 
    sum(case when account_id = '5000' then net_amount else 0 end) as gross_profit,
    
    -- Operating Expenses
    sum(case 
        when account_type = 'Expense' and account_id != '5000' 
        then net_amount else 0 
    end) as operating_expenses,
    
    -- Net Income  
    sum(case when account_type = 'Revenue' then -net_amount else 0 end) - 
    sum(case when account_type = 'Expense' then net_amount else 0 end) as net_income

from {{ ref('int_monthly_gl_summary') }}
where month_year >= '2023-01-01'
group by 1
order by month_year