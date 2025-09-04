{{ config(
    materialized='table',
    tags=['marts', 'finance', 'financial_statements'])}}


select
    month_year,
    
    -- Assets
    sum(case when account_type = 'Asset' then net_amount else 0 end) as total_assets,
    
    -- Liabilities  
    sum(case when account_type = 'Liability' then -net_amount else 0 end) as total_liabilities,
    
    -- Equity
    sum(case when account_type = 'Equity' then -net_amount else 0 end) as total_equity

from {{ ref('int_monthly_gl_summary') }}
where month_year >= '2023-01-01'
group by 1
order by month_year