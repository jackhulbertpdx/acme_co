{{ config(
    materialized='table',
    tags=['marts', 'finance', 'reporting']
) }}

select
    b.budget_year,
    b.budget_month,
    b.account_id,
    coa.account_name,
    coa.account_type,
    b.department,
    
    b.budget_amount,
    coalesce(gl.net_amount, 0) as actual_amount,
    b.budget_amount - coalesce(gl.net_amount, 0) as variance,
    
    case 
        when b.budget_amount = 0 then null
        else (coalesce(gl.net_amount, 0) / b.budget_amount) * 100 
    end as variance_percentage

from {{ ref('stg_finance__budgets_raw') }} b
left join {{ ref('stg_finance__chart_of_accounts_raw') }} coa
    on b.account_id = coa.account_id
left join (
    select 
        account_id,
        extract(year from month_year) as year,
        extract(month from month_year) as month,
        net_amount
    from {{ ref('int_monthly_gl_summary') }}
) gl on b.account_id = gl.account_id 
    and b.budget_year = gl.year 
    and b.budget_month = gl.month

order by b.budget_year, b.budget_month, b.account_id