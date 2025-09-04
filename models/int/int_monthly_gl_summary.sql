{{ config(
    materialized='table',
    tags=['intermediate', 'finance']
) }}

select
    date_trunc('month', gl.transaction_date) as month_year,
    gl.account_id,
    coa.account_name,
    coa.account_type,
    coa.account_subtype,
    
    sum(case when gl.debit_credit = 'Debit' then gl.amount else 0 end) as total_debits,
    sum(case when gl.debit_credit = 'Credit' then gl.amount else 0 end) as total_credits,
    sum(case when gl.debit_credit = 'Debit' then gl.amount else -gl.amount end) as net_amount,
    
    count(*) as transaction_count,
    min(gl.transaction_date) as first_transaction_date,
    max(gl.transaction_date) as last_transaction_date

from {{ ref('stg_finance__general_ledger_raw') }} gl
left join {{ ref('stg_finance__chart_of_accounts_raw') }} coa
    on gl.account_id = coa.account_id
    
group by all