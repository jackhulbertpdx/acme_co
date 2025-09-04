{{ config(
    materialized='table',
    tags=['intermediate', 'finance']
) }}

with invoice_aging as (
    select
        i.*,
        c.customer_name,
        c.customer_tier,
        c.region,
        case 
            when i.payment_status = 'Paid' then 0
            when current_date() - i.due_date <= 0 then 0
            when current_date() - i.due_date <= 30 then 1
            when current_date() - i.due_date <= 60 then 2  
            when current_date() - i.due_date <= 90 then 3
            else 4
        end as aging_bucket
    from {{ ref('stg_finance__invoices_raw') }} i
    left join {{ ref('stg_finance__customers_raw') }} c
        on i.customer_id = c.customer_id
)

select
    customer_id,
    customer_name,
    customer_tier,
    region,
    
    sum(case when aging_bucket = 0 then invoice_amount else 0 end) as current_amount,
    sum(case when aging_bucket = 1 then invoice_amount else 0 end) as days_1_30,
    sum(case when aging_bucket = 2 then invoice_amount else 0 end) as days_31_60,
    sum(case when aging_bucket = 3 then invoice_amount else 0 end) as days_61_90,
    sum(case when aging_bucket = 4 then invoice_amount else 0 end) as days_over_90,
    
    sum(invoice_amount) as total_outstanding,
    count(*) as total_invoices

from invoice_aging
where payment_status != 'Paid'
group by all