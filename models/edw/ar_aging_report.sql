{{ config(
    materialized='table',
    tags=['marts', 'finance', 'collections']
) }}

select
    customer_id,
    customer_name,
    customer_tier,
    region,
    
    current_amount,
    days_1_30,
    days_31_60, 
    days_61_90,
    days_over_90,
    total_outstanding,
    
    case 
        when total_outstanding = 0 then 0
        else (days_over_90 / total_outstanding) * 100 
    end as high_risk_percentage,
    
    case
        when days_over_90 > 50000 then 'High Risk'
        when days_over_90 > 10000 then 'Medium Risk'
        else 'Low Risk'
    end as risk_category,
    
    total_invoices

from {{ ref('int_customer_aging') }}
where total_outstanding > 0
order by total_outstanding desc