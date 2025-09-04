with 

source as (

    select * from {{ source('quickbooks', 'budgets_raw') }}

),

renamed as (

    select
        budget_id,
        budget_year,
        budget_month,
        account_id,
        budget_amount,
        department,
        cost_center,
        created_date,
        version

    from source

)

select * from renamed
