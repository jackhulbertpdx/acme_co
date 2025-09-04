with 

source as (

    select * from {{ source('quickbooks', 'general_ledger_raw') }}

),

renamed as (

    select
        entry_id,
        transaction_date,
        account_id,
        description,
        debit_credit,
        amount,
        reference_number,
        created_at,
        created_by

    from source

)

select * from renamed
