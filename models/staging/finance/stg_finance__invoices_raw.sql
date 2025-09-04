with 

source as (

    select * from {{ source('finance', 'invoices_raw') }}

),

renamed as (

    select
        invoice_id,
        customer_id,
        invoice_date,
        due_date,
        invoice_amount,
        payment_status,
        payment_date,
        sales_rep,
        product_category,
        currency
    from source

)

select * from renamed
