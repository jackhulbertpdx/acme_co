with 

source as (

    select * from {{ source('finance', 'customers_raw') }}

),

renamed as (

    select
        customer_id,
        customer_name,
        industry,
        region,
        customer_tier,
        payment_terms,
        credit_limit,
        is_active,
        created_date

    from source

)

select * from renamed
