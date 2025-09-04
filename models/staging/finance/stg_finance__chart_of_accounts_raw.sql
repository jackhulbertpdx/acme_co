with 

source as (

    select * from {{ source('finance', 'chart_of_accounts_raw') }}

),

renamed as (

    select
        account_id,
        account_name,
        account_type,
        account_subtype

    from source

)

select * from renamed
