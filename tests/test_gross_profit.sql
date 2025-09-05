-- models/tests/test_gross_profit.sql

select
    revenue,
    cost_of_goods_sold,
    gross_profit,
    (revenue - cost_of_goods_sold) as calculated_gross_profit
from {{ ref('income_statement') }}
where gross_profit != (revenue - cost_of_goods_sold)
