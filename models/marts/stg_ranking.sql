{{config(
    materialized='table',
    )}}

select
transaction_type_desc,
(avg(gas_total_amount_val)/1000000000) as avg_gas_total_amount_val,
(avg(value)/1000000000) as avg_value
from {{ref('stg_variation_between_real_and_calculated_val')}}
group by transaction_type_desc
order by avg_value desc