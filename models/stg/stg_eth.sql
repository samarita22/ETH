{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge'
) }}

WITH MAIN AS (
select
    PAYLOAD:hash::STRING as hash_key,
    PAYLOAD:block_number::NUMBER as block_number,
    TO_TIMESTAMP(PAYLOAD:block_timestamp) as block_timestamp,
    PAYLOAD:gas::NUMBER as gas_num,
    PAYLOAD:gas_price::NUMBER as gas_price_num,
    PAYLOAD:max_fee_per_gas::NUMBER as max_fee_per_gas,
    PAYLOAD:max_priority_fee_per_gas::NUMBER as max_priority_fee_per_gas,
    PAYLOAD:from_address::STRING as from_address,
    PAYLOAD:to_address::STRING as to_address,
    PAYLOAD:value::NUMBER as value,
    PAYLOAD:transaction_type::NUMBER as transaction_type
from {{ source('eth','ETH_TRANSACTIONS_RAW') }}

{% if is_incremental() %}
where TO_TIMESTAMP(PAYLOAD:block_timestamp) >= (
    select max(block_timestamp) from {{ this }}
)
{% endif %}
qualify row_number() over(partition by PAYLOAD:hash::STRING order by PAYLOAD:hash::STRING  desc) = 1
),

CALC AS(
    select
    hash_key,
    block_number,
    block_timestamp,
    gas_num,
    gas_price_num,
    {{ gas_total_amount("gas_num", "gas_price_num") }} as gas_total_amount_val,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    from_address,
    to_address,
    value,
    transaction_type,
    {{check_transaction_type_desc("transaction_type")}} as transaction_type_desc
from MAIN
)

select 
*
from CALC