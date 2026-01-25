{{config(
    materialized='incremental',
    incremental_strategy='append'
    )}}

with base as (
    select 
    hash_key,
    block_timestamp,
    gas_total_amount_val,
    transaction_type,
    transaction_type_desc,
    value
    from {{ref('stg_eth')}}

{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{this}})
{% endif %}
),

FROM_SEED as(
select
replace(snapped_at, ' UTC', '')::TIMESTAMP as snapped_at,
round(price, 2) as price_from_market
from {{ref('eth_usd_max')}}
),

final as(
    select
    b.*,
    f.price_from_market
    from base b
    cross join FROM_SEED f
    where date(b.block_timestamp) = date(f.snapped_at)
),

TRI AS(select 
transaction_type_desc,
avg(coalesce(gas_total_amount_val, 0))/1000000000000 as avg_gas_total_amount_val,
avg(coalesce(price_from_market, 0))/1000000000000 as avg_price_from_market,
avg(coalesce(value, 0))/1000000000000 as avg_value
from jointure
group by transaction_type_desc
order by avg_value desc
)

select 
* 
from final
