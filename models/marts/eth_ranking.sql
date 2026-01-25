{{config(materialized='view')}}
select
*
from {{ref('stg_variation_between_real_and_calculated_val')}}
