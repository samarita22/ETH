{% macro gas_total_amount(val1, val2) %}
    {{ val1 }} * {{ val2 }}
{% endmacro %}

{% macro check_transaction_type_desc(val) %}
    CASE
        WHEN {{val}}=0 THEN 'Legacy'
        WHEN {{val}}=1 THEN 'Access List'
        WHEN {{val}}=2 THEN 'EIP-1559'
        WHEN {{val}}=3 THEN 'Blob tx'
        WHEN {{val}}=4 THEN 'SetCode'
        ELSE 'Unknown'
    END
{% endmacro %}