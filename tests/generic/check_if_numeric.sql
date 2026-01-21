{% test assert_check_if_numeric(model, column_name) %}

select
    {{column_name}}
from {{model}}
where {{column_name}} is null and {{ column_name }} !RLIKE '^[0-9]+$'

{% endtest%}