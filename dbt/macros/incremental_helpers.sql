{% macro get_max_date(table, date_col, lookback_days=3) %}
    (
        select coalesce(
            max({{ date_col }}) - interval '{{ lookback_days }} days',
            '2023-01-01'::date
        )
        from {{ table }}
    )
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
    case when {{ denominator }} = 0 then null
         else {{ numerator }}::numeric / {{ denominator }}
    end
{% endmacro %}

{% macro date_trunc_to_period(date_col, period='day') %}
    date_trunc('{{ period }}', {{ date_col }})::date
{% endmacro %}
