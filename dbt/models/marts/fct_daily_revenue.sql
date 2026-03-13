{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='merge'
    )
}}

with orders as (
    select * from {{ ref('int_order_items') }}
    {% if is_incremental() %}
    where order_date >= (select max(order_date) - interval '3 days' from {{ this }})
    {% endif %}
),

daily as (
    select
        order_date,
        count(order_id)             as order_count,
        sum(total_amt)              as gross_revenue,
        avg(total_amt)              as avg_order_value,
        count(distinct user_id)     as unique_customers,
        sum(case when not is_completed then total_amt else 0 end) as cancelled_revenue
    from orders
    group by order_date
)

select
    order_date,
    order_count,
    round(gross_revenue::numeric, 2)        as gross_revenue,
    round(avg_order_value::numeric, 2)      as avg_order_value,
    unique_customers,
    round(cancelled_revenue::numeric, 2)    as cancelled_revenue,
    current_timestamp                       as updated_at
from daily
