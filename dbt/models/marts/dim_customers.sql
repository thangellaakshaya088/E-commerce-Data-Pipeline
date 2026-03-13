with activity as (
    select * from {{ ref('int_user_activity') }}
)

select
    user_id,
    email,
    segment,
    acquisition_channel,
    created_at                                                          as customer_since,
    total_orders,
    round(lifetime_value::numeric, 2)                                   as lifetime_value,
    round(avg_order_value::numeric, 2)                                  as avg_order_value,
    first_order_at,
    last_order_at,
    total_sessions,
    round(avg_session_min::numeric, 2)                                  as avg_session_min,
    case
        when lifetime_value >= 1000 then 'platinum'
        when lifetime_value >= 500  then 'gold'
        when lifetime_value >= 100  then 'silver'
        else 'bronze'
    end                                                                 as customer_tier,
    case
        when last_order_at >= current_date - interval '30 days' then 'active'
        when last_order_at >= current_date - interval '90 days' then 'at_risk'
        when last_order_at is not null                           then 'churned'
        else 'never_purchased'
    end                                                                 as rfm_status,
    current_timestamp                                                   as updated_at
from activity
