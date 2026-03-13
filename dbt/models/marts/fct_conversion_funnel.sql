{{
    config(
        materialized='incremental',
        unique_key='session_date',
        incremental_strategy='merge'
    )
}}

with sessions as (
    select * from {{ ref('int_session_events') }}
    {% if is_incremental() %}
    where session_date >= (select max(session_date) - interval '3 days' from {{ this }})
    {% endif %}
)

select
    session_date,
    count(session_id)                                                       as total_sessions,
    sum(case when add_to_cart_events > 0 then 1 else 0 end)                as sessions_with_cart,
    sum(case when converted then 1 else 0 end)                             as converted_sessions,
    round(avg(session_duration_min)::numeric, 2)                           as avg_session_min,
    round(avg(event_count)::numeric, 2)                                    as avg_events_per_session,
    round(
        sum(case when add_to_cart_events > 0 then 1 else 0 end)::numeric
        / nullif(count(session_id), 0) * 100, 2
    )                                                                      as cart_rate_pct,
    round(
        sum(case when converted then 1 else 0 end)::numeric
        / nullif(count(session_id), 0) * 100, 2
    )                                                                      as conversion_rate_pct,
    current_timestamp                                                      as updated_at
from sessions
group by session_date
