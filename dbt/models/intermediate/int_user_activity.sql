with users as (
    select * from {{ ref('stg_users') }}
),

orders as (
    select * from {{ ref('int_order_items') }}
),

sessions as (
    select * from {{ ref('int_session_events') }}
),

order_summary as (
    select
        user_id,
        count(order_id)             as total_orders,
        sum(total_amt)              as lifetime_value,
        avg(total_amt)              as avg_order_value,
        min(created_at)             as first_order_at,
        max(created_at)             as last_order_at
    from orders
    where is_completed
    group by user_id
),

session_summary as (
    select
        user_id,
        count(session_id)           as total_sessions,
        avg(session_duration_min)   as avg_session_min,
        sum(purchase_events)        as total_purchases_from_sessions
    from sessions
    where user_id is not null
    group by user_id
)

select
    u.user_id,
    u.email,
    u.segment,
    u.acquisition_channel,
    u.created_at,
    coalesce(o.total_orders, 0)          as total_orders,
    coalesce(o.lifetime_value, 0)        as lifetime_value,
    coalesce(o.avg_order_value, 0)       as avg_order_value,
    o.first_order_at,
    o.last_order_at,
    coalesce(s.total_sessions, 0)        as total_sessions,
    s.avg_session_min
from users u
left join order_summary o using (user_id)
left join session_summary s using (user_id)
