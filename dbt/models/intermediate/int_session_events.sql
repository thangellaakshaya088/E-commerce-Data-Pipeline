with events as (
    select * from {{ ref('stg_clickstream') }}
),

sessions as (
    select
        session_id,
        user_id,
        device,
        browser,
        min(event_ts)                                                               as session_start,
        max(event_ts)                                                               as session_end,
        count(event_id)                                                             as event_count,
        count(distinct page_type)                                                   as unique_page_types,
        sum(case when event_type = 'add_to_cart' then 1 else 0 end)                as add_to_cart_events,
        sum(case when event_type = 'purchase' then 1 else 0 end)                   as purchase_events,
        sum(case when event_type = 'search' then 1 else 0 end)                     as search_events,
        extract(epoch from (max(event_ts) - min(event_ts))) / 60                   as session_duration_min,
        min(event_ts)::date                                                         as session_date
    from events
    group by session_id, user_id, device, browser
)

select
    *,
    purchase_events > 0 as converted
from sessions
