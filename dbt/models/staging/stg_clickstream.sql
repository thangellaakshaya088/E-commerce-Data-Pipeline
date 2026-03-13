with source as (
    select * from {{ source('shopstream_bronze', 'clickstream') }}
),

cleaned as (
    select
        event_id,
        session_id,
        user_id,
        anonymous_id,
        lower(trim(event_type))  as event_type,
        lower(trim(page_type))   as page_type,
        url,
        referrer,
        lower(trim(device))      as device,
        browser,
        os,
        cast(timestamp as timestamp) as event_ts
    from source
    where event_id is not null
      and session_id is not null
      and timestamp is not null
)

select * from cleaned
