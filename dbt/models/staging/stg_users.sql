with source as (
    select * from {{ source('shopstream_bronze', 'users') }}
),

cleaned as (
    select
        user_id,
        lower(trim(email))                   as email,
        username,
        first_name,
        last_name,
        phone,
        is_active,
        lower(trim(segment))                 as segment,
        lower(trim(acquisition_channel))     as acquisition_channel,
        cast(created_at as timestamp)        as created_at,
        cast(updated_at as timestamp)        as updated_at
    from source
    where user_id is not null
      and email is not null
      and email ~* '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)

select * from cleaned
