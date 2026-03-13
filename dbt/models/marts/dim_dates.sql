with date_spine as (
    select generate_series(
        '2023-01-01'::date,
        '2025-12-31'::date,
        '1 day'::interval
    )::date as date_day
)

select
    date_day,
    extract(year from date_day)::int                                    as year,
    extract(quarter from date_day)::int                                 as quarter,
    extract(month from date_day)::int                                   as month,
    to_char(date_day, 'Month')                                          as month_name,
    extract(week from date_day)::int                                    as week_of_year,
    extract(dow from date_day)::int                                     as day_of_week,
    to_char(date_day, 'Day')                                            as day_name,
    extract(day from date_day)::int                                     as day_of_month,
    extract(doy from date_day)::int                                     as day_of_year,
    date_day = date_trunc('month', date_day)::date                      as is_first_of_month,
    extract(dow from date_day) in (0, 6)                                as is_weekend,
    date_trunc('month', date_day)::date                                 as month_start,
    (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end,
    date_trunc('quarter', date_day)::date                               as quarter_start,
    to_char(date_day, 'YYYY-"Q"Q')                                      as year_quarter
from date_spine
