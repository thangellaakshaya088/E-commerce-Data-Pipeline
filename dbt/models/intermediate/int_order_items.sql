with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

order_metrics as (
    select
        o.order_id,
        o.user_id,
        o.status,
        o.order_date,
        o.total_amt,
        o.payment_method,
        o.item_count,
        o.created_at,
        case
            when o.status in ('confirmed', 'shipped', 'delivered') then true
            else false
        end as is_completed,
        case
            when o.delivered_at is not null
            then extract(epoch from (o.delivered_at - o.created_at)) / 86400
        end as fulfillment_days
    from orders o
)

select * from order_metrics
