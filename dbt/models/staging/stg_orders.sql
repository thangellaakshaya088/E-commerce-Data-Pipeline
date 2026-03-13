with source as (
    select * from {{ source('shopstream_bronze', 'orders') }}
),

cleaned as (
    select
        order_id,
        user_id,
        lower(trim(status))                                    as status,
        item_count,
        cast(subtotal as numeric(12, 2))                       as subtotal,
        cast(shipping_cost as numeric(10, 2))                  as shipping_cost,
        cast(tax_amt as numeric(10, 2))                        as tax_amt,
        cast(total_amt as numeric(12, 2))                      as total_amt,
        lower(trim(payment_method))                            as payment_method,
        carrier,
        tracking_number,
        cast(created_at as timestamp)                          as created_at,
        cast(updated_at as timestamp)                          as updated_at,
        cast(shipped_at as timestamp)                          as shipped_at,
        cast(delivered_at as timestamp)                        as delivered_at,
        date_trunc('day', cast(created_at as timestamp))::date as order_date
    from source
    where order_id is not null
      and user_id is not null
      and total_amt > 0
)

select * from cleaned
