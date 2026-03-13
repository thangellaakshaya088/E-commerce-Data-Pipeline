with source as (
    select * from {{ source('shopstream_bronze', 'products') }}
),

cleaned as (
    select
        product_id,
        sku,
        name                                     as product_name,
        lower(trim(category))                    as category,
        lower(trim(subcategory))                 as subcategory,
        brand,
        cast(price as numeric(10, 2))            as price,
        cast(discount_pct as numeric(5, 4))      as discount_pct,
        cast(final_price as numeric(10, 2))      as final_price,
        stock_qty,
        cast(rating as numeric(3, 1))            as rating,
        review_count,
        is_active,
        cast(created_at as timestamp)            as created_at,
        cast(updated_at as timestamp)            as updated_at
    from source
    where product_id is not null
      and price > 0
)

select * from cleaned
