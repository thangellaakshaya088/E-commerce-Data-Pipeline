with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    price,
    discount_pct,
    final_price,
    stock_qty,
    rating,
    review_count,
    is_active,
    case
        when stock_qty = 0               then 'out_of_stock'
        when stock_qty < 10              then 'low_stock'
        else 'in_stock'
    end                                  as stock_status,
    case
        when rating >= 4.5               then 'excellent'
        when rating >= 3.5               then 'good'
        when rating >= 2.5               then 'average'
        else 'poor'
    end                                  as rating_band,
    created_at,
    updated_at
from products
