with ranked_orders as (
select *,
row_number() over (partition by session_id order by order_at_ts desc) as row_n
FROM {{ ref('BASE_ORDERS') }}
),
ranked_iv as (
select *,
row_number() over (partition by session_id order by item_view_at_ts desc) as row_n
FROM {{ ref('base_item_views') }}
),

iv AS (
    SELECT 
        item_name,
        add_to_cart_quantity,
        remove_from_cart_quantity,
        price_per_unit,
        session_id,
        item_view_at_ts
    FROM ranked_iv
    where row_n=1
),
o AS (
    SELECT 
        session_id,
        shipping_cost,
        tax_rate,
        order_at_ts,
        order_id
    FROM 
        ranked_orders
    where row_n=1
),

result as 
(SELECT 
    iv.item_name,
    iv.add_to_cart_quantity,
    iv.remove_from_cart_quantity,
    iv.price_per_unit,
    iv.session_id,
    iv.item_view_at_ts,
    o.shipping_cost,
    o.tax_rate,
    o.order_at_ts,
    o.order_id
FROM 
    iv
JOIN 
    o 
ON 
    iv.session_id = o.session_id),

ranked_result as
(select *,
row_number() over (partition by session_id order by order_at_ts desc) as row_n
FROM result),

pre as (select *
from ranked_result
where row_n=1)

select 
item_name,
add_to_cart_quantity,
remove_from_cart_quantity,
price_per_unit,
session_id,
item_view_at_ts,
shipping_cost,
tax_rate,
order_at_ts,
order_id
from pre

