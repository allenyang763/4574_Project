WITH ranked_orders AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY order_at_ts DESC) AS row_n
    FROM {{ ref('BASE_ORDERS') }}
),
ranked_sessions AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY session_at_ts DESC) AS row_n
    FROM {{ ref('BASE_SESSIONS') }}
),
iv AS (
    SELECT session_id,
        count(item_name) as total_no_item_viewed
    FROM {{ ref('base_item_views') }}
    GROUP BY session_id
),
o AS (
    SELECT 
        session_id
    FROM ranked_orders
    WHERE row_n = 1
),
pv AS (
    SELECT session_id,
       COUNT(page_name) AS total_no_page_viewed,
       MAX(CASE WHEN page_name = 'shop_plants' THEN true ELSE false END) AS SHOP_PLANTS_IS_VIEWED,
       MAX(CASE WHEN page_name = 'cart' THEN true ELSE false END) AS CART_IS_VIEWED,
       MAX(CASE WHEN page_name = 'faq' THEN true ELSE false END) AS FAQ_IS_VIEWED,
       MAX(CASE WHEN page_name = 'plant_care' THEN true ELSE false END) AS PLANT_CARE_IS_VIEWED,
       MAX(CASE WHEN page_name = 'landing_page' THEN true ELSE false END) AS LANDING_PAGE_IS_VIEWED
    FROM {{ ref('BASE_PAGE_VIEWS') }}
    GROUP BY session_id
),
s AS (
    SELECT session_id,
           IP,
           session_at_ts,
           OS
    FROM ranked_sessions
    WHERE row_n = 1
)

SELECT 
    s.session_id,
    s.IP,
    s.session_at_ts,
    s.OS,
    COALESCE(pv.total_no_page_viewed, 0) AS total_no_page_viewed,
    pv.SHOP_PLANTS_IS_VIEWED,
    pv.CART_IS_VIEWED,
    pv.FAQ_IS_VIEWED,
    pv.PLANT_CARE_IS_VIEWED,
    pv.LANDING_PAGE_IS_VIEWED,
    COALESCE(iv.total_no_item_viewed, 0) AS total_no_item_viewed,
    CASE WHEN o.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS "order",
    CASE WHEN iv.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS "item_viewed"
FROM 
    s
LEFT JOIN 
    pv ON s.session_id = pv.session_id
LEFT JOIN 
    iv ON s.session_id = iv.session_id
LEFT JOIN 
    o ON s.session_id = o.session_id