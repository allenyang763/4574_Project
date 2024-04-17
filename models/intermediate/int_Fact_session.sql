WITH ranked_orders AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY order_at_ts DESC) AS row_n
    FROM {{ ref('BASE_ORDERS') }}
),
ranked_iv AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY item_view_at_ts DESC) AS row_n
    FROM {{ ref('base_item_views') }}
),
ranked_pv AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY item_view_at_ts DESC) AS row_n
    FROM {{ ref('BASE_PAGE_VIEWS') }}
),
ranked_sessions AS (
    SELECT *,
           row_number() OVER (PARTITION BY session_id ORDER BY session_at_ts DESC) AS row_n
    FROM {{ ref('BASE_SESSIONS') }}
),
iv AS (
    SELECT 
        session_id
    FROM ranked_iv
    WHERE row_n = 1
),
o AS (
    SELECT 
        session_id
    FROM ranked_orders
    WHERE row_n = 1
),
pv AS (
    SELECT session_id,
           page_name
    FROM ranked_pv
    WHERE row_n = 1
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
    pv.page_name,
    CASE WHEN o.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS "order",
    CASE WHEN iv.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS "item_viewed",
FROM 
    s
LEFT JOIN 
    pv ON s.session_id = pv.session_id
LEFT JOIN 
    iv ON s.session_id = iv.session_id
LEFT JOIN 
    o ON s.session_id = o.session_id