SELECT
    COALESCE(e.date, r.date, rf.returned_at) AS date,
    COALESCE(e.total_expenses, 0) AS total_expenses,
    COALESCE(r.net_revenue, 0) AS income,  
    COALESCE(rf.refund_amount, 0) AS refund_amount,  
    COALESCE(r.net_revenue, 0) - COALESCE(rf.refund_amount, 0)- COALESCE(e.total_expenses, 0) AS net_revenue  
FROM
    (SELECT
        date,
        SUM(EXPENSE_AMOUNT_NUMERIC) AS total_expenses
    FROM {{ ref('BASE_EXPENSES') }}
    GROUP BY date) e
FULL OUTER JOIN
    (SELECT
        DATE(ITEM_VIEW_AT_TS) AS date, 
        (SUM(iv.ADD_TO_CART_QUANTITY * iv.PRICE_PER_UNIT) - SUM(iv.REMOVE_FROM_CART_QUANTITY * iv.PRICE_PER_UNIT)) AS net_revenue
    FROM {{ ref('base_item_views') }} iv
    JOIN {{ ref('BASE_ORDERS') }} o ON iv.session_id = o.session_id
    GROUP BY DATE(ITEM_VIEW_AT_TS)) r
ON e.date = r.date
FULL OUTER JOIN
    (SELECT
        DATE(returned_at) AS returned_at,
        SUM(net_revenue) AS refund_amount
    FROM
        (SELECT
            r.returned_at,
            (SUM(iv.ADD_TO_CART_QUANTITY * iv.PRICE_PER_UNIT) - SUM(iv.REMOVE_FROM_CART_QUANTITY * iv.PRICE_PER_UNIT)) AS net_revenue
        FROM {{ ref('BASE_RETURN') }} r
        JOIN {{ ref('BASE_ORDERS') }} o ON r.order_id = o.order_id
        JOIN {{ ref('base_item_views') }} iv ON o.session_id = iv.session_id
        WHERE r.is_refunded = 'yes'
        GROUP BY r.returned_at, o.order_id
        ) refund_data
    GROUP BY DATE(returned_at)) rf
ON COALESCE(e.date, r.date) = rf.returned_at
ORDER BY COALESCE(e.date, r.date, rf.returned_at)
