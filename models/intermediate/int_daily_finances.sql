SELECT
    COALESCE(e.date, r.date) AS date,
    COALESCE(e.total_expenses, 0) AS total_expenses,
    COALESCE(r.net_revenue, 0) AS net_revenue
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
ORDER BY COALESCE(e.date, r.date)
