WITH OrderedBaseOrders AS (
    SELECT 
        ORDER_ID, 
        SHIPPING_ADDRESS, 
        STATE, 
        CLIENT_NAME, 
        PAYMENT_METHOD, 
        ORDER_AT_TS,
        ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY ORDER_AT_TS) AS rn
    FROM {{ ref('BASE_ORDERS') }}
),

IncomeAndRefund AS (
    SELECT 
        o.ORDER_ID, 
        DATE(o.ORDER_AT_TS) AS date,
        SUM(iv.ADD_TO_CART_QUANTITY * iv.PRICE_PER_UNIT - iv.REMOVE_FROM_CART_QUANTITY * iv.PRICE_PER_UNIT) AS income,
        CASE WHEN MAX(br.IS_REFUNDED) = 'yes' THEN SUM(iv.ADD_TO_CART_QUANTITY * iv.PRICE_PER_UNIT - iv.REMOVE_FROM_CART_QUANTITY * iv.PRICE_PER_UNIT) ELSE 0 END AS refund_amount
    FROM {{ ref('BASE_ORDERS') }} o
    JOIN {{ ref('base_item_views') }} iv ON o.SESSION_ID = iv.SESSION_ID
    LEFT JOIN {{ ref('BASE_RETURN') }} br ON o.ORDER_ID = br.ORDER_ID
    GROUP BY o.ORDER_ID, DATE(o.ORDER_AT_TS)
),

JoinedAndNumbered AS (
    SELECT 
        bo.ORDER_ID, 
        bo.SHIPPING_ADDRESS, 
        bo.STATE, 
        bo.CLIENT_NAME, 
        bo.PAYMENT_METHOD, 
        bo.ORDER_AT_TS,
        COALESCE(MAX(br.IS_REFUNDED), 'not return') AS IS_REFUNDED,
        i.income,
        i.refund_amount,
        i.income - i.refund_amount AS net_revenue,
        ROW_NUMBER() OVER (PARTITION BY bo.ORDER_ID ORDER BY bo.ORDER_AT_TS) AS join_rn
    FROM OrderedBaseOrders bo
    LEFT JOIN IncomeAndRefund i ON bo.ORDER_ID = i.ORDER_ID
    LEFT JOIN {{ ref('BASE_RETURN') }} br ON bo.ORDER_ID = br.ORDER_ID
    WHERE bo.rn = 1
    GROUP BY bo.ORDER_ID, bo.SHIPPING_ADDRESS, bo.STATE, bo.CLIENT_NAME, bo.PAYMENT_METHOD, bo.ORDER_AT_TS, i.income, i.refund_amount
)

SELECT 
    ORDER_ID, 
    SHIPPING_ADDRESS, 
    STATE, 
    CLIENT_NAME, 
    PAYMENT_METHOD, 
    ORDER_AT_TS,
    IS_REFUNDED,
    income,
    refund_amount,
    net_revenue
FROM JoinedAndNumbered
WHERE join_rn = 1
