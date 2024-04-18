-- models/joined_and_numbered.sql

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

JoinedAndNumbered AS (
    SELECT 
        bo.ORDER_ID, 
        bo.SHIPPING_ADDRESS, 
        bo.STATE, 
        bo.CLIENT_NAME, 
        bo.PAYMENT_METHOD, 
        bo.ORDER_AT_TS,
        COALESCE(br.IS_REFUNDED, 'not return') as IS_REFUNDED,
        ROW_NUMBER() OVER (PARTITION BY bo.ORDER_ID ORDER BY bo.ORDER_AT_TS) AS join_rn
    FROM OrderedBaseOrders bo
    LEFT JOIN {{ ref('BASE_RETURN') }} br ON bo.ORDER_ID = br.ORDER_ID
    WHERE bo.rn = 1
)

SELECT 
    ORDER_ID, 
    SHIPPING_ADDRESS, 
    STATE, 
    CLIENT_NAME, 
    PAYMENT_METHOD, 
    ORDER_AT_TS,
    IS_REFUNDED
FROM JoinedAndNumbered
WHERE join_rn = 1
