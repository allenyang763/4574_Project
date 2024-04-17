WITH DistinctBaseOrders AS (
    SELECT ORDER_ID, MIN(ORDER_AT_TS) as FirstOrderTimestamp
    FROM {{ ref('BASE_ORDERS') }}
    GROUP BY ORDER_ID
)

SELECT 
    bo.ORDER_ID as ORDER_ID, 
    bo.SHIPPING_ADDRESS as SHIPPING_ADDRESS, 
    bo.STATE as STATE, 
    bo.CLIENT_NAME as CLIENT_NAME, 
    bo.PAYMENT_METHOD as PAYMENT_METHOD, 
    bo.ORDER_AT_TS as ORDER_AT_TS,
    COALESCE(br.IS_REFUNDED, 'not return') as IS_REFUNDED
FROM {{ ref('BASE_ORDERS') }} as bo
INNER JOIN DistinctBaseOrders as dbo 
    ON bo.ORDER_ID = dbo.ORDER_ID 
    AND bo.ORDER_AT_TS = dbo.FirstOrderTimestamp
LEFT JOIN {{ ref('BASE_RETURN') }} as br 
    ON bo.ORDER_ID = br.ORDER_ID
