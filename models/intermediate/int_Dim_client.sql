with ranked as(
    SELECT *,
ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY ORDER_AT_TS DESC) AS row_n,
ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY ORDER_AT_TS DESC) AS order_n,
FROM {{ ref('BASE_ORDERS') }}
)


select 
r.SESSION_ID,
SHIPPING_ADDRESS,
PAYMENT_INFO,
STATE,
CLIENT_NAME,
SHIPPING_COST,
PAYMENT_METHOD,
TAX_RATE,
PHONE,
ORDER_AT_TS,
ORDER_ID
from ranked r

join {{ ref('BASE_SESSIONS') }} s on r.session_id=s.session_id
where row_n=1 and order_n=1


