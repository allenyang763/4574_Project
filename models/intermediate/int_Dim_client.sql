


with orders as(
select 
SESSION_ID,
SHIPPING_ADDRESS,
PAYMENT_INFO,
STATE,
CLIENT_NAME,
PAYMENT_METHOD,
PHONE,
ORDER_AT_TS,
ORDER_ID
FROM {{ ref('BASE_ORDERS') }}
),
sessions as(
select
SESSION_ID,
CLIENT_ID,
IP,
SESSION_AT_TS,
OS
FROM {{ ref('BASE_SESSIONS') }}
),
joined as(
select
o.SESSION_ID,
SHIPPING_ADDRESS,
PAYMENT_INFO,
STATE,
CLIENT_NAME,
PAYMENT_METHOD,
PHONE,
ORDER_AT_TS,
ORDER_ID,
CLIENT_ID,
IP,
SESSION_AT_TS,
OS
from orders o
join sessions s on o.session_id=s.session_id
)

, ranked as(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY ORDER_AT_TS DESC NULLS FIRST) AS row_n,
ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY ORDER_AT_TS DESC NULLS FIRST) AS ord_n,
ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY ORDER_AT_TS DESC NULLS FIRST) AS cli_n


FROM joined)

select 
SESSION_ID,
SHIPPING_ADDRESS,
PAYMENT_INFO,
STATE,
CLIENT_NAME,
PAYMENT_METHOD,
PHONE,
ORDER_AT_TS,
ORDER_ID,
CLIENT_ID,
IP,
SESSION_AT_TS,
OS
from ranked
where row_n=1 and ord_n=1 and cli_n=1






