select _FIVETRAN_ID,
    SHIPPING_ADDRESS,
    PAYMENT_INFO,
    STATE,
    SESSION_ID,
    CLIENT_NAME,
    SHIPPING_COST,
    PAYMENT_METHOD,
    TAX_RATE,
    PHONE,
    ORDER_AT,
    ORDER_ID,
    _FIVETRAN_DELETED


from {{source('SNOWFLAKE','ORDERS')}}