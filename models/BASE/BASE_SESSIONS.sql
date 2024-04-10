select _FIVETRAN_ID,
    SESSION_ID,
    CLIENT_ID,
    IP,
    SESSION_AT,
    OS,
    _FIVETRAN_DELETED,
    _FIVETRAN_SYNCED


from {{source('snowflake','SESSIONS')}}