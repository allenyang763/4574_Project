SELECT _fivetran_id,
CAST(SESSION_ID AS STRING) AS SESSION_ID,
VIEW_AT AS ITEM_VIEW_AT_TS,
CAST(PAGE_NAME AS STRING) AS PAGE_NAME,
_fivetran_deleted,
_fivetran_synced AS _fivetran_synced_TS
FROM {{SOURCE('snowflake', 'PAGE_VIEWS')}}