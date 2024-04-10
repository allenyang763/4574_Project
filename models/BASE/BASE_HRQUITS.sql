SELECT 
    _FILE,
    _LINE,
    _MODIFIED AS _MODIFIED_TS,
    QUIT_DATE AS quitdate,
    EMPLOYEE_ID,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS
FROM {{ source('google_drive', 'HR_QUITS') }} 