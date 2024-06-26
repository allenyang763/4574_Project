SELECT 
    _FILE,
    _LINE,
    _MODIFIED AS _MODIFIED_TS,
    CAST(REPLACE(HIRE_DATE, 'day ', '') AS DATE) AS hiredate_formatted,
    EMPLOYEE_ID,
    NAME,
    ADDRESS,
    CITY,
    TITLE,
    ANNUAL_SALARY,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS
FROM {{ source('google_drive', 'HR_JOIN') }} 