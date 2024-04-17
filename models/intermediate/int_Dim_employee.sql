WITH join_data AS (
    SELECT 
        EMPLOYEE_ID,
        HIREDATE_FORMATTED as hire_date,
        NAME,
        ADDRESS,
        CITY,
        TITLE,
        ANNUAL_SALARY
    FROM {{ ref('BASE_HRJOIN') }}
),
quit_data AS (
    SELECT 
        EMPLOYEE_ID,
        QUITDATE AS quit_date
    FROM {{ ref('BASE_HRQUITS') }}
)
SELECT
    jd.EMPLOYEE_ID,
    jd.hire_date,
    jd.NAME,
    jd.ADDRESS,
    jd.CITY,
    jd.TITLE,
    jd.ANNUAL_SALARY,
    qd.quit_date
FROM join_data jd
LEFT JOIN quit_data qd
ON jd.EMPLOYEE_ID = qd.EMPLOYEE_ID
