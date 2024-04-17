SELECT
    date,
    expense_type,
    SUM(EXPENSE_AMOUNT_NUMERIC) AS total_expenses
FROM {{ ref('BASE_EXPENSES') }}
GROUP BY date, expense_type
ORDER BY date, expense_type
