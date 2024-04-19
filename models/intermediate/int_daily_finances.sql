WITH DailyOrders AS (
    SELECT
        DATE(ORDER_AT_TS) AS date,
        SUM(income) AS total_income,
        SUM(refund_amount) AS total_refund_amount,
        SUM(net_revenue) AS total_net_revenue
    FROM {{ ref('int_Fact_order') }}
    GROUP BY DATE(ORDER_AT_TS)
),

DailyExpenses AS (
    SELECT
        DATE(DATE) AS date,
        SUM(EXPENSE_AMOUNT_NUMERIC) AS total_expenses
    FROM {{ ref('BASE_EXPENSES') }}
    GROUP BY DATE(DATE)
),

DailyFinance AS (
    SELECT
        COALESCE(do.date, de.date) AS date,
        COALESCE(total_income, 0) AS total_income,
        COALESCE(total_refund_amount, 0) AS total_refund_amount,
        COALESCE(total_net_revenue, 0) AS total_net_revenue,
        COALESCE(total_expenses, 0) AS total_expenses,
        COALESCE(total_net_revenue, 0) - COALESCE(total_expenses, 0) AS net_after_expenses
    FROM DailyOrders do
    FULL OUTER JOIN DailyExpenses de ON do.date = de.date
)

SELECT
    date,
    total_income,
    total_refund_amount,
    total_net_revenue,
    total_expenses,
    net_after_expenses
FROM DailyFinance
ORDER BY date
