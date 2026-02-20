-- reconciliation.sql

WITH 

clean_success_payments AS (
  SELECT 
    payment_id,
    order_id,
    amount_cents,
    attempted_at,
    attempt_no,
    ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY attempted_at DESC NULLS LAST, attempt_no DESC NULLS LAST
    ) AS rn
  FROM payments
  WHERE status = 'SUCCESS'
    AND order_id IS NOT NULL
),

deduped_success AS (
  SELECT * 
  FROM clean_success_payments 
  WHERE rn = 1
),

successful_orders AS (
  SELECT 
    o.order_id,
    o.order_total_cents,
    o.currency,
    o.is_test,
    o.created_at
  FROM orders o
  INNER JOIN deduped_success dsp 
    ON o.order_id = dsp.order_id
  WHERE COALESCE(o.is_test, 0) = 0   
),

internal_sales AS (
  SELECT 
    COUNT(*) AS successful_orders_count,
    SUM(order_total_cents) / 100.0 AS total_internal_sales_usd
  FROM successful_orders
),

orphans AS (
  SELECT 
    COUNT(*) AS orphan_count,
    SUM(amount_cents) / 100.0 AS orphan_total_usd
  FROM payments
  WHERE status = 'SUCCESS' 
    AND order_id IS NULL
),

bank_settlements_total AS (
  SELECT 
    SUM(settled_amount_cents) / 100.0 AS total_bank_settlements_usd
  FROM bank_settlements
  WHERE status = 'SETTLED'
),

final_report AS (
  SELECT 
    i.successful_orders_count,
    i.total_internal_sales_usd,
    o.orphan_count,
    o.orphan_total_usd,
    b.total_bank_settlements_usd,
    (COALESCE(i.total_internal_sales_usd, 0) - COALESCE(b.total_bank_settlements_usd, 0)) AS discrepancy_gap_usd,
    CASE 
      WHEN i.total_internal_sales_usd > b.total_bank_settlements_usd 
        THEN 'Internal > Bank (possible un-settled revenue or missing bank records)'
      WHEN i.total_internal_sales_usd < b.total_bank_settlements_usd 
        THEN 'Bank > Internal (likely due to orphan payments or extra settlements)'
      ELSE 'Totals match perfectly'
    END AS gap_explanation
  FROM internal_sales i,
       orphans o,
       bank_settlements_total b
)

SELECT 
  successful_orders_count,
  ROUND(total_internal_sales_usd, 2)          AS total_internal_sales_usd,
  orphan_count,
  ROUND(orphan_total_usd, 2)                  AS orphan_total_usd,
  ROUND(total_bank_settlements_usd, 2)        AS total_bank_settlements_usd,
  ROUND(discrepancy_gap_usd, 2)               AS discrepancy_gap_usd,
  gap_explanation
FROM final_report;