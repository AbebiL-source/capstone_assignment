SELECT 'orders'           AS table_name, COUNT(*) AS row_count FROM orders
UNION ALL
SELECT 'payments'         AS table_name, COUNT(*) AS row_count FROM payments
UNION ALL
SELECT 'bank_settlements' AS table_name, COUNT(*) AS row_count FROM bank_settlements;