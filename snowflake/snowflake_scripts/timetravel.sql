DELETE FROM CUSTOMERS
WHERE customer_id = 'C001';

SELECT *
FROM customers
WHERE customer_id = 'C001';

SELECT *
FROM customers
AT (OFFSET => -60)
WHERE customer_id = 'C001';