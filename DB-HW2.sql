USE ecommerce_synth;

SELECT * FROM customers LIMIT 100;
SELECT COUNT(*) from customers;
SELECT * FROM order_items LIMIT 100;
SELECT COUNT(*) from order_items;
SELECT * FROM orders LIMIT 100;
SELECT COUNT(*) from orders;
SELECT * FROM products LIMIT 100;
SELECT COUNT(*) from products;

-- AI generated query

USE `ecommerce_synth`;

SELECT
  o.shipping_country,
  p.category,
  COUNT(DISTINCT o.order_id)    AS orders,
  SUM(oi.quantity)              AS units,
  ROUND(SUM(oi.line_total), 2)  AS revenue,
  COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p     ON p.product_id = oi.product_id
JOIN customers c    ON c.customer_id = o.customer_id
WHERE DATE(o.order_date) >= DATE(NOW() - INTERVAL 90 DAY)
GROUP BY o.shipping_country, p.category
ORDER BY revenue DESC
LIMIT 100;

-- optimized query

WITH three_months AS (
	SELECT order_id, customer_id, shipping_country, order_date FROM orders
    WHERE order_date >= CURDATE() - INTERVAL 90 DAY
    ),
	items AS (
	SELECT order_id, product_id, quantity, line_total FROM order_items
    )
SELECT
	tm.shipping_country,
    p.category,
    COUNT(DISTINCT(tm.order_id)) AS order_quantity,
    SUM(i.quantity) AS units,
    SUM(i.line_total) AS revenue,
    COUNT(DISTINCT(tm.customer_ID)) AS customers
FROM three_months tm
JOIN items i ON i.order_id = tm.order_id
JOIN products p ON p.product_id = i.product_id
JOIN customers c ON c.customer_id = tm.customer_id
GROUP BY tm.shipping_country, p.category
ORDER BY revenue DESC
LIMIT 100;