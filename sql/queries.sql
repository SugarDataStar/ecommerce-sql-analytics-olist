/* =========================================================
   OLIST SQL – Works with the 5 uploaded tables
   Engine: DuckDB or Postgres (DATE_TRUNC, INTERVAL math)
   ========================================================= */

/* 0) Quick sanity checks */
SELECT COUNT(*) AS n_orders FROM olist_orders_dataset;
SELECT COUNT(*) AS n_customers FROM olist_customers_dataset;
SELECT COUNT(*) AS n_payments FROM olist_order_payments_dataset;
SELECT COUNT(*) AS n_reviews FROM olist_order_reviews_dataset;

/* 1) Orders & unique customers by month (basic KPI) */
WITH m AS (
  SELECT
    DATE_TRUNC('month', order_purchase_timestamp) AS month,
    COUNT(*)                                       AS orders,
    COUNT(DISTINCT customer_id)                    AS unique_customers
  FROM olist_orders_dataset
  GROUP BY 1
)
SELECT month, orders, unique_customers
FROM m
ORDER BY month;

/* 2) Delivery performance (delivered only) */
SELECT
  DATE_TRUNC('month', order_purchase_timestamp) AS month,
  AVG(order_delivered_customer_date - order_purchase_timestamp) AS avg_days_purchase_to_delivery,
  AVG(order_estimated_delivery_date - order_delivered_customer_date)      AS avg_days_early_vs_eta
FROM olist_orders_dataset
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

/* 3) Order status mix over time */
SELECT
  DATE_TRUNC('month', order_purchase_timestamp) AS month,
  order_status,
  COUNT(*) AS n_orders
FROM olist_orders_dataset
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

/* 4) Revenue (sum of payments) by month */
SELECT
  DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
  SUM(p.payment_value)                            AS revenue
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset p USING(order_id)
GROUP BY 1
ORDER BY 1;

/* 5) Payment method mix + avg installments */
SELECT
  p.payment_type,
  COUNT(*)                           AS n_payments,
  SUM(p.payment_value)               AS total_value,
  AVG(NULLIF(p.payment_installments,0)) AS avg_installments
FROM olist_order_payments_dataset p
GROUP BY 1
ORDER BY total_value DESC;

/* 6) Review quality by month (avg score) */
SELECT
  DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
  AVG(r.review_score)                             AS avg_review_score,
  COUNT(*)                                        AS n_reviews
FROM olist_order_reviews_dataset r
JOIN olist_orders_dataset o USING(order_id)
GROUP BY 1
ORDER BY 1;

/* 7) Delivery delay vs review score (simple buckets) */
WITH delays AS (
  SELECT
    r.review_score,
    (o.order_delivered_customer_date - o.order_purchase_timestamp) AS delivery_days
  FROM olist_order_reviews_dataset r
  JOIN olist_orders_dataset o USING(order_id)
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
),
buckets AS (
  SELECT
    CASE
      WHEN delivery_days < 3        THEN 'under 3d'
      WHEN delivery_days < 7        THEN '3–6d'
      WHEN delivery_days < 14       THEN '7–13d'
      WHEN delivery_days < 30       THEN '14–29d'
      ELSE '30d+'
    END AS delay_bucket,
    review_score
  FROM delays
)
SELECT
  delay_bucket,
  AVG(review_score) AS avg_review_score,
  COUNT(*)          AS n_reviews
FROM buckets
GROUP BY 1
ORDER BY
  CASE delay_bucket
    WHEN 'under 3d' THEN 1
    WHEN '3–6d'     THEN 2
    WHEN '7–13d'    THEN 3
    WHEN '14–29d'   THEN 4
    ELSE 5
  END;

/* 8) New vs returning customers per month (by customer_unique_id) */
WITH first_order AS (
  SELECT
    c.customer_unique_id,
    MIN(o.order_purchase_timestamp) AS first_purchase_ts
  FROM olist_customers_dataset c
  JOIN olist_orders_dataset o USING(customer_id)
  GROUP BY 1
),
orders_with_flag AS (
  SELECT
    o.order_id,
    DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
    (o.order_purchase_timestamp =
     (SELECT first_purchase_ts FROM first_order f
      JOIN olist_customers_dataset c2 USING(customer_unique_id)
      WHERE c2.customer_id = o.customer_id)
    ) AS is_first_order
  FROM olist_orders_dataset o
)
SELECT
  month,
  SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END)                 AS new_customers_orders,
  SUM(CASE WHEN NOT is_first_order THEN 1 ELSE 0 END)             AS returning_customers_orders
FROM orders_with_flag
GROUP BY 1
ORDER BY 1;

/* 9) Cohort retention (0–12 months) */
WITH firsts AS (
  SELECT
    c.customer_unique_id,
    DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS cohort_month
  FROM olist_customers_dataset c
  JOIN olist_orders_dataset o USING(customer_id)
  GROUP BY 1
),
activity AS (
  SELECT
    c.customer_unique_id,
    DATE_TRUNC('month', o.order_purchase_timestamp) AS activity_month
  FROM olist_customers_dataset c
  JOIN olist_orders_dataset o USING(customer_id)
),
joined AS (
  SELECT
    a.customer_unique_id,
    f.cohort_month,
    a.activity_month,
    12 * (EXTRACT(YEAR FROM a.activity_month) - EXTRACT(YEAR FROM f.cohort_month))
      + (EXTRACT(MONTH FROM a.activity_month) - EXTRACT(MONTH FROM f.cohort_month)) AS month_offset
  FROM activity a
  JOIN firsts f USING(customer_unique_id)
)
SELECT
  cohort_month,
  month_offset,
  COUNT(DISTINCT customer_unique_id) AS active_customers
FROM joined
WHERE month_offset BETWEEN 0 AND 12
GROUP BY 1,2
ORDER BY 1,2;

/* 10) Geography – top customer states by order volume */
SELECT
  c.customer_state,
  COUNT(*) AS n_orders
FROM olist_orders_dataset o
JOIN olist_customers_dataset c USING(customer_id)
GROUP BY 1
ORDER BY n_orders DESC
LIMIT 15;

/* 11) Product catalog overview (no order_items available) */
SELECT
  COALESCE(product_category_name,'unknown') AS category,
  COUNT(*)   AS n_products
FROM olist_products_dataset
GROUP BY 1
ORDER BY n_products DESC
LIMIT 20;
