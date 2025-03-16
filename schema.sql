CREATE TABLE raw_customers AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_customers.csv');

CREATE TABLE raw_items AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_items.csv');

CREATE TABLE raw_orders AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_orders.csv');

CREATE TABLE raw_products AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_products.csv');

CREATE TABLE raw_stores AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_stores.csv');

CREATE TABLE raw_supplies AS
SELECT
  *
FROM READ_CSV('examples/jaffle_shop/sources/raw_supplies.csv');

CREATE TABLE stg_customers AS
WITH source AS (
  SELECT
    *
  FROM raw_customers
), renamed AS (
  /* --------  ids */
  SELECT
    id AS customer_id,
    name AS customer_name /* -------- text */
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE stg_order_items AS
WITH source AS (
  SELECT
    *
  FROM raw_items
), renamed AS (
  /* --------  ids */
  SELECT
    id AS order_item_id,
    order_id,
    sku AS product_id
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE stg_orders AS
WITH source AS (
  SELECT
    *
  FROM raw_orders
), renamed AS (
  /* --------  ids */
  SELECT
    id AS order_id,
    store_id AS location_id,
    customer AS customer_id,
    subtotal AS subtotal_cents, /* -------- numerics */
    tax_paid AS tax_paid_cents,
    order_total AS order_total_cents,
    CAST(subtotal_cents AS DOUBLE) / 100.0 AS subtotal,
    CAST(tax_paid_cents AS DOUBLE) / 100.0 AS tax_paid,
    CAST(order_total_cents AS DOUBLE) / 100.0 AS order_total,
    DATE_TRUNC('DAY', ordered_at) AS ordered_at /* -------- timestamps */
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE stg_products AS
WITH source AS (
  SELECT
    *
  FROM raw_products
), renamed AS (
  /* --------  ids */
  SELECT
    sku AS product_id,
    name AS product_name, /* -------- text */
    type AS product_type,
    description AS product_description,
    CAST(price AS DOUBLE) /* -------- numerics */ / 100.0 AS product_price,
    COALESCE(type = 'jaffle', FALSE) AS is_food_item, /* -------- booleans */
    COALESCE(type = 'beverage', FALSE) AS is_drink_item
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE stg_locations AS
WITH source AS (
  SELECT
    *
  FROM raw_stores
), renamed AS (
  /* --------  ids */
  SELECT
    id AS location_id,
    name AS location_name, /* -------- text */
    tax_rate, /* -------- numerics */
    DATE_TRUNC('DAY', opened_at) AS opened_date /* -------- timestamps */
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE stg_supplies AS
WITH source AS (
  SELECT
    *
  FROM raw_supplies
), renamed AS (
  /* --------  ids */
  SELECT
    id || '_' || sku AS supply_uuid,
    id AS supply_id,
    sku AS product_id,
    name AS supply_name, /* -------- text */
    CAST(cost AS DOUBLE) /* -------- numerics */ / 100.0 AS supply_cost,
    perishable AS is_perishable_supply /* -------- booleans */
  FROM source
)
SELECT
  *
FROM renamed;

CREATE TABLE customers AS
WITH customers AS (
  SELECT
    *
  FROM stg_customers
), orders AS (
  SELECT
    *
  FROM stg_orders
), customer_orders_summary AS (
  SELECT
    orders.customer_id,
    COUNT(DISTINCT orders.order_id) AS count_lifetime_orders,
    COUNT(DISTINCT orders.order_id) > 1 AS is_repeat_buyer,
    MIN(orders.ordered_at) AS first_ordered_at,
    MAX(orders.ordered_at) AS last_ordered_at,
    SUM(orders.subtotal) AS lifetime_spend_pretax,
    SUM(orders.tax_paid) AS lifetime_tax_paid,
    SUM(orders.order_total) AS lifetime_spend
  FROM orders
  GROUP BY
    1
), joined AS (
  SELECT
    customers.*,
    customer_orders_summary.count_lifetime_orders,
    customer_orders_summary.first_ordered_at,
    customer_orders_summary.last_ordered_at,
    customer_orders_summary.lifetime_spend_pretax,
    customer_orders_summary.lifetime_tax_paid,
    customer_orders_summary.lifetime_spend,
    CASE WHEN customer_orders_summary.is_repeat_buyer THEN 'returning' ELSE 'new' END AS customer_type
  FROM customers
  LEFT JOIN customer_orders_summary
    ON customers.customer_id = customer_orders_summary.customer_id
)
SELECT
  *
FROM joined;

CREATE TABLE products AS
WITH products AS (
  SELECT
    *
  FROM stg_products
)
SELECT
  *
FROM products;

CREATE TABLE locations AS
WITH locations AS (
  SELECT
    *
  FROM stg_locations
)
SELECT
  *
FROM locations;

CREATE TABLE order_items AS
WITH order_items AS (
  SELECT
    *
  FROM stg_order_items
), orders AS (
  SELECT
    *
  FROM stg_orders
), products AS (
  SELECT
    *
  FROM stg_products
), supplies AS (
  SELECT
    *
  FROM stg_supplies
), order_supplies_summary AS (
  SELECT
    product_id,
    SUM(supply_cost) AS supply_cost
  FROM supplies
  GROUP BY
    1
), joined AS (
  SELECT
    order_items.*,
    orders.ordered_at,
    products.product_name,
    products.product_price,
    products.is_food_item,
    products.is_drink_item,
    order_supplies_summary.supply_cost
  FROM order_items
  LEFT JOIN orders
    ON order_items.order_id = orders.order_id
  LEFT JOIN products
    ON order_items.product_id = products.product_id
  LEFT JOIN order_supplies_summary
    ON order_items.product_id = order_supplies_summary.product_id
)
SELECT
  *
FROM joined;

CREATE TABLE supplies AS
WITH supplies AS (
  SELECT
    *
  FROM stg_supplies
)
SELECT
  *
FROM supplies;

CREATE TABLE orders AS
WITH orders AS (
  SELECT
    *
  FROM stg_orders
), order_items_cte AS (
  SELECT
    *
  FROM order_items
), order_items_summary AS (
  SELECT
    order_id,
    SUM(supply_cost) AS order_cost,
    SUM(product_price) AS order_items_subtotal,
    COUNT(order_item_id) AS count_order_items,
    SUM(CASE WHEN is_food_item THEN 1 ELSE 0 END) AS count_food_items,
    SUM(CASE WHEN is_drink_item THEN 1 ELSE 0 END) AS count_drink_items
  FROM order_items_cte
  GROUP BY
    1
), compute_booleans AS (
  SELECT
    orders.*,
    order_items_summary.order_cost,
    order_items_summary.order_items_subtotal,
    order_items_summary.count_food_items,
    order_items_summary.count_drink_items,
    order_items_summary.count_order_items,
    order_items_summary.count_food_items > 0 AS is_food_order,
    order_items_summary.count_drink_items > 0 AS is_drink_order
  FROM orders
  LEFT JOIN order_items_summary
    ON orders.order_id = order_items_summary.order_id
), customer_order_count AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at ASC NULLS FIRST) AS customer_order_number
  FROM compute_booleans
)
SELECT
  *
FROM customer_order_count;


