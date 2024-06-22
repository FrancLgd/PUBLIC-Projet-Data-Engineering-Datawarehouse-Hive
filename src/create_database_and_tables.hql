-- Pour la base

DROP DATABASE IF EXISTS freshness CASCADE;
CREATE DATABASE freshness;

-- Pour la table aisles

CREATE TABLE IF NOT EXISTS freshness.aisles_csv (
  aisle_id INT,
  aisle STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/admin/hive-dataset/aisles.csv' INTO TABLE freshness.aisles_csv;

CREATE TABLE freshness.aisles LIKE freshness.aisles_csv STORED AS ORC;

INSERT INTO TABLE freshness.aisles
SELECT * FROM freshness.aisles_csv;

DROP TABLE freshness.aisles_csv;

-- Pour la table departments

CREATE TABLE IF NOT EXISTS freshness.departments_csv (
  department_id INT,
  department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/admin/hive-dataset/departments.csv' INTO TABLE freshness.departments_csv;

CREATE TABLE freshness.departments LIKE freshness.departments_csv STORED AS ORC;

INSERT INTO TABLE freshness.departments
SELECT * FROM freshness.departments_csv;

DROP TABLE freshness.departments_csv;

-- Pour la table products

CREATE TABLE IF NOT EXISTS freshness.products_csv (
  product_id INT,
  product_name STRING,
  aisle_id INT,
  department_id INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/admin/hive-dataset/products.csv' INTO TABLE freshness.products_csv;

CREATE TABLE freshness.products LIKE freshness.products_csv STORED AS ORC;

INSERT OVERWRITE TABLE freshness.products
SELECT * FROM freshness.products_csv;

DROP TABLE freshness.products_csv;

-- Pour la table orders

CREATE TABLE IF NOT EXISTS freshness.orders_csv (
  order_id INT,
  user_id INT,
  eval_set INT,
  order_number INT,
  order_dow INT,
  days_since_prior_order INT,
  order_hour_of_day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/admin/hive-dataset/order_products.csv' INTO TABLE freshness.orders_csv;

CREATE TABLE freshness.orders LIKE freshness.orders_csv STORED AS ORC;

INSERT OVERWRITE TABLE freshness.orders
SELECT * FROM freshness.orders_csv;

DROP TABLE freshness.orders_csv;

-- Pour la table order_products

CREATE TABLE IF NOT EXISTS freshness.order_products_csv (
  order_id INT,
  product_id INT,
  add_to_cart_order INT,
  reordered INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/admin/hive-dataset/order_products.csv' INTO TABLE freshness.order_products_csv;

CREATE TABLE freshness.order_products LIKE freshness.order_products_csv STORED AS ORC;

INSERT INTO TABLE freshness.order_products
SELECT * FROM freshness.order_products_csv;

DROP TABLE freshness.order_products_csv;