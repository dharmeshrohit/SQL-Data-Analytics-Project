/*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

SQL Functions Used:
    - COUNT(), SUM(), AVG()
===============================================================================
*/


-- Find the total sales
select sum(sales_amount) as total_sales from gold.fact_sales

-- Find How many items are sold 
select sum(quantity) as total_quantity from gold.fact_sales

-- Find the Average selling prise
select avg(price) as avg_price from gold.fact_sales

-- Find the total numbers of orders
select count(order_number) as total_order from gold.fact_sales
select count(distinct order_number) as total_order from gold.fact_sales

-- Find the total numbers of produsts
select count(product_name) as total_product from gold.dim_products
select count(distinct product_name) as total_product from gold.dim_products

-- Find the total numbers of Customers
select count(customer_key) as total_customers from gold.dim_customers;

-- Find the total numbers of customers that has placed an order
select count(distinct customer_key) as total_customers from gold.fact_sales


-- Generate a report that shows all key metrica of the business
select 'Total Sales' as measure_name, sum(sales_amount) as measure_value from gold.fact_sales
union all
select 'Total quantity', sum(quantity) from gold.fact_sales
union all
select 'Average Price', avg(price) from gold.fact_sales
union all
select 'Total Nr. Orders', count(distinct order_number) from gold.fact_sales
union all
select 'Total Nr. Products', count(distinct product_name) from gold.dim_products
union all
select 'Total Nr. Customers', count(customer_key) from gold.dim_customers;
