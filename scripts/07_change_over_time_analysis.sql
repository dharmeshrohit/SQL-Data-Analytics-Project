/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - To track trends, growth, and changes in key metrics over time.
    - For time-series analysis and identifying seasonality.
    - To measure growth or decline over specific periods.

SQL Functions Used:
    - Date Functions: EXTRACT(), DATE_TRUNC(), TO_CHAR()
    - Aggregate Functions: SUM(), COUNT(), AVG()
===============================================================================
*/

-- Analyse sales performance over time
-- Quick Date Functions
select
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    sum(sales_amount) as total_sales,
    count(distinct customer_key) as total_customers,
    sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by extract(year from order_date), extract(month from order_date)
order by extract(year from order_date), extract(month from order_date);


-- DATE_TRUNC()
select
    date_trunc('month', order_date::timestamp) as order_date,
    sum(sales_amount) as total_sales,
    count(distinct customer_key) as total_customers,
    sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by date_trunc('month', order_date::timestamp)
order by date_trunc('month', order_date::timestamp);


-- TO_CHAR()
select
    to_char(order_date, 'YYYY-Mon') as order_date,
    sum(sales_amount) as total_sales,
    count(distinct customer_key) as total_customers,
    sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by to_char(order_date, 'YYYY-Mon')
order by to_char(order_date, 'YYYY-Mon');