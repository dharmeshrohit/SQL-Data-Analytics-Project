/*
===============================================================================
Date Range Exploration 
===============================================================================
Purpose:
    - To determine the temporal boundaries of key data points.
    - To understand the range of historical data.
===============================================================================
*/


-- Find the date of the first and last order
-- How many years & months of sales are available
select 
min(order_date) as first_order_date,
max(order_date) as last_order_date,
extract(year from age(max(order_date), min(order_date))) as order_range_years,
extract(year from age(max(order_date), min(order_date))) * 12 +
extract(month from age(max(order_date), min(order_date))) as order_range_total_months
from gold.fact_sales

-- Find the Youngest and the Oldest customers 
select
min(birthdate) as oldest_birthdate,
extract(year from age(current_date, min(birthdate))) as oldest_age,
max(birthdate) as youngest_birthdate,
extract(year from age(current_date, max(birthdate))) as youngest_age
from gold.dim_customers;
