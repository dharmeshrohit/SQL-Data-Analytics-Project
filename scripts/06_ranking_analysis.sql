/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- Which 5 products generate the highest revenue
select
p.product_name,
sum(f.sales_amount) total_revenue
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.product_name
order by total_revenue desc
limit 5;

-- complex but flexible ranking using window functions
select *
from (
    select
        p.product_name,
        sum(f.sales_amount) as total_revenue,
        rank() over (order by sum(f.sales_amount) desc) as rank_products
    from gold.fact_sales f
    left join gold.dim_products p
        on p.product_key = f.product_key
    group by p.product_name
) as ranked_products
where rank_products <= 5;


-- what are the 5 worst performing products in terms of sales
select
p.product_name,
sum(f.sales_amount) total_revenue
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.product_name
order by total_revenue
limit 5;


-- find the top 10 customers who have generated the highest revenue
select 
    c.customer_key,
    c.first_name,
    c.last_name,
    sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_customers c
    on c.customer_key = f.customer_key
group by 
    c.customer_key,
    c.first_name,
    c.last_name
order by total_revenue desc
limit 10;


-- the 3 customers with the fewest orders placed
select 
    c.customer_key,
    c.first_name,
    c.last_name,
    count(distinct order_number) as total_orders
from gold.fact_sales f
left join gold.dim_customers c
    on c.customer_key = f.customer_key
group by 
    c.customer_key,
    c.first_name,
    c.last_name
order by total_orders
limit 3;
