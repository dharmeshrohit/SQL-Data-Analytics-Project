/*
===============================================================================
Part-to-Whole Analysis
===============================================================================
Purpose:
    - To compare performance or metrics across dimensions or time periods.
    - To evaluate differences between categories.
    - Useful for A/B testing or regional comparisons.

SQL Functions Used:
    - SUM(), AVG(): Aggregates values for comparison.
    - Window Functions: SUM() OVER() for total calculations.
===============================================================================
*/
-- Which categories contribute the most to overall sales?
with category_sales as (
    select
        p.category,
        sum(f.sales_amount) as total_sales
    from gold.fact_sales f
    left join gold.dim_products p
        on p.product_key = f.product_key
    group by p.category
)
select
    category,
    total_sales,
    sum(total_sales) over () as overall_sales,
    round((total_sales::numeric / sum(total_sales) over ()) * 100, 2) as percentage_of_total
from category_sales
order by total_sales desc;

