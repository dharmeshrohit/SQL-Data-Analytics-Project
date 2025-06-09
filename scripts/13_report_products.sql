-- ================================================================================
-- product report
-- ================================================================================
-- purpose:
--   this report consolidates key product metrics and behaviors.
--
-- highlights:
--   1. gathers essential fields such as product name, category, subcategory, and cost.
--   2. segments products by revenue to identify high-performers, mid-range, or low-performers.
--   3. aggregates product-level metrics:
--      - total orders
--      - total sales
--      - total quantity sold
--      - total customers (unique)
--      - lifespan (in months)
--   4. calculates valuable kpis:
--      - recency (months since last sale)
--      - average order revenue (aor)
--      - average monthly revenue
-- ================================================================================

drop view if exists gold.report_products;

create view gold.report_products as

with base_query as (
    -- base query: retrieves core columns from fact_sales and dim_products
    select
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    from gold.fact_sales f
    left join gold.dim_products p
        on f.product_key = p.product_key
    where f.order_date is not null
),

product_aggregations as (
    -- product aggregations: summarizes key metrics at the product level
    select
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        date_part('month', age(max(order_date), min(order_date))) as lifespan,
        max(order_date) as last_sale_date,
        count(distinct order_number) as total_orders,
        count(distinct customer_key) as total_customers,
        sum(sales_amount) as total_sales,
        sum(quantity) as total_quantity,
        round(avg(sales_amount::numeric / nullif(quantity, 0)), 1) as avg_selling_price
    from base_query
    group by
        product_key,
        product_name,
        category,
        subcategory,
        cost
)

select 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    date_part('month', age(current_date, last_sale_date)) as recency_in_months,
    case
        when total_sales > 50000 then 'high-performer'
        when total_sales >= 10000 then 'mid-range'
        else 'low-performer'
    end as product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    case 
        when total_orders = 0 then 0
        else total_sales / total_orders
    end as avg_order_revenue,
    case
        when lifespan = 0 then total_sales
        else total_sales / lifespan
    end as avg_monthly_revenue
from product_aggregations;
