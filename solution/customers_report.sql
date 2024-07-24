--They want to know who are the best customers and the product categories that they most buy.

with customer_sales as (

    select 
        c.customer_unique_id,
        p.product_category,
        sum(oi.total_value) as sales

    from 
        fact_order_items oi
    join 
        dim_customers c 
        on oi.customer_id = c.customer_id
    join
        dim_products p
        on oi.product_id = p.product_id
    group by 1,2
    order by 3 desc
)

,best_customers as (

select 
    customer_unique_id,
    sum(sales) as sales

from customer_sales
group by 1
order by 2 desc
)

, ranked_categories as (

    select 
        *,
        DENSE_RANK() over (partition by customer_unique_id order by sales desc) as ranking

    from customer_sales

)

, top_categories as (

    select 
        customer_unique_id,
        product_category,
        sales 

    from ranked_categories
    where ranking = 1
    order by 3 desc 
)

select 
    bc.customer_unique_id,
    bc.sales as customer_lifetime_value,
    tc.product_category as top_categories,
    tc.sales as category_sales

from 
    best_customers bc 
join 
    top_categories tc 
    on bc.customer_unique_id = tc.customer_unique_id


order by customer_lifetime_value desc
;



--In which cities are the most sales?




    select 
        c.customer_city as city,
        c.customer_state as state,
        sum(oi.total_value) as sales

    from 
        fact_order_items oi
    join 
        dim_customers c 
        on oi.customer_id = c.customer_id
    group by 1,2
    order by 3 desc 
;


--What are the most used payment types? Also, what are the payments types that collect the most money?

select 
    payment_type,
    sum(payment_value) as amout_paid,
    count(*) as num_of_payments

from fact_payments

group by 1
order by 2 desc 

;






