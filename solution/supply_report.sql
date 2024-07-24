--Who are the best sellers per city?

with sellers_city as (

    select
        s.seller_city,
        s.seller_id,
        sum(oi.total_value) as sales

    from 
        dim_sellers s
    join 
        fact_order_items oi 
        on s.seller_id = oi.seller_id
    group by 1,2
    order by 1,3 desc
)

, ranked_sellers as (

    select 
        *,
        DENSE_RANK() over (partition by seller_city order by sales desc) as ranking

    from sellers_city
)


select 
    seller_city as city,
    seller_id,
    sales

from ranked_sellers
where ranking = 1
order by 3 desc 

;

-- They want to provission better their inventory based on the expected demand so want to see the peak moments each year.

/* This would be the sales report with the monthly sales per year */

--Where should they open more stores, supply more?


    select top 10
        s.seller_city,
        count(distinct s.seller_id) as num_of_sellers,
        sum(oi.total_value) as sales,
        sales / num_of_sellers as avg_sales_per_seller

    from 
        dim_sellers s
    join 
        fact_order_items oi 
        on s.seller_id = oi.seller_id
    group by 1
    order by 4 desc

;

