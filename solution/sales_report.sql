--How are the total sales going per month?
with monthly_sales as (


    select
        d.year,
        d.month_name,
        sum(ifnull(oi.total_value, 0)) as sales

    from 
        CLP.STRUCTURED.DIM_DATE d 
        
    join
        CLP.STRUCTURED.FACT_ORDER_ITEMS oi
        on date(oi.purchase_date) = d.calendar_date

    where oi.order_status not in ('unavailable', 'canceled')
    group by 1,2
   
)

SELECT * 
  FROM monthly_sales
    PIVOT(SUM(sales) FOR month_name IN ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))
      AS p (year, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
  ORDER BY year;

--How are the total sales going comparing the current month with the same month for the former year?

with monthly_sales as (


    select
        d.year,
        d.month,
        d.month_name,
        sum(ifnull(oi.total_value, 0)) as sales

    from 
        CLP.STRUCTURED.DIM_DATE d 
        
    join
        CLP.STRUCTURED.FACT_ORDER_ITEMS oi
        on date(oi.purchase_date) = d.calendar_date

    where oi.order_status not in ('unavailable', 'canceled')
    group by 1,2,3
    order by 2,1
   
)

SELECT
    *,
    lag(sales) over (partition by month order by year) as previous_year_sales,
    case
        when previous_year_sales is null then 0
        else (sales/previous_year_sales)-1 end as growth

FROM monthly_sales
order by month

  ;

-- What are the best-selling products for different price ranges historically?

with price_ranges as (

SELECT 
  oi.*, 
  concat(
    cast(floor(oi.unit_price / 400) * 400 as STRING),
    ' - ',
    cast(floor(oi.unit_price / 400) * 400 + 400 as STRING)
  ) price_range
FROM fact_order_items oi
where oi.order_status not in ('unavailable', 'canceled')
ORDER BY oi.unit_price desc

),

order_qty as (

    select 
        price_range,
        product_id,
        sum(product_quantity) as qty_ordered


    from price_ranges
    group by 1,2
),

selling_order as (
    select distinct
        *,
        ROW_NUMBER() over (partition by price_range order by qty_ordered desc) as row_num

    from order_qty
    order by price_range, row_num
)

select 
    price_range,
    product_id,
    qty_ordered

from selling_order
where row_num = 1
order by price_range

;

-- What are the best-selling product categories per month?

with category_sales as (

    select 
        d.year,
        d.month,
        d.month_name,
        p.product_category,
        sum(oi.total_value) as sales

    from 
        fact_order_items oi 
    join 
        dim_products p 
        on oi.product_id = p.product_id
    join 
        dim_date d 
        on date(oi.purchase_date) = d.calendar_date

    where oi.order_status not in ('unavailable', 'canceled')
    group by 1,2,3,4
    order by 1,2,5 desc
),

ranked_sales as (

    select 
        *,
        DENSE_RANK() over (partition by year,month order by sales desc) as row_num 

    from category_sales
)

select 
    year,
    month,
    month_name,
    product_category,
    sales 
from ranked_sales
where row_num = 1
order by 1,2
;


--What are the product categories with the highest review score per month?

with category_reviews as (

select 
    d.year,
    d.month,
    d.month_name,
    p.product_category,
    AVG(r.review_score) as avg_score,
    count(DISTINCT r.order_product_review_id) as num_of_reviews

from 
    fact_reviews r 
join 
    dim_products p 
    on r.product_id = p.product_id
join 
    dim_date d 
    on date(r.review_answer_timestamp) = d.calendar_date
group by 1,2,3,4
order by 1,2,3,5 desc,6 desc

),

ranked_scores as (

    select 
        *,
        DENSE_RANK() over (partition by year,month order by avg_score desc, num_of_reviews desc) as ranking

    from category_reviews
)
select 
    year,
    month,
    month_name,
    product_category,
    avg_score,
    num_of_reviews

from ranked_scores
where ranking = 1
order by 1,2

