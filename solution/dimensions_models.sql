
-- dim location

with dim_location as (
    
    select distinct
        geolocation_zip_code_prefix as zip_code_prefix,
        first_value(geolocation_city) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as city,
        first_value(geolocation_state) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as state,
        first_value(geolocation_lat) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as latitude,
        first_value(geolocation_lng) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as longitude

    from 
        CLP.STAGING.GEOLOCATION 
),


-- dim customers
dim_customers as (

    select
        c.customer_id,
        c.customer_unique_id, -- this needs to be treated as 'customer name'
        coalesce(loc.city, c.customer_city) as customer_city,
        coalesce(loc.state, c.customer_state) as customer_state,
        loc.latitude as customer_latitude,
        loc.longitude as customer_longitude


    from 
        CLP.STAGING.CUSTOMERS c
    left join 
        dim_location loc
        on c.customer_zip_code_prefix = loc.zip_code_prefix
),

-- dim sellers

dim_sellers as (

select 
    seller_id,
    coalesce(loc.city, s.seller_city) as seller_city,
    coalesce(loc.state, s.seller_state) as seller_state,
    loc.latitude as seller_latitude,
    loc.longitude as sellet_longitude

from 
    CLP.STAGING.SELLERS s
left join
    dim_location loc
    on s.seller_zip_code_prefix = loc.zip_code_prefix
);


-- dim_products

SELECT
    p.product_id,
    coalesce(c.product_category_name_english, p.category_name) as category,
    p.product_name_length,
    p.product_desc_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

FROM
    CLP.STAGING.PRODUCTS p 
LEFT JOIN
    CLP.STAGING.PRODUCT_CATEGORY_NAME_TRANSLATION c 
    ON p.category_name = c.product_category_name

;


-- dim_date

WITH CTE_MY_DATE AS (
SELECT DATEADD(DAY, SEQ4(), '2016-01-01 00:00:00') AS MY_DATE
FROM TABLE(GENERATOR(ROWCOUNT=>5000))
)
SELECT
TO_DATE(MY_DATE) as date
,YEAR(MY_DATE) as year
,MONTH(MY_DATE) as month
,MONTHNAME(MY_DATE) as monthname
,QUARTER(MY_DATE) as quarter
,DAYOFWEEK(MY_DATE) as dayofweek
,DAYNAME(MY_DATE) as dayname
,WEEKOFYEAR(MY_DATE) as weekofyear
,DAYOFYEAR(MY_DATE) as dayofyear

FROM CTE_MY_DATE

;

