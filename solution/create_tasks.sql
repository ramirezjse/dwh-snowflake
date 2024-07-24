--DIMENSIONS TABLES TASKS

create task CLP.STRUCTURED.DIM_CUSTOMERS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.CUSTOMERS_STREAM')
as
merge into CLP.STRUCTURED.DIM_CUSTOMERS c
using (
    with dim_location as (
    
    select distinct
        geolocation_zip_code_prefix as zip_code_prefix,
        first_value(geolocation_city) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as city,
        first_value(geolocation_state) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as state,
        first_value(geolocation_lat) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as latitude,
        first_value(geolocation_lng) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as longitude

    from 
        CLP.STAGING.GEOLOCATION 
    )

    select
        c.customer_id,
        c.customer_unique_id, -- this needs to be treated as 'customer name'
        coalesce(loc.city, c.customer_city) as customer_city,
        coalesce(loc.state, c.customer_state) as customer_state,
        loc.latitude as customer_latitude,
        loc.longitude as customer_longitude


    from 
        CLP.STAGING.CUSTOMERS_STREAM c
    left join 
        dim_location loc
        on c.customer_zip_code_prefix = loc.zip_code_prefix
    WHERE c.METADATA$ACTION = 'INSERT'
) cs 
on c.customer_id = cs.customer_id
when matched then update set 
    c.customer_unique_id = cs.customer_unique_id,
    c.customer_state = cs.customer_state,
    c.customer_city = cs.customer_city,
    c.customer_latitude = cs.customer_latitude,
    c.customer_longitude = cs.customer_longitude
when not matched then insert values (
    cs.customer_id,
    cs.customer_unique_id,
    cs.customer_state,
    cs.customer_city,
    cs.customer_latitude,
    cs.customer_longitude
);



create task CLP.STRUCTURED.DIM_SELLERS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.SELLERS_STREAM')
as
merge into CLP.STRUCTURED.DIM_SELLERS s
using (
    with dim_location as (
    
    select distinct
        geolocation_zip_code_prefix as zip_code_prefix,
        first_value(geolocation_city) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as city,
        first_value(geolocation_state) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as state,
        first_value(geolocation_lat) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as latitude,
        first_value(geolocation_lng) over (partition by geolocation_zip_code_prefix order by geolocation_zip_code_prefix nulls last) as longitude

    from 
        CLP.STAGING.GEOLOCATION 
    )

    select 
    seller_id,
    coalesce(loc.city, s.seller_city) as seller_city,
    coalesce(loc.state, s.seller_state) as seller_state,
    loc.latitude as seller_latitude,
    loc.longitude as seller_longitude

    from 
        CLP.STAGING.SELLERS_STREAM s
    left join
        dim_location loc
        on s.seller_zip_code_prefix = loc.zip_code_prefix
    WHERE s.METADATA$ACTION = 'INSERT'
) ss

on s.seller_id = ss.seller_id

when matched then update set 
    s.seller_city = ss.seller_city,
    s.seller_state = ss.seller_state,
    s.seller_latitude = ss.seller_latitude,
    s.seller_longitude = ss.seller_longitude
when not matched then insert values (
    ss.seller_id,
    ss.seller_city,
    ss.seller_state,
    ss.seller_latitude,
    ss.seller_longitude
    
);

create task CLP.STRUCTURED.DIM_PRODUCTS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.PRODUCTS_STREAM') or system$stream_has_data('CLP.STAGING.CATEGORY_STREAM') 
as
merge into CLP.STRUCTURED.DIM_PRODUCTS p
using (
    SELECT
    p.product_id,
    coalesce(c.product_category_name_english, p.category_name) as product_category,
    p.product_name_length,
    p.product_desc_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

    FROM
        CLP.STAGING.PRODUCTS_STREAM p 
    LEFT JOIN
        CLP.STAGING.PRODUCT_CATEGORY_NAME_TRANSLATION c 
        ON p.category_name = c.product_category_name
    where p.METADATA$ACTION = 'INSERT'

    UNION

    SELECT
    p.product_id,
    coalesce(c.product_category_name_english, p.category_name) as product_category,
    p.product_name_length,
    p.product_desc_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

    FROM
        CLP.STAGING.PRODUCTS p 
    INNER JOIN
        CLP.STAGING.CATEGORY_STREAM c 
        ON p.category_name = c.product_category_name
    where p.METADATA$ACTION = 'INSERT'
 
) ps 

on p.product_id = ps.product_id

when matched then update set 
   p.product_category = ps.product_category,
   p.product_name_length = ps.product_name_length,
   p.product_desc_length = ps.product_desc_length,
   p.product_photos_qty = ps.product_photos_qty,
   p.product_weight_g = ps.product_weight_g,
   p.product_length_cm = ps.product_length_cm,
   p.product_height_cm = ps.product_height_cm,
   p.product_width_cm = ps.product_width_cm

when not matched then insert values (
   ps.product_id,
   ps.product_category,
   ps.product_name_length,
   ps.product_desc_length,
   ps.product_photos_qty,
   ps.product_weight_g,
   ps.product_length_cm,
   ps.product_height_cm,
   ps.product_width_cm
    
);






-- FACTS TABLES TASKS

create task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.ORDERS_STREAM') AND system$stream_has_data('CLP.STAGING.ORDER_ITEMS_STREAM') 
as
merge into CLP.STRUCTURED.FACT_ORDER_ITEMS oi
using (

    with order_line as (
    
    select
        order_id,
        product_id,
        seller_id,
        shipping_limit_date,
        count(product_id) as quantity,
        avg(price) as unit_price,
        avg(freight_value) as unit_freight_value,
        sum(price) as item_value,
        sum(freight_value) as freight_value
    from CLP.STAGING.ORDER_ITEMS_STREAM
    where METADATA$ACTION = 'INSERT'
    group by 
        order_id,
        product_id,
        seller_id,
        shipping_limit_date
    ),


    order_line_items as (
        
        select
            *,
            row_number() over (partition by order_id order by product_id) as line_item

        from 
            order_line
    )

    select
        o.order_id,
        oi.line_item,
        oi.product_id,
        o.customer_id,
        oi.seller_id,
        o.order_status,
        oi.quantity as product_quantity,
        oi.unit_price,
        oi.unit_freight_value,
        oi.item_value as order_item_value,
        oi.freight_value,
        oi.item_value + oi.freight_value as total_value,
        o.order_purchase_timestamp as purchase_date,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        oi.shipping_limit_date

    from
        CLP.STAGING.ORDERS_STREAM o
    join
        order_line_items oi
        on o.order_id = oi.order_id
    where o.METADATA$ACTION = 'INSERT'

) ois 

on (oi.order_id = ois.order_id and oi.line_item = ois.line_item)

when matched then update set 
        oi.product_id = ois.product_id,
        oi.customer_id = ois.customer_id,
        oi.seller_id = ois.seller_id,
        oi.order_status = ois.order_status,
        oi.product_quantity = ois.product_quantity,
        oi.unit_price = ois.unit_price,
        oi.unit_freight_value = ois.unit_freight_value,
        oi.order_item_value = ois.order_item_value,
        oi.freight_value = ois.freight_value,
        oi.total_value = ois.total_value,
        oi.purchase_date = ois.purchase_date,
        oi.order_approved_at = ois.order_approved_at,
        oi.order_delivered_carrier_date = ois.order_delivered_carrier_date,
        oi.order_delivered_customer_date = ois.order_delivered_customer_date,
        oi.order_estimated_delivery_date = ois.order_estimated_delivery_date,
        oi.shipping_limit_date = ois.shipping_limit_date

when not matched then insert values (
        ois.order_id,
        ois.line_item,
        ois.product_id,
        ois.customer_id,
        ois.seller_id,
        ois.order_status,
        ois.product_quantity,
        ois.unit_price,
        ois.unit_freight_value,
        ois.order_item_value,
        ois.freight_value,
        ois.total_value,
        ois.purchase_date,
        ois.order_approved_at,
        ois.order_delivered_carrier_date,
        ois.order_delivered_customer_date,
        ois.order_estimated_delivery_date,
        ois.shipping_limit_date
    
);

create task CLP.STRUCTURED.FACT_REVIEWS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.REVIEWS_STREAM')
as
merge into CLP.STRUCTURED.FACT_REVIEWS r
using (

    with order_line as (
    
    select
        order_id,
        product_id,
        seller_id,
        shipping_limit_date,
        count(product_id) as quantity,
        avg(price) as unit_price,
        avg(freight_value) as unit_freight_value,
        sum(price) as item_value,
        sum(freight_value) as freight_value
    from CLP.STAGING.ORDER_ITEMS
    group by 
        order_id,
        product_id,
        seller_id,
        shipping_limit_date
    ),

    order_line_items as (
        
        select
            *,
            row_number() over (partition by order_id order by product_id) as line_item

        from 
            order_line
    )

    select
    r.review_id || r.order_id || oi.product_id as order_product_review_id,
    r.*,
    o.customer_id,
    oi.product_id,
    oi.seller_id

    from 
        CLP.STAGING.REVIEWS_STREAM r
    join
        CLP.STAGING.ORDERS o
        on r.order_id = o.order_id
    join
        order_line_items oi
        on o.order_id = oi.order_id
    where r.METADATA$ACTION = 'INSERT'


) rs

on r.order_product_review_id = rs.order_product_review_id

when matched then update set 
        r.review_score = rs.review_score,
        r.review_comment_title = rs.review_comment_title,
        r.review_comment_message = rs.review_comment_message,
        r.review_creation_date = rs.review_creation_date,
        r.review_answer_timestamp = rs.review_answer_timestamp,
        r.customer_id = rs.customer_id,
        r.seller_id = rs.seller_id

when not matched then insert values (
        rs.order_product_review_id,
        rs.review_id,
        rs.order_id,
        rs.review_score,
        rs.review_comment_title,
        rs.review_comment_message,
        rs.review_creation_date,
        rs.review_answer_timestamp,
        rs.customer_id,
        rs.product_id,
        rs.seller_id
    
);

create task CLP.STRUCTURED.FACT_PAYMENTS_TASK 
schedule = '1 minute'
when
system$stream_has_data('CLP.STAGING.PAYMENTS_STREAM')
as
merge into CLP.STRUCTURED.FACT_PAYMENTS p
using (
    select 
    p.order_id || '-' || p.payment_sequential as payment_id,
    p.*,
    o.customer_id,
    date(o.order_purchase_timestamp) as order_purchase_date


    from 
        CLP.STAGING.PAYMENTS_STREAM p
    join
        CLP.STAGING.ORDERS o 
        on p.order_id = o.order_id
    where p.METADATA$ACTION = 'INSERT'
) ps 

on p.payment_id = ps.payment_id

when matched then update set 
        p.PAYMENT_TYPE = ps.PAYMENT_TYPE,
        p.PAYMENT_INSTALLMENTS = ps.PAYMENT_INSTALLMENTS,
        p.PAYMENT_VALUE = ps.PAYMENT_VALUE,
        p.CUSTOMER_ID = ps.CUSTOMER_ID,
        p.ORDER_PURCHASE_DATE = ps.ORDER_PURCHASE_DATE
        

when not matched then insert values (
        ps.PAYMENT_ID,
        ps.ORDER_ID,
        ps.PAYMENT_SEQUENTIAL,
        ps.PAYMENT_TYPE,
        ps.PAYMENT_INSTALLMENTS,
        ps.PAYMENT_VALUE,
        ps.CUSTOMER_ID,
        ps.ORDER_PURCHASE_DATE
    
);


-- Unloading tasks

create task CLP.STRUCTURED.DIM_CUSTOMERS_TASK_UNLOAD
after CLP.STRUCTURED.DIM_CUSTOMERS_TASK 
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_CUSTOMERS
from CLP.STRUCTURED.DIM_CUSTOMERS
file_format = (type = CSV)
overwrite = true
single = true
header = true
;

create task CLP.STRUCTURED.DIM_SELLERS_TASK_UNLOAD
after CLP.STRUCTURED.DIM_SELLERS_TASK
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_SELLERS
from CLP.STRUCTURED.DIM_SELLERS
file_format = (type = CSV)
overwrite = true
single = true
header = true
;

create task CLP.STRUCTURED.DIM_PRODUCTS_TASK_UNLOAD
after CLP.STRUCTURED.DIM_PRODUCTS_TASK
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_PRODUCTS
from CLP.STRUCTURED.DIM_PRODUCTS
file_format = (type = CSV)
overwrite = true
single = true
header = true
;

create task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK_UNLOAD
after CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK 
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/FACT_ORDER_ITEMS
from CLP.STRUCTURED.FACT_ORDER_ITEMS
partition by ('year=' || to_varchar(year(PURCHASE_DATE)))
file_format = (type = CSV)
header = true
;

create task CLP.STRUCTURED.FACT_REVIEWS_TASK_UNLOAD
after CLP.STRUCTURED.FACT_REVIEWS_TASK 
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/FACT_REVIEWS
from CLP.STRUCTURED.FACT_REVIEWS
file_format = (type = CSV)
overwrite = true
header = true
;

create task CLP.STRUCTURED.FACT_PAYMENTS_TASK_UNLOAD
after CLP.STRUCTURED.FACT_PAYMENTS_TASK 
as 
copy into @CLP.STRUCTURED.UNLOAD_STG/FACT_PAYMENTS
from CLP.STRUCTURED.FACT_PAYMENTS
partition by ('year=' || to_varchar(year(ORDER_PURCHASE_DATE)))
file_format = (type = CSV)
header = true
;


