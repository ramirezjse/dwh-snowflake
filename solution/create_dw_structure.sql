
CREATE OR REPLACE DATABASE CLP;

-- Create staging schema
CREATE OR REPLACE SCHEMA STAGING;
CREATE OR REPLACE SCHEMA STRUCTURED;


--create staging tables

create table staging.products(
    product_id text,
    category_name text,
    product_name_length text,
    product_desc_length text,
    product_photos_qty text,
    product_weight_g number,
    product_length_cm number,
    product_height_cm number,
    product_width_cm number

);

create table staging.order_payments(
    order_id text,
    payment_sequential number,
    payment_type text,
    payment_installments number,
    payment_value number
  
);

create table staging.customers(
    customer_id text,
    customer_unique_id text,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text
);

create table staging.geolocation(
    geolocation_zip_code_prefix text,
    geolocation_lat number,
    geolocation_lng number,
    geolocation_city text,
    geolocation_state text
);

create table staging.order_items(
    order_id text,
    order_item_id number,
    product_id text,
    seller_id text,
    shipping_limit_date timestamp,
    price number,
    freight_value number

);

create table staging.order_reviews(
    review_id text,
    order_id text,
    review_score number,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp

);

create table staging.orders(
    order_id text,
    customer_id text,
    order_status text,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

create table staging.sellers(
    seller_id text,
    seller_zip_code_prefix text,
    seller_city text,
    seller_state text
);

create table staging.product_category_name_translation(
    product_category_name text,
    product_category_name_english text
);


-- Structured schema

create table structured.dim_customers(
    customer_id text primary key,
    customer_unique_id text,
    customer_city text,
    customer_state text,
    customer_latitude number,
    customer_longitude number
);

create table structured.dim_sellers(
    seller_id text primary key, 
    seller_city text,
    seller_state text,
    seller_latitude number,
    seller_longitude number
);

create table structured.dim_products(
    product_id text primary key,
    product_category text,
    product_name_length number,
    product_desc_length number,
    product_photos_qty number,
    product_weight_g number,
    product_length_cm number,
    product_height_cm number,
    product_width_cm number
);

create table structured.dim_date(
    calendar_date date,
    year integer,
    month integer,
    month_name text,
    quarter integer,
    day_of_week integer,
    day_name text,
    week_of_year integer,
    day_of_year integer 
);

create table structured.fact_order_items(
    order_id text,
    line_item integer,
    product_id text foreign key references structured.dim_products(product_id),
    customer_id text foreign key references structured.dim_customers(customer_id),
    seller_id text foreign key references structured.dim_sellers(seller_id),
    order_status text,
    product_quantity number,
    unit_price number,
    unit_freight_value number,
    order_item_value number,
    freight_value number,
    total_value number,
    purchase_date timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    shipping_limit_date timestamp
    );

create table structured.fact_reviews(
    order_product_review_id text,
    review_id text,
    order_id text,
    review_score number,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    customer_id text foreign key references structured.dim_customers(customer_id),
    product_id text foreign key references structured.dim_products(product_id),
    seller_id text foreign key references structured.dim_sellers(seller_id)
);

create table structured.fact_payments(
    payment_id text,
    order_id text,
    payment_sequential number,
    payment_type text,
    payment_installments number,
    payment_value number,
    customer_id text foreign key references structured.dim_customers(customer_id),
    order_purchase_date date
)