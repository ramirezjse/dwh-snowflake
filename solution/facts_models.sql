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
),

fact_order_items as (
    
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
        CLP.STAGING.ORDERS o
    join
        order_line_items oi
        on o.order_id = oi.order_id

),

fact_reviews as (
select
    r.review_id || r.order_id || oi.product_id as order_product_review_id,
    r.*,
    o.customer_id,
    oi.product_id,
    oi.seller_id

from 
    CLP.STAGING.ORDER_REVIEWS r
join
    CLP.STAGING.ORDERS o
    on r.order_id = o.order_id
join
    order_line_items oi
    on o.order_id = oi.order_id

)

;

-- fact_payments
select 
    p.order_id || '-' || p.payment_sequential as payment_id,
    p.*,
    o.customer_id,
    date(o.order_purchase_timestamp) as order_purchase_date


from 
    CLP.STAGING.ORDER_PAYMENTS p
join
    CLP.STAGING.ORDERS o 
    on p.order_id = o.order_id