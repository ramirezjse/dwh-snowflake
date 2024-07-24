

create stage clp.staging.load_stg
    storage_integration = CLP_STORE_INTEGRATION
    url = 's3://clp-data-warehouse-snowflake-sebastian-ramirez-data-lake/raw-zone/';

show stages;

create pipe clp.staging.customers_pipe auto_ingest=true as 
    copy into CLP.STAGING.CUSTOMERS
    from  @clp.staging.load_stg/olist_customers_dataset.csv 
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );

create pipe clp.staging.geolocation_pipe auto_ingest=true as 
    copy into CLP.STAGING.GEOLOCATION
    from  @clp.staging.load_stg/olist_geolocation_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );


create pipe clp.staging.orders_pipe auto_ingest=true as 
    copy into CLP.STAGING.ORDERS
    from  @clp.staging.load_stg/olist_orders_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );

create pipe clp.staging.order_items_pipe auto_ingest=true as 
    copy into CLP.STAGING.ORDER_ITEMS
    from  @clp.staging.load_stg/olist_order_items_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );

create pipe clp.staging.order_payments_pipe auto_ingest=true as 
    copy into CLP.STAGING.ORDER_PAYMENTS
    from  @clp.staging.load_stg/olist_order_payments_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );


create pipe clp.staging.order_reviews_pipe auto_ingest=true as 
    copy into CLP.STAGING.ORDER_REVIEWS
    from  @clp.staging.load_stg/olist_order_reviews_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );

create pipe clp.staging.products_pipe auto_ingest=true as 
    copy into CLP.STAGING.PRODUCTS
    from  @clp.staging.load_stg/olist_products_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );


create pipe clp.staging.product_category_pipe auto_ingest=true as 
    copy into CLP.STAGING.PRODUCT_CATEGORY_NAME_TRANSLATION
    from  @clp.staging.load_stg/product_category_name_translation.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );


create pipe clp.staging.sellers_pipe auto_ingest=true as 
    copy into CLP.STAGING.SELLERS
    from  @clp.staging.load_stg/olist_sellers_dataset.csv
    file_format = (type = CSV skip_header = 1 field_optionally_enclosed_by = '"' );


show pipes;



select SYSTEM$PIPE_STATUS( 'clp.staging.geolocation_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.customers_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.orders_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.order_items_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.order_payments_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.order_reviews_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.products_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.product_category_pipe')
union all
select SYSTEM$PIPE_STATUS( 'clp.staging.sellers_pipe')
