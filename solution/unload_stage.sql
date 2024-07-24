create stage clp.structured.unload_stg
    storage_integration = CLP_STORE_INTEGRATION
    url = 's3://clp-data-warehouse-snowflake-sebastian-ramirez-data-lake/structured-zone/';

show stages;