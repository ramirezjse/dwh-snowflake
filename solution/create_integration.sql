

CREATE STORAGE INTEGRATION CLP_STORE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::525329760533:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://clp-data-warehouse-snowflake-sebastian-ramirez-data-lake/raw-zone/', 's3://clp-data-warehouse-snowflake-sebastian-ramirez-data-lake/structured-zone/')
;

DESC INTEGRATION CLP_STORE_INTEGRATION