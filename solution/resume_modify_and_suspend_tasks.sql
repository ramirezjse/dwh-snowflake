
-- resume tasks
alter task CLP.STRUCTURED.DIM_CUSTOMERS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.DIM_CUSTOMERS_TASK resume;
alter task CLP.STRUCTURED.DIM_PRODUCTS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.DIM_PRODUCTS_TASK resume;
alter task CLP.STRUCTURED.DIM_SELLERS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.DIM_SELLERS_TASK resume;
alter task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK resume;
alter task CLP.STRUCTURED.FACT_PAYMENTS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.FACT_PAYMENTS_TASK resume;
alter task CLP.STRUCTURED.FACT_REVIEWS_TASK_UNLOAD resume;
alter task CLP.STRUCTURED.FACT_REVIEWS_TASK resume;


-- check correct task execution

select * from table(CLP.INFORMATION_SCHEMA.TASK_HISTORY())  ;


-- modify tasks with errors

alter task CLP.STRUCTURED.FACT_PAYMENTS_TASK_UNLOAD modify

 as 

copy into @CLP.STRUCTURED.UNLOAD_STG/FACT_PAYMENTS
from CLP.STRUCTURED.FACT_PAYMENTS
partition by ('year=' || to_varchar(year(ORDER_PURCHASE_DATE)))
file_format = (type = CSV)
header = true

;

alter task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK_UNLOAD modify
as 

copy into @CLP.STRUCTURED.UNLOAD_STG/FACT_ORDER_ITEMS
from CLP.STRUCTURED.FACT_ORDER_ITEMS
partition by ('year=' || to_varchar(year(PURCHASE_DATE)))
file_format = (type = CSV)
header = true
;

alter task CLP.STRUCTURED.DIM_CUSTOMERS_TASK_UNLOAD MODIFY

as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_CUSTOMERS
from CLP.STRUCTURED.DIM_CUSTOMERS
file_format = (type = CSV)
overwrite = true
single = true
header = true
;

alter task CLP.STRUCTURED.DIM_SELLERS_TASK_UNLOAD modify

as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_SELLERS
from CLP.STRUCTURED.DIM_SELLERS
file_format = (type = CSV)
overwrite = true
single = true
header = true

;

alter task CLP.STRUCTURED.DIM_PRODUCTS_TASK_UNLOAD modify

as 
copy into @CLP.STRUCTURED.UNLOAD_STG/DIM_PRODUCTS
from CLP.STRUCTURED.DIM_PRODUCTS
file_format = (type = CSV)
overwrite = true
single = true
header = true
;


-- suspend tasks

alter task CLP.STRUCTURED.DIM_CUSTOMERS_TASK suspend;
alter task CLP.STRUCTURED.DIM_CUSTOMERS_TASK_UNLOAD suspend;

alter task CLP.STRUCTURED.DIM_PRODUCTS_TASK suspend;
alter task CLP.STRUCTURED.DIM_PRODUCTS_TASK_UNLOAD suspend;

alter task CLP.STRUCTURED.DIM_SELLERS_TASK suspend;
alter task CLP.STRUCTURED.DIM_SELLERS_TASK_UNLOAD suspend;

alter task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK suspend;
alter task CLP.STRUCTURED.FACT_ORDER_ITEMS_TASK_UNLOAD suspend;

alter task CLP.STRUCTURED.FACT_PAYMENTS_TASK suspend;
alter task CLP.STRUCTURED.FACT_PAYMENTS_TASK_UNLOAD suspend;

alter task CLP.STRUCTURED.FACT_REVIEWS_TASK suspend;
alter task CLP.STRUCTURED.FACT_REVIEWS_TASK_UNLOAD suspend;