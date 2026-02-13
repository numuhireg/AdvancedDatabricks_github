import dlt
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

## you need to create the view to transform the data the we will use that view as the source of the silver table 
# now we work with customer table Upcase

@dlt.view(
    name = "customers_enr_view"
)
def sales_stg_transform():
    df = spark.readstream.table("customers_stg")    
    df = df.withColumn("customer_name",upper(col("customer_name")))
    df = df.withColumn("customer_address",lower(col("customer_address")))
    df = df.withColumn("customer_city",upper(col("customer_city")))
    return df 
 
 
 # creating destination silver table
dlt.create_steaming_table (
    name = "customers_enr"
)

dlt.create_auto_cdc_flow(
    target = "customers_enr",
    source = "customers_enr_view",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None
)

