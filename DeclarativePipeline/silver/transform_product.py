import dlt
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

## you need to create the view to transform the data the we will use that view as the source of the silver table 
# nw we work with product table 

@dlt.view(
    name = "product_enr_view"
)
def sales_stg_transform():
    df = spark.readstream.table("product_stg")    
    df = df.withColumn("Price",col("price").cast(IntegerType()))
    return df 
 
 
 # creating destination silver table
dlt.create_steaming_table (
    name = "product_enr"
)

dlt.create_auto_cdc_flow(
    target = "product_enr",
    source = "product_enr_view",
    keys = ["product_id", "customer_id", "product_id"],
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

