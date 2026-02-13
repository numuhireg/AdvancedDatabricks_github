import dlt
from pyspark.sql.functions import *

## you need to create the view to transform the data the we will use that view as the source of the silver table 

@dlt.view(
    name = "sales_enr_view"
)
def sales_stg_transform():
    df = spark.readstream.table("sales_stg")    
    df = df.withColumn("total_amount",col("quantity")*col("amount"))
    return df 
 
 
 # creating destination silver table
dlt.create_steaming_table (
    name = "sales_enr"
)

dlt.create_auto_cdc_flow(
    target = "sales_enr",
    source = "sales_enr_view",
    keys = ["sales_id", "customer_id", "product_id"],
    sequence_by = "sales_timestamp",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None
)

