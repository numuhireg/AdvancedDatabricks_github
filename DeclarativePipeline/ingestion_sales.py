import dlt

# expactations rules sales
sales_rules = {"rule_1": "product IS NOT NULL",
"rule_2": "price >= 0",
"rules_3" : "sales_ID not null"}

 # Empty Streamind Table  

dlt.create_streaming_table (
name = "sales_stg",
expect_all =sales_rules
)
 
 # creating East Sales Flow
 
@dlt.append_flow(target = "sales_stg")
def east_sales():
   return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "dbfs:/FileStore/tables/sales_schema").option("cloudFiles.schemaEvolutionMode")
