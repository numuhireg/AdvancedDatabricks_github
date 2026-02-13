import dlt

product_rules = {
    "rule_1": "product_id IS NOT NULL",
    "rule_2": "price >= 0"
}
# ingesting product 

@dlt.table(
name = "products"
)
def products_stg():
    df = spark.readStream.table ("default.bronze.products")
    return df 


    ## for reading file from repository 

  ##df = spark.read.format("delta").load("/mnt/bronze/products")