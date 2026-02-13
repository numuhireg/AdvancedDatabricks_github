import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating the Materialize business view for our compute which will regroupe dim_customer, dim_product,fact for 
# spesfic KPI 
@dlt.table (
    name = "business_sales"
)
 def business_sales():
     df_fact = spark.read.table("fact_sales")
     df_dimCust = spark.read.table("dim_customers")
     df_dimProd = spark.read.table("dim_products")

     df_join = df_fact.join(df_dimCust,df_fact.customer_id == df_dimCust.customer_id,"inner").join
     (df_dimProd,df_fact.product_id == df_dimProd.product_id,"inner") ##" simple join"
     df_prun = df_join.groupBy("customer_id","customer_name","product_id","product_name","category","region").agg(sum("total_amount").alias("total_amount")) ## group for particular kpi

     df_prun = df_prun.select("customer_id","customer_name","product_id","product_name","category","region","sales")
     return df_prun

     