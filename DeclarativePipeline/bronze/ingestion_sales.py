import dlt


# create the empty streaming table 

@dlt.create_streaming_table(
    name = "append_sales"
)

# creating East flow for Sales Flow

@dlt.append_flow(target = "append_sales")

# creating East Sales Flow
def east_sales():
    df = spark.readstream.table ("default.sales_east")
    return df

# creating West flow for Sales Flow

@dlt.append_flow(target = "append_sales")
def west_sales():
    df = spark.readstream.table ("default.sales_west")
    return df