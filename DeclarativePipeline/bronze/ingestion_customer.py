import dlt


# customers expactation 
customers_rules = {
    "rule_1": "customer IS NOT NULL",
    "rule_2": "price >= 0
}
# ingesting customers

@dlt.table(
name = "customers"
)
@dlt.expect_all(customers_rules)
#@dp.expect_all_or_drop(customers_rules)
def customers_stg():
    df = spark.readStream.table ("default.bronze.customers")
    return df 