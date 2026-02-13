import dlt
@dlt.table
def silver():
  df = spark.read.table("bronze")
  df = df.filter(df.device == "mobile")
  return df