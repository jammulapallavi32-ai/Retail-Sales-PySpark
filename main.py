from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import*
spark=SparkSession.builder.appName("Retail Sales Analysis").getOrCreate()
# Read CSV 
df=spark.read.csv("retail_sales.csv",header=True,inferSchema=True)
print("Dataset")
df.show()
# Data Transformations
df = df.withColumn("Total_Amount", col("Quantity") * col("Price"))

# Aggregations
print("Total Sales by City")
df.groupBy("City") \
  .agg(sum("Total_Amount").alias("Sales")) \
  .show()

print("Total Sales by Category")
df.groupBy("Category") \
  .agg(sum("Total_Amount").alias("Sales")) \
  .show()

print("Top Products")
df.groupBy("Product") \
  .agg(sum("Quantity").alias("Qty")) \
  .orderBy(desc("Qty")) \
  .show()

df = df.withColumn("Month", month(col("Order_Date")))

print("Monthly Sales")
df.groupBy("Month") \
  .agg(sum("Total_Amount").alias("Sales")) \
  .show()

print("Top Customer")
df.groupBy("Customer") \
  .agg(sum("Total_Amount").alias("Spent")) \
  .orderBy(desc("Spent")) \
  .show(1)

# Window Specification
windowSpec = Window.orderBy(desc("Total_Amount"))

# Row Number
df_row = df.withColumn("Row_Number", row_number().over(windowSpec))
print("Row Number")
df_row.select("Customer", "Product", "Total_Amount", "Row_Number").show()

# Rank
df_rank = df.withColumn("Rank", rank().over(windowSpec))
print("Rank")
df_rank.select("Customer", "Product", "Total_Amount", "Rank").show()

# Dense Rank
df_dense = df.withColumn("Dense_Rank", dense_rank().over(windowSpec))
print("Dense Rank")
df_dense.select("Customer", "Product", "Total_Amount", "Dense_Rank").show()

# PartitionBy

window_city = Window.partitionBy("City").orderBy(desc("Total_Amount"))

df_city = df.withColumn("City_Rank", row_number().over(window_city))

print("Top Sale in Each City")
df_city.select(
    "City",
    "Customer",
    "Product",
    "Total_Amount",
    "City_Rank"
).show()


#df.write.mode("overwrite").csv("output", header=True)

spark.stop()
