from pyspark.sql import SparkSession
from pyspark.sql.functions import count,countDistinct, desc, first, last,col

spark = SparkSession.builder.appName("aggApp").getOrCreate()
df = spark.read.format("csv").option("inferSchema","True").option("header","True")\
    .load("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/retail-data/all/*.csv")\
        .coalesce(5)
df.cache()
print(df.count()) 

# Simple grouping
df.show(3)
df.select("*").show(5)
df.select(count("StockCode")).show()
df.select(countDistinct("StockCode").alias("UniqueStockCodes")).show()
df.select(first("StockCode"),last("StockCode")).show()

# group by

df.groupBy("Country").agg(countDistinct("CustomerID")).withColumnRenamed("count(CustomerID)","CountOfCustomer")\
    .sort(desc("CountOfCustomer")).show(3)

# Window

# grouping set

# roll up

# cube

