from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggApp").getOrCreate()
df = spark.read.format("csv").option("inferSchema","True").option("header","True")\
    .load("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/retail-data/all/*.csv")\
        .coalesce(5)
df.cache()
print(df.count()) 

# Simple grouping




# group by

# Window

# grouping set

# roll up

# cube

