from pyspark.sql import SparkSession
spark =SparkSession.builder.appName("Dark").getOrCreate()
myRange = spark.range(1000).toDF("number")
myRange.show()