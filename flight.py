from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import current_date, current_timestamp, expr, lit, max,col,column 
spark = SparkSession.builder.appName("FlightApp").getOrCreate()
df = spark.read.csv("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/flight-data/csv/2015-summary.csv",inferSchema=True,header=True)
#df.sort("count").show(2)
#x = df.groupBy("DEST_COUNTRY_NAME").count()
#x.show()
#df.select(max("count")).show(3)
#df.select(col("DEST_COUNTRY_NAME"),"DEST_COUNTRY_NAME").show(10)
#df.select(["DEST_COUNTRY_NAME"]).show(10)
#df.selectExpr("DEST_COUNTRY_NAME as Destination" ).show(5)
#df.select(lit("Blink").alias("Today")).show(2)
#df.withColumn("Load_date",expr("current_timestamp")).show(2)
#df.filter(col("DEST_COUNTRY_NAME")== "United States").groupBy("DEST_COUNTRY_NAME").count().show()
#df.groupBy().count().show()
schema = df.schema
newRows = [Row("New Country","Other Country",5),Row("New Country 2","Other Country 3",1)]
parallelizedRows = spark.sparkContext.parallelize(newRows)
df1 = spark.createDataFrame(parallelizedRows,schema)
df.union(df1).where("count = 1").where(col("ORIGIN_COUNTRY_NAME") != "United States").show()