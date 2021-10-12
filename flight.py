from pyspark.sql import SparkSession
from pyspark.sql.functions import max
spark = SparkSession.builder.appName("FlightApp").getOrCreate()
df = spark.read.csv("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/flight-data/csv/2015-summary.csv",inferSchema=True,header=True)
df.sort("count").show(2)
#x = df.groupBy("DEST_COUNTRY_NAME").count()
#x.show()
df.select(max("count")).show(3)