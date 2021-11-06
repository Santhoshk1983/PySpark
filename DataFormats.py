from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFiles").getOrCreate()

# CSV Options

csvFile = spark.read.format("csv")\
    .option("header", "true")\
    .option("mode", "FAILFAST")\
    .option("inferSchema", "true")\
    .load("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/flight-data/csv/2010-summary.csv")

csvFile.printSchema()
csvFile.show(3)

csvFile.write.format("csv").mode("overwrite").option("sep","|").save("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/flight-data/csv/tmp/my-tsv-file.tsv")

# Parquet Options
print("Parquet Files")
parquetFile = spark.read.format("parquet")\
    .load("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/flight-data/parquet/2010-summary.parquet")\
        .show(5)


#jdbc