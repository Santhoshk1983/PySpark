from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("dataSources").\
    config('spark.driver.extraClassPath', '/Users/santhoshkumarkannan/Downloads/mysql-connector-java-8.0.15/mysql-connector-java-8.0.15.jar')\
    .getOrCreate()

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

#spark = SparkSession.builder.appName('my_awesome')\
   # .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1')\
   # .getOrCreate()

#conf = SparkConf().set("spark.jars", "/Users/santhoshkumarkannan/Downloads/mysql-connector-java-8.0.15/mysql-connector-java-8.0.15.jar")

#sc = SparkContext( conf=conf)
pushdown = "(select Year,count(1) from Constituency group by Year) a"
dbTable = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
        .option("url","jdbc:mysql://localhost:3306/Election")\
            .option("dbtable",pushdown)\
                .option("user","root")\
                    .option("password","Pioneer*369").load().show()

#Write to DB

#Insert or Update

