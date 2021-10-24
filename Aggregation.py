from pyspark.sql import SparkSession
from pyspark.sql import window
from pyspark.sql.functions import count,countDistinct, desc, first, last,col,expr,to_date,dense_rank
from pyspark.sql.window import Window, WindowSpec

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

df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"),expr("count(Quantity)")).show(5)

# Window
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
dfDate = df.withColumn("date",to_date(col("InvoiceDate"),"MM/dd/yyyy"))
windowSpec = Window.partitionBy("CustomerID","date").orderBy(desc("Quantity")).rowsBetween(Window.unboundedPreceding,Window.currentRow)
purchaseDenseRank = dense_rank().over(windowSpec)
dfDate.where("CustomerID is not null").orderBy("CustomerID")\
    .select(col("CustomerID"),col("date"),col("Quantity"),purchaseDenseRank.alias("QuantityDesnseRank")).show(3,False)

# grouping set

    # Available only on Spark SQL. Can be used in Rollup for PySpark


# roll up

dfNoNull = dfDate
rolledUpDF = dfNoNull.rollup("Date", "Country").agg(count("InvoiceNo")).orderBy("Date")
rolledUpDF.show()



