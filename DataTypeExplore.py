from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataTypeApp").getOrCreate()

# Boolean Exploration

df = spark.read.csv("/Users/santhoshkumarkannan/Desktop/Learnings/SparkDaBunda/Spark-The-Definitive-Guide-master/data/retail-data/by-day/2010-12-01.csv",inferSchema=True,header=True)
df.printSchema()
df.show(5)
df.select("InvoiceNo","Description").where("InvoiceNo <> 536365").show(5,False)

priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description,"POSTAGE") >=1
df.where (df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

dotCodeFilter = col("StockCode") == "DOT"
df.withColumn("Expensive_Flag",dotCodeFilter & (priceFilter | descripFilter)).where("Expensive_Flag").select("UnitPrice","Expensive_Flag").show(20)


# Number Exploration

df.withColumn("Correction",round(pow(col("Quantity")*col("UnitPrice"),2)+5)).select("CustomerId","Correction").show(5)

df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show()
df.describe().show()
df.stat.crosstab("StockCode","Quantity").show(2)
df.withColumn("serialKey",monotonically_increasing_id()).show(5)


# String Exploration

