from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType,IntegerType

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

df.select(initcap(col("Description")),"Description").show(2,False)
regex = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(col("Description"),regex,"COLOR").alias("Color_Clean"),col("Description")).show(3)

df.select(substring("Description",1,5),"Description").show(2,False)


# Date-Time Exploration

dateDF = spark.range(10)\
    .withColumn("today",current_date())\
    .withColumn("now",current_timestamp())

dateDF.select(col("today"),date_add(col("today"),5),date_sub(col("today"),5)).show()

dateFormat = "yyyy-dd-MM"
formattedDate = spark.range(1).select(to_date(lit("1983-30-07"),dateFormat))
formattedDate.show()


# Null Exploration
# Coalesce, ifnull, nullif, nvl, nvl2

df.select(col("Description"),col("CustomerId"),coalesce(col("Description"),col("CustomerId"))).show()

df.show(5,False)


myNullSchema = StructType([StructField("DataValue",IntegerType(),True)])
nullData = [Row(0),Row(2),Row(5),Row(None)]
newData = spark.createDataFrame(nullData,myNullSchema)
newData.select(isnull(col("DataValue"))).show()


# Complex Type Exploration

c=df.select(struct("Description", "InvoiceNo").alias("Struct_Type"))
c.select("Struct_Type.Description").show(3)
df.select(split(col("Description")," ").alias("array_val")).selectExpr("array_val[1]").show(3)
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2,False)
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).selectExpr("complex_map['WHITE METAL LANTERN']").show(2)