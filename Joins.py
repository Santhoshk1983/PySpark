from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,broadcast
spark = SparkSession.builder.appName("joinApp").getOrCreate()

person = spark.createDataFrame([
      (0, "Bill Chambers", 0, [100]),
      (1, "Matei Zaharia", 1, [500, 250, 100]),
      (2, "Michael Armbrust", 1, [250, 100])])\
    .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")])\
    .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor")])\
.toDF("id", "status")
 
 
 
 



# Inner
joinType ="inner"
joinExpression = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram,joinExpression,joinType).show()

# Left Outer
joinType="left_outer"
joinExpression = person["graduate_program"] == graduateProgram["id"]
graduateProgram.join(person,joinExpression,joinType).show()

# Right Outer

# Left Semi

joinType="left_semi"
joinExpression = person["graduate_program"] == graduateProgram["id"]
graduateProgram.join(person,joinExpression,joinType).show()


# Left Anti
print("Left Anti")
joinType="left_anti"
joinExpression = person["graduate_program"] == graduateProgram["id"]
graduateProgram.join(person,joinExpression,joinType).show()

# Natural 

# Cross
print("Cross Join")
joinType="cross"
joinExpression = person["graduate_program"] == graduateProgram["id"]
graduateProgram.join(person,joinExpression,joinType).show()


# Complex Joins

print("Complex Joins")
person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()


# Broadcast

joinType ="inner"
joinExpression = person["graduate_program"] == graduateProgram["id"]
person.join(broadcast(graduateProgram),joinExpression,joinType).show()