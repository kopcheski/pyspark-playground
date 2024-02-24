from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, lit

# Spark session & context
spark = (SparkSession.builder
         .appName("Read Json File")
         .master("local")
         .getOrCreate())

df = spark.read.option("multiline", "true").json("sample.json")

df.printSchema()
df.show(10, False)

(df.filter(df.profession == "Footballer")
    .sort(col("date_of_birth"))
    .withColumn("current_age", lit(datediff(current_date(), col("date_of_birth")) / 365))
    .withColumn("current_age", col("current_age").cast("int"))
    .show(10, False, True))