from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col

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
    # .withColumn("current_age", date.today().year() - col("date_of_birth"))
    # .withColumn("current_age", lit("22"))
    .show(10, False, True))