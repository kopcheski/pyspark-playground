from pyspark.sql import SparkSession

# Spark session & context
spark = (SparkSession.builder
         .appName("Read Json File")
         .master("local")
         .getOrCreate())

df = spark.read.option("multiline", "true").json("sample.json")

df.printSchema()

df.select("people.name", "people.profession").show()
