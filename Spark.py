from pyspark.sql import SparkSession
import random

# Spark session & context
spark = (SparkSession.builder
         .appName("Simple calculation")
         .master("local")
         .getOrCreate())
sc = spark.sparkContext

num_samples = 10000000

def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples
print(pi)