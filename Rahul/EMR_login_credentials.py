from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import sys

data=sys.argv[1]

df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",",").load(data)


output=sys.argv[2]
df.write.format("csv").option("header","true").save(output)

# output=sys.argv[3]
# df.write.format("csv").option("header","true").save(output)
