# import libraries
import findspark
findspark.init('/Users/pratikajitb/server/spark-2.4.0-bin-hadoop2.7')

from pyspark.sql import SparkSession

# creating the spark session
spark = SparkSession.builder.appName('walmart').getOrCreate()