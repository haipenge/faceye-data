from pyspark.sql import SparkSession
logFile = "/home/prnp/hadoop/spark-2.2.0-bin-hadoop2.7/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("run spark with python").master("spark://hd01.aspire.com.cn:7077").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
#Run Command is:
#/bin/spark-submit --master local[4] spark_1.py