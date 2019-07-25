'''
PySpark for Big Data and Machine Learning
UDEMY
'''

# paths to spark and python3
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--executor-memory 1G pyspark-shell'
os.environ["SPARK_HOME"] = "/home/pacha/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())

# inititate session
spark = SparkSession.builder.appName('dates').getOrCreate()

# import data
df = spark.read.csv('data/appl_stock.csv', 
                    header=True, 
                    inferSchema=True)