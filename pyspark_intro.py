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
spark = SparkSession.builder.appName('intro').getOrCreate()

# import data
df = spark.read.json('data/people.json')

# visualize data
df.show()
df.printSchema()

# columns names
df.columns

# statistical summary of data
df.describe().show()

'''
ATTENTION TO SCHEMA
'''

# import schemas functions 
from pyspark.sql.types import (StructField, StringType, 
                                IntegerType, StructType)

# define schema
data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]

final_struc = StructType(fields = data_schema)

# read data with schema 
df = spark.read.json('data/people.json', schema=final_struc)
df.printSchema()

# get column [DONT SHOW]
df['age']

# get dataframe with column
df.select('age').show()

# select multiple columns
df.select(['age', 'name']).show()

# add a new column
df = df.withColumn('newage', df['age']*2)
df.show()

# rename column name
df.withColumnRenamed('age', 'freestydf.le_sge').show()

'''
PURE SQL
'''

# register the dataframe as a SQL temporary view
df.createOrReplaceTempView('people')

# select all columns from people with SQL
results = spark.sql('SELECT * FROM people')
results.show()

# select based on SQL condition
new_results = spark.sql('SELECT * FROM people WHERE age=30')
new_results.show()





# 