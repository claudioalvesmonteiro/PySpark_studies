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
spark = SparkSession.builder.appName('basics2').getOrCreate()

'''
DEALING WITH MISSING DATA
'''

df = spark.read.csv('data/ContainsNull.csv', 
                    header=True,
                    inferSchema=True)

df.show()

# drop na rows
df.na.drop().show()

# drop na rows [the row must have at least 2 nulls to be droped]
df.na.drop(thresh=2).show()

# drop na rows that have all values missing
df.na.drop(how='all').show()

# drop na rows based on one column
df.na.drop(subset=['Sales']).show()

# fill na values to any string column
df.na.fill('FILL VALUE').show()

# fill na values to any number column
df.na.fill(0).show()

# fill na values to a specific column
df.na.fill('No Name', subset=['Name']).show()

## fill with AVG
from pyspark.sql.functions import mean 

mean = df.select(mean(df['Sales'])).collect()

mean_sales = mean[0][0]

df.na.fill(mean_sales, subset = ['Sales']).show()

