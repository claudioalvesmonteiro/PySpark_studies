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
spark = SparkSession.builder.appName('basics').getOrCreate()

# import data
df = spark.read.csv('data/appl_stock.csv', 
                    inferSchema=True, 
                    header=True)

df.printSchema()

df.show(2)

# filter data with SQL style
df.filter('Close < 500').show()

df.filter('Close < 500').select(['Open', 'Close']).show()

# filter PySpark style
df.filter(df['Close'] < 500).select('Volume').show()

# filter data with multiple conditions SQL style
df.filter('Close < 500 AND Open > 200').select(['Open', 'Close']).show()

# filter data with multiple conditions PySpark style
df.filter((df['Close'] < 500) & (df['Open'] > 200) ).select(['Open', 'Close']).show()

# filter not operator SQL style
df.filter('Close < 500 AND NOT Open > 200').select(['Open', 'Close']).show()

# filter not operator PySpark style
df.filter((df['Close'] < 500) & ~(df['Open'] > 200) ).select(['Open', 'Close']).show()

'''
COLLECT TO FORCE EXECUTION AND RETURN 
VALUES TO BE WORKING WITH
'''

df.filter(df['Low'] == 197.16).show()

result = df.filter(df['Low'] == 197.16).collect()
result

row = result[0]
row.asDict()['Volume']

'''
GROUPBY AND AGGREGATE FUNCTIONS
'''

# import data
df = spark.read.csv('data/sales_info.csv', 
                    inferSchema=True,
                    header=True)

df.show()

# groupby mean
df.groupby('Company').mean().show()

# groupby sum
df.groupby('Company').sum().show()

# groupby max
df.groupby('Company').max().show()

# groupby min
df.groupby('Company').min().show()

# groupby count
df.groupby('Company').count().show()

# generalized aggregate sum
df.agg({'Sales':'sum'}).show()

# generalized aggregate max
df.agg({'Sales':'max'}).show()

# combine grouṕby and for multiple aggregations
dfg = df.groupby('Company')
dfg.agg({'Person':'count','Sales':'max'}).show()

# combine grouṕby and for multiple aggregations
dfg = df.groupby('Company')

(
dfg.agg({'Sales':'max', 'Person':'count'})
    .toDF('company', 'sales_max','person_count')
    .show()
)

'''
IMPORT FUNCTIONS FROM SPARK
'''

from pyspark.sql.functions import countDistinct, avg, stddev

# count the number of different values in sales
df.select(countDistinct('Sales')).show()






