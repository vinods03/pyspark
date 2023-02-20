# caching

import os
from util import get_spark_session
from pyspark.sql.functions import count, min, max, avg, col, lit

appName = os.environ.get('appName')
spark = get_spark_session(appName)

spark.sql('select current_date()').show()

df = spark.read.options(header = 'True', inferSchema = 'True', delimiter = ',').csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
# df.show()

print('***************************** Transformation 1 *****************************************')
df = df.groupBy('age', 'gender').agg(count('*').alias('Number of students'), min('marks').alias('minimum marks'), max('marks').alias('maximum marks'))

print('***************************** Transformation 2 *****************************************')
df = df.select('age', 'gender', 'Number of students', 'minimum marks', 'maximum marks')

print('***************************** Action *****************************************')
df.show()

print('***************************** Results of action stored in memory *****************************************')
df.cache()

print('for this action, memory cache will be used instead of starting from reading the data from csv file again')
df = df.filter(col('Number of students') >= 250).show()