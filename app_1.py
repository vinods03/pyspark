# spark udfs

import os
from util import get_spark_session
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField

appName = os.environ.get('appName')
spark = get_spark_session(appName)
spark.sql('select current_date()').show()

df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('C:\Vinod\SparkLocalDevelopment\Data\OfficeData.csv')
df.printSchema()
df.show()

# define a spark function to calc total salary

def get_total_salary(sal, bonus):
    return sal + bonus

def get_increment(state, sal, bonus):
    if state == 'NY':
        incr = ((0.1 * sal) + (0.5 * bonus))
    elif state == 'CA':
        incr = ((0.12 * sal) + (0.3 * bonus))
    else:
        incr = ((0.05 * sal) + (0.1 * bonus))
    return incr

get_incr_udf = udf(lambda x, y, z: get_increment(x, y, z), DoubleType())
# DoubleType() indicates the return type

print('************************* Use a spark function to get total salary ****************************')
# worked even without udf
# but always a good practice to use udf
df = df.withColumn('Total Salary', get_total_salary(df.salary, df.bonus))
df.printSchema()
df.show()

print('************************* Use a spark funcction to get increment ******************************')
# without udf, i was getting below error. 
# ValueError: Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
# note that we are passing entire column and not single value
df = df.withColumn('Increment', get_incr_udf(df.state, df.salary, df.bonus))
df.printSchema()
df.show()



