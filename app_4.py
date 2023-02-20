# dataframe write

import os
from util import get_spark_session
from pyspark.sql.functions import count, min, max, avg, col, lit

appName = os.environ.get('appName')
spark = get_spark_session(appName)
spark.sql('select current_date()').show()

df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
df.show()

# other modes available are append, ignore, error
# note that a folder is created with multiple files. this is because of the distributed processing done by spark.
df = df.groupBy('gender','age').agg(count('*').alias('Number of students'), min('marks').alias('Minimum marks'), max('marks').alias('Maximum marks'))
df.write.mode("overwrite").options(header='True', delimiter='\t').csv('C:\Vinod\SparkLocalDevelopment\Data\output')

# read the output data for validation
# it is enough if you read from the folder. spark will read all the files under the folder
output_df = spark.read.options(header='True', inferSchema='True', delimiter='\t').csv('C:\Vinod\SparkLocalDevelopment\Data\output')
output_df.show()