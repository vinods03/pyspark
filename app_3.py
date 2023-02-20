# Spark SQL

import os
from util import get_spark_session

appName = os.environ.get('appName')
spark = get_spark_session(appName)
spark.sql('select current_date()').show()

df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
df.createOrReplaceTempView('student_data')

df = spark.sql('select age, gender, count(*), min(marks), max(marks) from student_data group by age, gender')
df.show()

# with column alias. note that spaces are not allowed in column alias i.e. instead of num_students, you cannot use Number of students
df = spark.sql('select age, gender, count(*) as num_students, min(marks) as minimum_marks, max(marks) as maximum_marks from student_data group by age, gender')
df.show()