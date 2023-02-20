# general spark dataframe operations like select, printSchema(), show(), withColumn, withColumnRenamed, distinct, dropDuplicates, filter, 
# groupBy, orderBy, join

import os
from util import get_spark_session

def core():

    appName = os.environ.get('appName')
    print('The appName is ', appName)
    spark = get_spark_session(appName)
    spark.sql('select current_date()').show()

    print('************************ SAMPLE DATA **************************')
    df = spark.read.option("header", True).csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
    df.show()
    df.show(truncate=False) # to ensure column values are not truncated in the display

    print('************************ SCHEMA OF DATA BEFORE INFERRING SCHEMA **************************')
    df.printSchema()

    print('************************ SCHEMA OF DATA AFTER INFERRING SCHEMA **************************')
    df = spark.read.options(header='True', delimiter=',', inferSchema='True').csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
    df.printSchema()

    print('************************ DEFINE YOUR OWN SCHEMA *****************************')
    print('************************ HERE WE WANT TO CHANGE THE DATA TYPE OF ROLL TO STRING ********************************')
    from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
    schema = StructType(
        [
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("name", StringType(), True),
        StructField("course", StringType(), True),
        StructField("roll", StringType(), True),
        StructField("marks", IntegerType(), True),
        StructField("email", StringType(), True)
        ]
    )
    df = spark.read.options(header='True', delimiter=',').schema(schema).csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
    df.printSchema()

    print('************************ select only the required columns - Method 1 ********************************')
    df1 = df.select('name', 'age', 'gender')
    df1.show()

    print('************************ select only the required columns - Method 2 ********************************')
    df2 = df.select(df.name, df.age, df.gender)
    df2.show()

    print('************************ select only the required columns - Method 3 ********************************')
    from pyspark.sql.functions import col, lit, expr
    df3 = df.select(col('name'), col('age'), col('gender'))
    df3.show()

    print('************************ select only the required columns - Method 4 ********************************')
    print(df.columns)
    df4 = df.select(df.columns[1:5])
    df4.show()

    print('************************ withColumn to modify data type of a column ****************************')
    print('************************* Schema before casting *********************')
    df.printSchema()

    df5 = df.withColumn('age', col("age").cast("String"))
    print('************************* Schema after casting *********************')
    df5.printSchema()

    print('*********************** withColumn to modify column value in place *************************')
    df = df.withColumn('marks', col('marks')*10)
    df.show()

    print('************************* withColumn to add a new column ***************************')
    print('one new column has hardcoded value and another column is derived based on another column')
    df = df.withColumn('updated marks', col('marks')+10).withColumn('country', lit('India'))
    df.show()

    print('************************* withColumn to add a new column, complex expr ****************************')
    df = df.withColumn('grade', expr("case when marks <= 300 then 'C' when marks > 300 and marks <= 600 then 'B' else 'A' end"))
    df.show()

    print('************************* withColumnRenamed ******************************')
    df = df.withColumnRenamed('gender', 'sex').withColumnRenamed('roll', 'roll number')
    df.show()

    print('************************ column renaming using alias. df does not change, only select result changes **************************')
    df.select('name', 'age', 'sex', col('course').alias('department')).show()
    df.printSchema()

    print('************************* filter ***************************')
    df = df.filter(df.course == 'DB')
    df.show()

    print('************************* multiple filters *****************')
    df = df.filter((col('marks') > 500) & (col('sex') == 'Female'))
    df.show()

    print('************************ isin filter ************************')
    df = spark.read.options(header='True', delimiter=',').schema(schema).csv('C:\Vinod\SparkLocalDevelopment\Data\StudentData.csv')
    courses = ['DB', 'Cloud', 'OOP']
    df.filter(col('course').isin(courses)).show()
    # df.filter(df.course.isin(courses)).show() This will also work

    print('*********************** misc filters ***************************')
    print('*********************** startswith ***************************')
    df.filter(col('name').startswith('Jenna')).show()
    df.filter(df.name.startswith('Jenna')).show()

    print('*********************** endswith ****************************')
    df.filter(col('name').endswith('Towler')).show()
    df.filter(df.name.endswith('Towler')).show()

    print('*********************** contains ******************************')
    df.filter(col('name').contains('Panos')).show()
    df.filter(df.name.contains('Panos')).show()

    print('**************************** like *******************************')
    df.filter(col('name').like('%vera%')).show()
    df.filter(df.name.like('%vera%')).show()

    print('************************** count **********************************')
    print('The count is ', df.filter(col('course') == 'OOP').count())

    print('************************* distinct *****************************')
    df.select(df.gender).distinct().show()

    print('************************ distinct on all columns ********************')
    print('Count of records before distinct is: ', df.count())
    print('Count of records after distinct is: ', df.distinct().count())
    print('Number of distinct courses is ', df.select('course').distinct().count())

    print('*** instead of selecting distinct records, we can drop duplicates from df itself ********')
    df.dropDuplicates(['gender','course']).show()

    print('***************************** drop duplicates based on all columns *******************************')
    df.dropDuplicates(df.columns).show()

    print('************************ sorting *************************')
    df.sort(df.age, df.marks).show() # asc() is the default
    df.sort('age', 'marks').show()
    df.orderBy(col('age'), col('marks')).show()
    df.orderBy(df.age.asc(), df.marks.desc()).show()

    print('************************** groupBy and aggregate single column ****************************')
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender').agg(count('*').alias('Number of students')).show()

    print('************************** groupBy and aggregate single column - usual way ****************************')
    # the alias will not work i.e. u wont see 'Number of Students' on the console. You will see "count" as the column name
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender').count().alias('Number of students').show() 

    print('************************** groupBy and aggregate multiple columns ****************************')
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender').agg(count('*').alias('Number of students'), sum('marks').alias('Total marks'), min('marks').alias('Minimum marks'), max('marks').alias('Maximum marks'), avg('marks').alias('Average Marks')).show()

    print('************************** groupBy multiple columns and aggregate multiple columns ****************************')
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender','course').agg(count('*').alias('Number of students'), sum('marks').alias('Total marks'), min('marks').alias('Minimum marks'), max('marks').alias('Maximum marks'), avg('marks').alias('Average Marks')).show()

    print('************************** groupBy multiple columns and aggregate multiple columns and orderBy ****************************')
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender','course').agg(count('*').alias('Number of students'), sum('marks').alias('Total marks'), min('marks').alias('Minimum marks'), max('marks').alias('Maximum marks'), avg('marks').alias('Average Marks')).orderBy('course','gender').show()

    print('************************** groupBy multiple columns and aggregate multiple columns and orderBy aggregated column ****************************')
    from pyspark.sql.functions import min, max, sum, count, avg
    df.groupBy('gender','course').agg(count('*').alias('Number of students'), sum('marks').alias('Total marks'), min('marks').alias('Minimum marks'), max('marks').alias('Maximum marks'), avg('marks').alias('Average Marks')).orderBy('Average Marks').show()

    print('************************** group by and filtering ********************************')
    # In normail sql, where clause is used to filter rows before aggregation and having cluase is used to filter rows after aggregation
    # in pyspark, we use filter in both cases
    # below statement can be broken down into multiple steps / statements if needed, for easier understanding
    df.filter(df.gender == 'Male').groupBy(df.course, df.gender).agg(count('*').alias('Number of enrollments')).filter(col('Number of enrollments') > 85).show()
    # Note that you cannot use df.Number of enrollments in the filter clause because that is not a part of the df, but just an alias
    # If you take the  approach of splitting this step into multiple steps, then df.NUmber of enrollments will work

    print('**************************** join ***************************')
    df1 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('C:\Vinod\SparkLocalDevelopment\Data\movies.csv')
    df2 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('C:\Vinod\SparkLocalDevelopment\Data\myratings.csv')

    # In this syntax movieId column is repeated twice
    # This syntax is useful when the column on which join needs to be performed, has a different name in the dataframes
    joined_df = df1.join(df2, df1.movieId == df2.movieId, 'left')
    joined_df.show()

    # When the column on which join needs to be performed has the same name in both the dataframes, below syntax will help in removing the duplicate join column in the output dataframe
    joined_df = df1.join(df2, 'movieId', 'left')
    joined_df.show(truncate=False)

if __name__ == '__main__':
    core()