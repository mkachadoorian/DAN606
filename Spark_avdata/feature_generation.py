#imports
#nano ~/.bashrc
#export PYSPARK_PYTHON=python3
#export JAVA_HOME=/usr/java/latest

#control X then Y enter
#source ~/.bashrc
#echo $PYSPARK_PYTHON
#pyspark --> 3.6.8


from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

#set up SparkSession

spark = SparkSession.builder \
.master("local[8]")\
.appName("avdata")\
.getOrCreate()

df = spark.read.format('csv').options(header="True", inferSchema="True").load("avdata/*")
df.show()
df.printSchema()

#get a count of rows
df.count()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()

#######################################################################
# handle dates and times
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df.show(2)

# now we should be able to convert or extract date features from timestamp
df.withColumn('dayofmonth', fun.dayofmonth("timestamp")).show(2)
df.withColumn('month', fun.month("timestamp")).show(2)
df.withColumn('year', fun.year("timestamp")).show(2)
df.withColumn('dayofyear', fun.dayofyear("timestamp")).show(2) 

# calculate the difference from the current date ('days_ago')
df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show()



########################################################################
#group_by
# summarize within group data
df.groupBy("sourcefile").count().show(99)
df.groupBy("sourcefile").min('open').show(99)
df.groupBy("sourcefile").mean('open').show(99)
df.groupBy("sourcefile").max('open','close').show(99)



########################################################################
#window functions
from pyspark.sql.window import Window
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

windowSpec  = Window.partitionBy("sourcefile").orderBy("days_ago")

#see also lead
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag.select('sourcefile', 'lag', 'open').show(99)

dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open')).show() 
