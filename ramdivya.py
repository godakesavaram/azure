
import math
import os
import urllib.request
import ssl
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\Kesavaram\.jdks\corretto-1.8.0_442'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("SalesTableExample").getOrCreate()

# Define schema
# schema = StructType([
#     StructField("sale_id", IntegerType(), True),
#     StructField("city", StringType(), True),
#     StructField("sale_date", StringType(), True),  # Keep as string
#     StructField("amount", IntegerType(), True)
# ])
#
# # Data with date as string
# data = [
#     (11, "Mumbai", "2024-01-10", 25000),
#     (12, "Delhi", "2024-01-15", 7000),
#     (13, "vpt", "2024-01-15", 9000),
#     (17, "Bangalore", "2024-01-15", 2000),
#     (18, "Delhi", "2024-02-15", 7000),
#     (10, "vpt", "2024-02-15", 11000),
#     (19, "Bangalore", "2024-01-20", 10000),
#     (14, "Chennai", "2024-02-05", 3000),
#     (15, "Mumbai", "2024-02-08", 9000),
#     (111, "Mumbai", "2024-03-10", 5000),
#     (121, "Delhi", "2024-03-15", 17000),
#     (131, "vpt", "2024-03-15", 19000),
# ]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema=schema)
#
#
# df.show()
# win=Window.orderBy(desc("amount"))
# df=df.withColumn('rnk',dense_rank().over(win)).filter("rnk=2").drop('rnk')
# df.show()
#
# data = [
#     (1, "Neha", 50000, "HR"),
#     (2, "Ravi", 70000, "IT"),
#     (13, "Aman", 50000, "HR"),
#     (14, "Pooja", 90000, "IT"),
#     (15, "Karan", 70000, "IT")
# ]
#
# # Schema
# columns = ["emp_id", "emp_name", "salary", "department"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# dff=df.groupBy('salary','department').agg(count('*').alias('count'))
# dff=dff.filter("count>1").drop('count')
# fin=df.join(dff,(df['salary']==dff['salary'])&(df['department']==dff['department']),'inner')\
# .drop(dff['salary'],dff['department'])
# fin.show()

# df.createOrReplaceTempView("df")
#
# # Now use SQL to query
# dff = spark.sql("""
# SELECT *
# FROM df
# WHERE (department, salary) IN (
#   SELECT department, salary
#   FROM df
#   GROUP BY department, salary
#   HAVING COUNT(*) > 1
# )
# """)
# dff.show()
# data = [
#     (1, "Sameer", "sameer@gmail.com"),
#     (2, "Anjali", "anjali@gmail.com"),
#     (13, "Sameer", "sameer@gmail.com"),
#     (14, "Rohan", "rohan@gmail.com"),
#     (15, "Rohan", "rohan@gmail.com")
# ]
#
# columns = ["user_id", "user_name", "email"]
#
# df = spark.createDataFrame(data, columns)
# win=Window.partitionBy('email').orderBy('user_id')
# df=df.withColumn('rnk',row_number().over(win)).filter("rnk>1").drop('rnk')
#
# df.show()
# schema = [
#     "order_id",
#     "customer_id",
#    "order_date",
#     "amount",
# ]
#
# # Create data
# data = [
#     (1, 101, "2024-01-10", 1000),
#     (2, 102, "2024-02-15", 2000),
#     (3, 101, "2024-03-20", 1500),
#     (4, 103, "2024-04-05", 2500),
#     (5, 101, "2024-05-08", 3000)
# ]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema)
# df=df.withColumn('order_date',to_date("order_date"))
# max_date = df.select(max("order_date")).collect()[0][0]
#
# cutoff_date = add_months(lit(max_date), -6)
#
# df_filtered = df.filter(col("order_date") >= cutoff_date)
# win=Window.partitionBy('customer_id').orderBy('order_id')
# dfff=df_filtered.withColumn("rnk",dense_rank().over(win)).filter("rnk>=3").drop('rnk')
#
# dfff.show()

# data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
#         ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
#         ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
#         ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
#         ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
# myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
# df = spark.createDataFrame(data,schema=myschema)
# df.show()
# dff=df.groupBy('salary').agg(count('*').alias('count'))
# dff=dff.filter("count>1")
# df.join(dff,'salary','inner').drop('count').show()
# df.createOrReplaceTempView('df')
#
# sqldf=spark.sql('''with s as (select salary ss,count(*) c from df group by salary)
#                 select * from df
#                 join
#                 s on(df.salary=s.ss)
#                 where c>1'''
#                 )
#
# sqldf.drop('ss','c').show()

# data = [
#     (1, "1-Jan", "Ordered"),
#     (1, "2-Jan", "dispatched"),
#     (1, "3-Jan", "dispatched"),
#     (1, "4-Jan", "Shipped"),
#     (1, "5-Jan", "Shipped"),
#     (1, "6-Jan", "Delivered"),
#     (2, "1-Jan", "Ordered"),
#     (2, "2-Jan", "dispatched"),
#     (2, "3-Jan", "shipped")]
# myschema = ["orderid","statusdate","status"]
# df = spark.createDataFrame(data,schema=myschema)
# df.show()
# df.filter("status ='dispatched'").show()
#
# data = [(1111, "2021-01-15", 10),
#         (1111, "2021-01-16", 15),
#         (1111, "2021-01-17", 30),
#         (1112, "2021-01-15", 10),
#         (1112, "2021-01-15", 20),
#         (1112, "2021-01-15", 30)]
#
# myschema = ["sensorid", "timestamp", "values"]
#
# df = spark.createDataFrame(data, schema=myschema)
# df.show()
# df.createOrReplaceTempView('df')
# dff=spark.sql("""
# with a as (select f.*, lead(values) over(partition by sensorid order by values) k from df f)
# select a.*,abs(values-k) from a
#
# """)
# dff.dropna().show()
# led=Window.partitionBy("sensorid").orderBy("timestamp")
# df=df.withColumn('new',lead('values').over(led))
# df.withColumn('v',expr("abs(values-new)")).dropna().drop('values','new').show()

# data = [(1, "Mark Ray", "AB"),
#         (2, "Peter Smith", "CD"),
#         (1, "Mark Ray", "EF"),
#         (2, "Peter Smith", "GH"),
#         (2, "Peter Smith", "CD"),
#         (3, "Kate", "IJ")]
# myschema = ["custid", "custname", "address"]
# df = spark.createDataFrame(data, schema=myschema)
# df.show()
# # df.groupBy('custid','custname').agg(collect_list('address')).show()
# df.createOrReplaceTempView('df')
# spark.sql("""
# select custid,custname,collect_list(address) from df
# group by custid,custname
#
# """).show()

# data = [
#     ("India",),
#     ("Pakistan",),
#     ("SriLanka",)
# ]
# myschema = ["teams"]
# df = spark.createDataFrame(data, schema=myschema)
# df.show()
# df1=df
# df1=df.withColumnRenamed('teams','team')
# # df1.show()
# dfff=df.join(df1,df['teams']<df1['team'], 'inner')
# # dff.filter("teams<team").show()
# dfff.selectExpr("concat_ws(' vs ',teams,team)").show()
# data = [
#     ("a", [1, 1, 1, 3]),
#     ("b", [1, 2, 3, 4]),
#     ("c", [1, 1, 1, 1, 4]),
#     ("d", [3])
# ]
# df = spark.createDataFrame(data, ["name", "rank"])
# df.show()
# dff=df.select("name",explode("rank").alias('rnk')).filter("rnk=1")
# dff=dff.groupBy('name').agg(count("*").alias('count')).orderBy(desc('count')).limit(1)
# dff.show()
# data = [
#     (1, 300, "31-Jan-2021"),
#     (1, 400, "28-Feb-2021"),
#     (1, 200, "31-Mar-2021"),
#     (2, 1000, "31-Oct-2021"),
#     (2, 900, "31-Dec-2021")
# ]
# df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
#
# df = df.withColumn(
#     "monthlastdate",
#     to_date("monthlastdate", "dd-MMM-yyyy")  # specify format
# )
# dff=df.groupBy('empid').agg(max("monthlastdate").alias(('date')))
# dff.withColumn("date",date_format("date", "dd-MMM-yyyy")).show()


# l=[5, 9, 9, 1]
# k=0
# s=0
# for i in l:
#     s=s+1
#     k=k+i
#     if k>19:
#
#         k=k-i
#         s=s-1
#     if k==19:
#         print(s)
# else:
#     print(-1)
# print(s)
# l=['hi how are you doing','hello how are you doing']
# # l=l.split(' ')
#
# rdd=sc.parallelize(l)
# rdd1=rdd.flatMap(lambda x : x.split(' '))
# print(rdd.collect())
# grouped_rdd = rdd1.groupBy(lambda x: x)
# counts_rdd = grouped_rdd.mapValues(lambda x: len(list(x)))
# print(counts_rdd.collect())
df=spark.read.format('csv').option("sep",'|').option('header','false').load('prod.csv')
df.show()
df=df.filter("_c1  not like '%HDR%' or _c1  not like '%TLR%'")

# Get the first row (Row object)
header = df.first()
print(header)
# Convert row into a list of column names
columns = [str(c) for c in header]
for i in header:
    print(i)
print(columns)

# Filter out the header row from the DataFrame
data = df.filter(df["_c0"] != header[0])

# Apply new header
final_df = data.toDF(*columns)

final_df.show()




