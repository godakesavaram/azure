import os
import urllib.request
import ssl
from itertools import dropwhile

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

ssl_context = ssl._create_unverified_context()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\Kesavaram\.jdks\corretto-1.8.0_442'
######################🔴🔴🔴################################

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)

######################🔴🔴🔴################################
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# FULL URL CODE

# data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
# myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
# df = spark.createDataFrame(data,schema=myschema)
# df.show()

# data = [
#     ("P1", "Apple"),
#     ("P1", "Banana"),
#     ("P1", "Mango"),
#     ("P3", "Banana"),
#     ("P3", "Apple"),
#     ("P2", "Apple"),
#     ("P2", "Mango")
# ]
#
# # Define Schema
# columns = ["persons", "fruit"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Show DataFrame
# df.show()
# df.createOrReplaceTempView('df')
#
# fin=sp

# data = [("Alice", "HR"), ("Bob", "IT"), ("Charlie", "IT"), ("Alice", "HR"), ("David", "Finance")]
# columns = ["name", "department"]
# df = spark.createDataFrame(data, columns)
#
# # Aggregate: Count total rows and distinct departments
# r=df.agg(
#     count("*").alias("total_rows")
# )
# r.show()


data=[(1,2),(3,2),(6,8),(9,8),(2,5),(8,5),(5,None)]
schema=['n','p']
df=spark.createDataFrame(data,schema)

df1 = df.alias("df1")
df2 = df.select(col("p").alias("p")).alias("df2")

# Use Column expression in the join condition
ddd = df1.join(df2, col("df1.n") == col("df2.p"), how="left_anti")
ddd=ddd.withColumn('status',lit('leaf'))
ddr=df.filter("p is null").withColumn('status',lit('root'))
dff=ddd.union(ddr)

result = df.join(dff, df["n"] == dff["n"], how="left_anti")

result=result.withColumn('status',lit('inner'))
res=dff.union(result)
df.show()
res.orderBy('n').select('n','status').show()

