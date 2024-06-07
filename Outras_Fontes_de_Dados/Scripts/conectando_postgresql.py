from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys

user = os.environ["USER_POSTG"]
pwd = os.environ["PASS_POSTG"]


spark = SparkSession \
    .builder \
    .appName("formacao_spark_pyspark") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

sqlContext = SparkSession(spark)
spark.sparkContext.setLogLevel("ERROR")

resumo = spark.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable","vendas") \
    .option("user",user) \
    .option("password",pwd) \
    .option("driver","org.postgresql.Driver") \
    .load().show()

print("Tipo: ", type(resumo))
print("sqlContext: ", type(sqlContext))

sql = "select data, total from vendas"

etl = sqlContext

dfs = etl.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/vendas") \
    .option("query",sql) \
    .option("user",user) \
    .option("password",pwd) \
    .option("driver","org.postgresql.Driver") \
    .load().show()