import sys, os, getopt
from pyspark.sql import SparkSession


user = os.environ["USER_POSTG"]
pwd = os.environ["PASS_POSTG"]

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("Aplicativo_tabela")\
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

    opts, args = getopt.getopt(sys.argv[1:], "a:t:")
    # iniciando as variaveis
    arquivo, tabela = "",""

    for opt, arg in opts:
        if opt == "-a":
            arquivo = arg
        elif opt == "-t":
            tabela = arg

    df = spark.read.load(arquivo)

    df.write.format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/db_tabelas") \
    .option("dbtable","tabelas") \
    .option("user",user) \
    .option("password",pwd) \
    .option("driver","org.postgresql.Driver")\
    .save()

    spark.stop()