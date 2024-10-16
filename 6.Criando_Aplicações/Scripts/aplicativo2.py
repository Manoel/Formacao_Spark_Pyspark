import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Lê o arquivo despachantes.csv como datafram, passa como argumento o caminho do arquivo e calcula o numero de vendas por ano e exibe no console.

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("Aplicativo_exemplo") \
    .master("local[*]") \
    .getOrCreate()

    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv(sys.argv[1], header=False, schema=arqschema)
    calculo = despachantes.select("data").groupBy(year("data")).count()
    calculo.write.format("console").save()
    spark.stop()