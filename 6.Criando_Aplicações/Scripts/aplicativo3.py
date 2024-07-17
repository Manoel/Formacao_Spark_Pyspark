import sys, getopt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#  O módulo getopt é um analisador sintático para opções de linha de comando cuja API é projetada para ser familiar aos usuários da função C getopt()

# Lê o arquivo despachantes.csv como datafram, calcula o numero de vendas por ano e exibe no console.

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("Aplicativo_exemplo") \
    .master("local[*]") \
    .getOrCreate()

    opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")
    # iniciando as variaveis
    formato, infile, outdir = "","",""

    # Percorrer as opções e verificar se as opçoes são do tipo t,i ou o

    for opt, arg in opts:
        if opt == "-t":
            formato = arg
        elif opt == "-i":
            infile = arg
        elif opt == "-o":
            outdir = arg

    # Criando um dataframe para lert o arquivo atraves da passagem de parâmetro.
    dados = spark.read.csv(infile, header=False, inferSchema=True)
    # Salvando o arquivo no formato a ser passado.
    dados.write.format(formato).save(outdir)

    spark.stop()