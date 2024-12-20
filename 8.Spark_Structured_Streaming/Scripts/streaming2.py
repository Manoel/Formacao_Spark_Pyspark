from pyspark.sql import SparkSession
import os

user = os.environ["USER_POSTG"]
pwd = os.environ["PASS_POSTG"]

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("Streaming2") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

    jsonschema = "nome STRING, postagem STRING, data INT"

    df = spark.readStream.json("/home/manoel/testestream/", schema=jsonschema)

    diretorio = "/home/manoel/temp/"

    def atualizapostgres(dataf,batchID):
        dataf.write \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/posts") \
        .option("dbtable","posts") \
        .option("user",user) \
        .option("password",pwd) \
        .option("driver","org.postgresql.Driver") \
        .mode("append") \
        .save()

    Stcal = df.writeStream \
        .foreachBatch(atualizapostgres) \
        .outputMode("append") \
        .trigger(processingTime="5 second") \
        .option("checpointlocation", diretorio) \
        .start()
    
    Stcal.awaitTermination()



