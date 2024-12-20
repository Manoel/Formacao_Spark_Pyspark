from pyspark.sql import SparkSession

# De Json para o Console

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Streaming").getOrCreate()
	
	jsonschema = "nome STRING, postagem STRING, data INT"
	
	df = spark.readStream.json("/home/manoel/testestream/", schema=jsonschema)
	
	diretorio = "/home/manoel/temp/"
	
	stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()
	
	stcal.awaitTermination()