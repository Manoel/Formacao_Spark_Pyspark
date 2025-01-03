* Particionamento e Bucketing no Spark

* Visualizando os databases

```pyspark

spark.sql("show databases").show()

```

* Usando o database desp

```pyspark

spark.sql("use desp").show()

```

* Carregando os dados

```pyspark

churn = spark.read.csv("/home/manoel/download/Churn.csv", inferSchema=True, header=True, sep=";")
churn.show(5)
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
|CreditScore|Geography|Gender|Age|Tenure| Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
|        619|   France|Female| 42|     2|       0|            1|        1|             1|       10134888|     1|
|        608|    Spain|Female| 41|     1| 8380786|            1|        0|             1|       11254258|     0|
|        502|   France|Female| 42|     8| 1596608|            3|        1|             0|       11393157|     1|
|        699|   France|Female| 39|     1|       0|            2|        0|             0|        9382663|     0|
|        850|    Spain|Female| 43|     2|12551082|            1|        1|             1|         790841|     0|
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
only showing top 5 rows


```

* Criar o particionamento

```pyspark

churn.write.partitionBy("Geography").saveAsTable("Churn_Geo")

```

* Consultando a tabela criada

```pyspark

spark.sql("select * from Churn_Geo").show(5)
+-----------+------+---+------+--------+-------------+---------+--------------+---------------+------+---------+
|CreditScore|Gender|Age|Tenure| Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|Geography|
+-----------+------+---+------+--------+-------------+---------+--------------+---------------+------+---------+
|        619|Female| 42|     2|       0|            1|        1|             1|       10134888|     1|   France|
|        502|Female| 42|     8| 1596608|            3|        1|             0|       11393157|     1|   France|
|        699|Female| 39|     1|       0|            2|        0|             0|        9382663|     0|   France|
|        822|  Male| 50|     7|       0|            2|        1|             1|         100628|     0|   France|
|        501|  Male| 44|     4|14205107|            2|        0|             1|         749405|     0|   France|
+-----------+------+---+------+--------+-------------+---------+--------------+---------------+------+---------+
only showing top 5 rows

```

* Observando o particionamento criado

```pyspark

➜ cd spark-warehouse       

~/spark-warehouse …
➜ ls
desp.db  vendasvarejo.db

~/spark-warehouse …
➜ cd desp.db        

~/spark-warehouse/desp.db …
➜ ls
churn_geo  despachantes  reclamacoes

~/spark-warehouse/desp.db …
➜ cd churn_geo                                                                                                                                                    

spark-warehouse/desp.db/churn_geo …
➜ ls
'Geography=France'  'Geography=Germany'  'Geography=Spain'   _SUCCESS

spark-warehouse/desp.db/churn_geo …
➜ cd 'Geography=France'

desp.db/churn_geo/Geography=France …
➜ ls
part-00000-2274e896-117e-4fc0-a6a7-755ece69b38c.c000.snappy.parquet



```

* Bucketing, definindo o numero de bucketing e coluna.

```pyspark
churn.write.bucketBy(3,"Geography").saveAsTable("Churn_Geo2")


```

* Consultando a tabela em disco.

```pyspark

spark.sql("select * from Churn_Geo2").show(5)
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
|CreditScore|Geography|Gender|Age|Tenure| Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
|        619|   France|Female| 42|     2|       0|            1|        1|             1|       10134888|     1|
|        608|    Spain|Female| 41|     1| 8380786|            1|        0|             1|       11254258|     0|
|        502|   France|Female| 42|     8| 1596608|            3|        1|             0|       11393157|     1|
|        699|   France|Female| 39|     1|       0|            2|        0|             0|        9382663|     0|
|        850|    Spain|Female| 43|     2|12551082|            1|        1|             1|         790841|     0|
+-----------+---------+------+---+------+--------+-------------+---------+--------------+---------------+------+
only showing top 5 rows

```

* Observando o particionamento, verificamos que não temos mais em diretorios, mas sim em arquivos parquets diferentes.

```pyspark

~/spark-warehouse/desp.db …
➜ cd churn_geo2

spark-warehouse/desp.db/churn_geo2 …
➜ ls
part-00000-550194ae-ccef-452e-a1bd-3d9586f132f8_00001.c000.snappy.parquet  part-00000-550194ae-ccef-452e-a1bd-3d9586f132f8_00002.c000.snappy.parquet  _SUCCESS

```

### Cache (Padrão em memórioa) e Persist (Definido pelo usuário).

* Exemplos de colocar objeto em cache.

* Importando bibliotecas

```pyspark

from pyspark import StorageLevel

```

* Usando a tabela desp

```pyspark

spark.sql("use desp")

```

* Visualizando as tabelas existentes.

```pyspark

spark.sql("show tables").show()
+---------+---------------+-----------+
|namespace|      tableName|isTemporary|
+---------+---------------+-----------+
|     desp|      churn_geo|      false|
|     desp|     churn_geo2|      false|
|     desp|   despachantes|      false|
|     desp|despachantes_ng|      false|
|     desp|    reclamacoes|      false|
+---------+---------------+-----------+

```

* Consultando os dados da tabela despachantes via dataframe.

```pyspark

df = spark.sql("select * from despachantes")
df.show(2)
+---+----------------+------+-------------+------+----------+
| id|            nome|status|       cidade|vendas|      data|
+---+----------------+------+-------------+------+----------+
|  1|Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2| Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
+---+----------------+------+-------------+------+----------+
only showing top 2 rows

```

* Verificando o storage level.

```pyspark

df.storageLevel
StorageLevel(False, False, False, False, 1)

# DISCO, MEMÓRIA, OFFHEAP, SERIALIZADO, REPLICAÇÃO
```

* Criando a cache.

```pyspark
df.cache()
DataFrame[id: int, nome: string, status: string, cidade: string, vendas: int, data: string]
>>> df.storageLevel
StorageLevel(True, True, False, True, 1)


```

* Tirando o tipo de cache para mudar para apenas em disco..

```pyspark

df.unpersist()
DataFrame[id: int, nome: string, status: string, cidade: string, vendas: int, data: string]

df.persist(StorageLevel.DISK_ONLY)
DataFrame[id: int, nome: string, status: string, cidade: string, vendas: int, data: string]

df.storageLevel
StorageLevel(True, False, False, False, 1)

```