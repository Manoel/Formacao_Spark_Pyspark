>>> from pyspark.sql import SparkSession
>>> from pyspark.sql.types import *
>>> spark.sql("show database").show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/session.py", line 1034, in sql
    return DataFrame(self._jsparkSession.sql(sqlQuery), self)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/spark/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 196, in deco
    raise converted from None
pyspark.sql.utils.ParseException: 
Syntax error at or near end of input: missing 'FUNCTIONS'(line 1, pos 13)

== SQL ==
show database
-------------^^^

>>> spark.sql("show databases").show()
+---------+
|namespace|
+---------+
|  default|
+---------+

>>> spark.sql("create database desp")
23/02/23 15:47:53 WARN ObjectStore: Failed to get database desp, returning NoSuchObjectException
23/02/23 15:47:53 WARN ObjectStore: Failed to get database desp, returning NoSuchObjectException
23/02/23 15:47:53 WARN ObjectStore: Failed to get database desp, returning NoSuchObjectException
DataFrame[]
>>> spark.sql("show databases").show()
+---------+
|namespace|
+---------+
|  default|
|     desp|
+---------+

>>> spark.sql("use desp")
DataFrame[]
>>> arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
>>> despachantes = spark.read.csv("/home/manoel/download/despachantes.csv", header=False, schema = arqschema)
>>> despachantes.show()
+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

>>> despachantes.write.saveAsTable("despachantes")
23/02/23 15:55:29 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
23/02/23 15:55:29 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
23/02/23 15:55:29 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
23/02/23 15:55:29 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
>>> spark.sql("select * from despachantes").show()
+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

>>> spark.sql("select * from despachantes").show()
+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

>>> spark.sql("show tables").show()
+---------+------------+-----------+
|namespace|   tableName|isTemporary|
+---------+------------+-----------+
|     desp|despachantes|      false|
+---------+------------+-----------+

>>> despachantes.write.mode("overwrite").saveAsTable("despachantes")
>>> 

~ via ☕ v11.0.17 took 3h 51m 26,5s …
➜ pyspark
Python 3.11.0 (main, Dec 29 2022, 20:45:55) [GCC 9.4.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
23/02/23 19:32:54 WARN Utils: Your hostname, manoel-VirtualBox resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface enp0s3)
23/02/23 19:32:54 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/02/23 19:32:55 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
23/02/23 19:32:56 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
23/02/23 19:32:56 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
23/02/23 19:32:56 WARN Utils: Service 'SparkUI' could not bind on port 4042. Attempting port 4043.
23/02/23 19:32:56 WARN Utils: Service 'SparkUI' could not bind on port 4043. Attempting port 4044.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.1
      /_/

Using Python version 3.11.0 (main, Dec 29 2022 20:45:55)
Spark context Web UI available at http://10.0.2.15:4044
Spark context available as 'sc' (master = local[*], app id = local-1677191576114).
SparkSession available as 'spark'.
>>> despachantes.show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'despachantes' is not defined
>>> spark.sql("use desp").show()
23/02/23 19:33:37 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
23/02/23 19:33:37 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
23/02/23 19:33:40 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
23/02/23 19:33:40 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore manoel@127.0.1.1
23/02/23 19:33:40 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
++
||
++
++

>>> spark.sql("select * from despachantes").show()
+---+-------------------+------+-------------+------+----------+                
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+