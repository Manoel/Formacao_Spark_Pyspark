## 10. Outros

* Instalando o jupyter e findspark

```pyspark
pip install jupyter
pip install findspark

```

* Usando Spark com Notebooks do Jupyter

* Importando bibliotecas
```pyspark

import findspark
findspark.init()
import pyspark

```
* Importando bibliotecas

```ipynb
mport findsparkImportando bibliotecas
findspark.init()
import pyspark
import pandas as pd

```

* Lendo e mostrando o arquivo de churn

```pyspark

churn = pd.read_csv("/home/manoel/download/Churn.csv", sep=";")
churn.head(5)
```

* Obtendo o tipo do arquivo

```ipynb

type(churn)
pandas.core.frame.DataFrame

```


* Convertendo Pandas para DataFrame do Spark

```ipynb

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Pandas").getOrCreate()

churn_df = spark.createDataFrame(churn)

type(churn_df)
pyspark.sql.dataframe.DataFrame

churn_df.show(5)

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

* Convertendo Dataframe do Spark em Pandas

```ipynb
pandas = churn_df.toPandas()

type(pandas)
pandas.core.frame.DataFrame

pandas.head(5)

CreditScore	Geography	Gender	Age	Tenure	Balance	NumOfProducts	HasCrCard	IsActiveMember	EstimatedSalary	Exited
0	619	France	Female	42	2	0	1	1	1	10134888	1
1	608	Spain	Female	41	1	8380786	1	0	1	11254258	0
2	502	France	Female	42	8	1596608	3	1	0	11393157	1
3	699	France	Female	39	1	0	2	0	0	9382663	0
4	850	Spain	Female	43	2	12551082	1	1	1	790841	0



```

* Usando bibliotecas Koalas (Permite migrar de Pandas para Spark)

* 

```ipynb

import databricks.koalas as ks

```

* Spark UI

* localhost:4040

```pyspark

from pyspark.sql.types import *

>>> from pyspark import StorageLevel

>>> despachantes = spark.read.csv("/home/manoel/download/despachantes.csv", header=False, inferSchema=True)

>>> despachantes.show(5)

+---+-------------------+-----+-------------+---+----------+

|_c0|                _c1|  _c2|          _c3|_c4|       _c5|

+---+-------------------+-----+-------------+---+----------+

|  1|   Carminda Pestana|Ativo|  Santa Maria| 23|2020-08-11|

|  2|    Deolinda Vilela|Ativo|Novo Hamburgo| 34|2020-03-05|

|  3|   Emídio Dornelles|Ativo| Porto Alegre| 34|2020-02-05|

|  4|Felisbela Dornelles|Ativo| Porto Alegre| 36|2020-02-05|

|  5|     Graça Ornellas|Ativo| Porto Alegre| 12|2020-02-05|

+---+-------------------+-----+-------------+---+----------+

only showing top 5 rows



>>> despachantes.cache()

DataFrame[_c0: int, _c1: string, _c2: string, _c3: string, _c4: int, _c5: date]

>>> despachantes.persist(StorageLevel.MEMORY_AND_DISK).count()

25/01/02 19:12:13 WARN CacheManager: Asked to cache already cached data.

10



```