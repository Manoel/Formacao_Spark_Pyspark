>>> from pyspark.sql import functions as Func
>>> from pyspark.sql.functions import *
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

>>> spark.sql("select nome, vendas from despachantes").show()
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|     Graça Ornellas|    12|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
|   Viviana Sequeira|     0|
+-------------------+------+

>>> despachantes.select("nome", "vendas").show()
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|     Graça Ornellas|    12|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
|   Viviana Sequeira|     0|
+-------------------+------+

>>> spark.sql("select nome, vendas from despachantes where vendas > 20").show()
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
+-------------------+------+

>>> despachantes.select("nome", "vendas").where(Func.col("vendas") > 20).show()
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
+-------------------+------+

>>> spark.sql("select cidade, sum(vendas) from despachantes group by cidade order by 2 desc").show()
+-------------+-----------+
|       cidade|sum(vendas)|
+-------------+-----------+
| Porto Alegre|        223|
|  Santa Maria|         68|
|Novo Hamburgo|         34|
+-------------+-----------+

>>> despachantes.groupby("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()
+-------------+-----------+
|       cidade|sum(vendas)|
+-------------+-----------+
| Porto Alegre|        223|
|  Santa Maria|         68|
|Novo Hamburgo|         34|
+-------------+-----------+
