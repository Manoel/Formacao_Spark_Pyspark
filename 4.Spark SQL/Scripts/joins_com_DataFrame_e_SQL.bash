>>> recschema = "idrec INT, datarec STRING, iddesp INT"
>>> reclamacoes = spark.read.csv("/home/manoel/download/reclamacoes.csv", header=False, schema=recschema)
>>> reclamacoes.show()
+-----+----------+------+
|idrec|   datarec|iddesp|
+-----+----------+------+
|    1|2020-09-12|     2|
|    2|2020-09-11|     2|
|    3|2020-10-05|     4|
|    4|2020-10-02|     5|
|    5|2020-12-06|     5|
|    6|2020-01-09|     5|
|    7|2020-01-05|     9|
+-----+----------+------+

>>> reclamacoes.write.saveAsTable("reclamacoes")
>>> spark.sql("select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+

>>> spark.sql("select reclamacoes.*, despachantes.nome from despachantes right join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+

>>> spark.sql("select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
| null|      null|  null|   Carminda Pestana|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    1|2020-09-12|     2|    Deolinda Vilela|
| null|      null|  null|   Emídio Dornelles|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    6|2020-01-09|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    4|2020-10-02|     5|     Graça Ornellas|
| null|      null|  null|   Matilde Rebouças|
| null|      null|  null|    Noêmia   Orriça|
| null|      null|  null|      Roque Vásquez|
|    7|2020-01-05|     9|      Uriel Queiroz|
| null|      null|  null|   Viviana Sequeira|
+-----+----------+------+-------------------+

>>> despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "inner").select("idrec","datarec","iddesp","nome").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    2|2020-09-11|     2|    Deolinda Vilela|
|    1|2020-09-12|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    6|2020-01-09|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    4|2020-10-02|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+

>>> despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "right").select("idrec","datarec","iddesp","nome").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+

>>> despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "left").select("idrec","datarec","iddesp","nome").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
| null|      null|  null|   Carminda Pestana|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    1|2020-09-12|     2|    Deolinda Vilela|
| null|      null|  null|   Emídio Dornelles|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    6|2020-01-09|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    4|2020-10-02|     5|     Graça Ornellas|
| null|      null|  null|   Matilde Rebouças|
| null|      null|  null|    Noêmia   Orriça|
| null|      null|  null|      Roque Vásquez|
|    7|2020-01-05|     9|      Uriel Queiroz|
| null|      null|  null|   Viviana Sequeira|
+-----+----------+------+-------------------+

