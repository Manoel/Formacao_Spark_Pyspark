### Fundamentos de Machine Learning

* Aplicações
	
	* Prever Fralde
	* Saber se o aluno vai abandonar a faculdade, anteceder uma ocorrencia
	* Classificar uma doença
	* Reconhecer uma imagem, etc...
	
* Churn

	* Preve se o cliente vai abandonar a empresa.
	* Modelo
	* Treinamento Modelo
	* Classificação
	* Regressão
	
### Machine Learning no Spark

* Bibliotecas

	* spark.mllib
	* spark.ml

### Preparando dados para Regressão

* Importando as bibliotecas

	* from pyspark.ml.regression import LinearRegression, RandomForestRegressor # bibliotecas de regressão.
	* from pyspark.ml.evaluation import RegressionEvaluator # biblioteca de Avaliação de regressão.
	* from pyspark.ml.feature import VectorAssembler # biblioteca de transformação em dados numericos que cria as colunas.
	
* Criando variaveis

	* Carros_temp = spark.read.csv("/home/manoel/download/Carros.csv", inferSchema=True, header=True, sep=";")
	* Carros = Carros_temp.select ("Consumo", "Cilindros", "Cilindradas", "HP")

* Criar o vetor de caracteristicas

	* veccaracteristicas = VectorAssembler(inputCols=[("Consumo"),("Cilindros"),("Cilindradas")], outputCol="caracteristicas") # Metodo VectorAssemblerssembly
	
	* Carros_transf = veccaracteristicas.transform(Carros) # Aplicando a variavel.

* Variaveis de teste e Treino

	* CarrosTreino, CarrosTeste = Carros_transf.randomSplit([0.7,0.3])
	* print(CarrosTreino.count())
	17
	* print(CarrosTeste.count())
	15

### Criando um Modelo de Regressão.

* Criando o objeto LinearRegression

	* reglin = LinearRegression(featuresCol="caracteristicas", labelCol="HP")
	
* Criando de fato o modelo.

	* modelo = reglin.fit(CarrosTreino)
	
* Fazendo previsão (Usamos os dados do CarrosTeste com o modelo criado), pois estes vão ser usados para avaliar o modelo.

	* previsao = modelo.transform(CarrosTeste)
```shell
	
previsao.show(5)
+-------+---------+-----------+---+-----------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|  caracteristicas|        prediction|
+-------+---------+-----------+---+-----------------+------------------+
|     15|        8|        301|335| [15.0,8.0,301.0]| 183.4071808891622|
|     26|        4|       1203| 91|[26.0,4.0,1203.0]|43.505855866174755|
|    104|        8|        460|215|[104.0,8.0,460.0]|191.70735752654866|
|    133|        8|        350|245|[133.0,8.0,350.0]|195.67301386981336|
|    143|        8|        360|245|[143.0,8.0,360.0]|196.66691741583463|
+-------+---------+-----------+---+-----------------+------------------+
only showing top 5 rows
	
```

* Para avaliar se o modelo criado é bom, utilizaremos de uma metrica de performance (RNC - Quando menor, melhor), a melhor forma de avaliar é criando mais de um modelo e os comparando.

* Criando o avaliador de regressão.

	* avaliar = RegressionEvaluator(predictionCol="prediction", labelCol="HP", metricName="rmse")
	
* Avaliando

	* avaliar = RegressionEvaluator(predictionCol="prediction", labelCol="HP", metricName="rmse")
	* rmse = avaliar.evaluate(previsao)
	* print(rmse)
	  51.16750835556104
	  
* Gerar o modelo e metrica, depois comparamos.

	* rfreg = RandomForestRegressor(featuresCol="caracteristicas", labelCol="HP")
	* modelo2 = rfreg.fit(CarrosTreino)
	* previsao2 = modelo2.transform(CarrosTeste)
	* previsao2.show(5)
```shell

+-------+---------+-----------+---+-----------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|  caracteristicas|        prediction|
+-------+---------+-----------+---+-----------------+------------------+
|     15|        8|        301|335| [15.0,8.0,301.0]|  176.711858974359|
|     26|        4|       1203| 91|[26.0,4.0,1203.0]|103.22055555555555|
|    104|        8|        460|215|[104.0,8.0,460.0]|  192.201858974359|
|    133|        8|        350|245|[133.0,8.0,350.0]|  193.076858974359|
|    143|        8|        360|245|[143.0,8.0,360.0]|  193.076858974359|
+-------+---------+-----------+---+-----------------+------------------+
only showing top 5 rows

```

### Preparando Dados para Classificação

* Importando as bibliotecas

```pyspark
from pyspark.ml.feature import RFormula 
from pyspark.ml.classification import DecisionTreeClassifier # Algoritimo de classificação, modelo simples de machine learning
from pyspark.ml.evaluatin import BinaryClassificationEvaluator # Classificação é binaria, sim ou não

churn = spark.read.csv("/home/manoel/download/Churn.csv", inferSchema=True, header=True, sep=";")

churn.show(3)
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
|CreditScore|Geography|Gender|Age|Tenure|Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
|        619|   France|Female| 42|     2|      0|            1|        1|             1|       10134888|     1|
|        608|    Spain|Female| 41|     1|8380786|            1|        0|             1|       11254258|     0|
|        502|   France|Female| 42|     8|1596608|            3|        1|             0|       11393157|     1|
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
only showing top 3 rows

```

* Criando o objeto formula e aplica-se aos dados.
* O ~ ., significa que a classe que fica a esquerda se chama Exited, sendo o "." os demais atributos como variaveis independentes.
* Os nomes featuresCol e labelCol são nomes que eu dei para as colunas que o RFormula vai gerar.
* O parametro truncate é para não truncar os textos que vão ficar grandes.

```pyspark

formula = RFormula(formula="Exited ~ .", featuresCol="features", labelCol="label", handleInvalid="skip")
churn_trans = formula.fit(churn).transform(churn).select("features", "label")

churn_trans.show(truncate=False)
+----------------------------------------------------------------+-----+
|features                                                        |label|
+----------------------------------------------------------------+-----+
|[619.0,1.0,0.0,0.0,42.0,2.0,0.0,1.0,1.0,1.0,1.0134888E7]        |1.0  |
|[608.0,0.0,0.0,0.0,41.0,1.0,8380786.0,1.0,0.0,1.0,1.1254258E7]  |0.0  |
|[502.0,1.0,0.0,0.0,42.0,8.0,1596608.0,3.0,1.0,0.0,1.1393157E7]  |1.0  |
|(11,[0,1,4,5,7,10],[699.0,1.0,39.0,1.0,2.0,9382663.0])          |0.0  |
|[850.0,0.0,0.0,0.0,43.0,2.0,1.2551082E7,1.0,1.0,1.0,790841.0]   |0.0  |
|[645.0,0.0,0.0,1.0,44.0,8.0,1.1375578E7,2.0,1.0,0.0,1.4975671E7]|1.0  |
|[822.0,1.0,0.0,1.0,50.0,7.0,0.0,2.0,1.0,1.0,100628.0]           |0.0  |
|[376.0,0.0,1.0,0.0,29.0,4.0,1.1504674E7,4.0,1.0,0.0,1.1934688E7]|1.0  |
|[501.0,1.0,0.0,1.0,44.0,4.0,1.4205107E7,2.0,0.0,1.0,749405.0]   |0.0  |
|[684.0,1.0,0.0,1.0,27.0,2.0,1.3460388E7,1.0,1.0,1.0,7172573.0]  |0.0  |
|[528.0,1.0,0.0,1.0,31.0,6.0,1.0201672E7,2.0,0.0,0.0,8018112.0]  |0.0  |
|[497.0,0.0,0.0,1.0,24.0,3.0,0.0,2.0,1.0,0.0,7639001.0]          |0.0  |
|[476.0,1.0,0.0,0.0,34.0,10.0,0.0,2.0,1.0,0.0,2626098.0]         |0.0  |
|(11,[0,1,4,5,7,10],[549.0,1.0,25.0,5.0,2.0,1.9085779E7])        |0.0  |
|[635.0,0.0,0.0,0.0,35.0,7.0,0.0,2.0,1.0,1.0,6595165.0]          |0.0  |
|[616.0,0.0,1.0,1.0,45.0,3.0,1.4312941E7,2.0,0.0,1.0,6432726.0]  |0.0  |
|[653.0,0.0,1.0,1.0,58.0,1.0,1.3260288E7,1.0,1.0,0.0,509767.0]   |1.0  |
|[549.0,0.0,0.0,0.0,24.0,9.0,0.0,2.0,1.0,1.0,1440641.0]          |0.0  |
|(11,[0,3,4,5,7,10],[587.0,1.0,45.0,6.0,1.0,1.5868481E7])        |0.0  |
|[726.0,1.0,0.0,0.0,24.0,6.0,0.0,2.0,1.0,1.0,5472403.0]          |0.0  |
+----------------------------------------------------------------+-----+
only showing top 20 rows


```
	
### Criando um Modelo de Classificação

* Dividir o churn_trans em churnTreino e churnTeste, usando um proporção de 70,30

 ```pyspark
 
churnTreino, churnTeste = churn_trans.randomSplit([0.7,0.3])

print(churnTreino.count())
7013
print(churnTeste.count())
2987
 
```
 
* Agora podemos criar o modelo

 ```pyspark
 
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
modelo = dt.fit(churnTreino)

previsao = modelo.transform(churnTeste)
previsao.show()
+--------------------+-----+--------------+--------------------+----------+
|            features|label| rawPrediction|         probability|prediction|
+--------------------+-----+--------------+--------------------+----------+
|(11,[0,1,3,4,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0| [155.0,241.0]|[0.39141414141414...|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|  [20.0,188.0]|[0.09615384615384...|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|    [0.0,40.0]|           [0.0,1.0]|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|  [188.0,46.0]|[0.80341880341880...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|  [188.0,46.0]|[0.80341880341880...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0| [155.0,241.0]|[0.39141414141414...|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4369.0,485.0]|[0.90008240626287...|       0.0|
+--------------------+-----+--------------+--------------------+----------+
only showing top 20 rows

previsao.show(truncate=False)
+--------------------------------------------------------+-----+--------------+----------------------------------------+----------+
|features                                                |label|rawPrediction |probability                             |prediction|
+--------------------------------------------------------+-----+--------------+----------------------------------------+----------+
|(11,[0,1,3,4,7,10],[642.0,1.0,1.0,26.0,1.0,4747268.0])  |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[418.0,1.0,39.0,2.0,2.0,904171.0])   |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[449.0,1.0,21.0,7.0,2.0,1.7574392E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[476.0,1.0,40.0,4.0,2.0,1.8254704E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[480.0,1.0,28.0,6.0,2.0,4813192.0])  |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[483.0,1.0,30.0,9.0,2.0,1.3635697E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[496.0,1.0,36.0,7.0,2.0,1.0809828E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[535.0,1.0,49.0,3.0,1.0,6182041.0])  |1.0  |[155.0,241.0] |[0.39141414141414144,0.6085858585858586]|1.0       |
|(11,[0,1,4,5,7,10],[543.0,1.0,42.0,5.0,2.0,1.0190534E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[545.0,1.0,55.0,5.0,1.0,1003477.0])  |1.0  |[20.0,188.0]  |[0.09615384615384616,0.9038461538461539]|1.0       |
|(11,[0,1,4,5,7,10],[549.0,1.0,25.0,5.0,2.0,1.9085779E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[556.0,1.0,43.0,6.0,3.0,1.2515457E7])|1.0  |[0.0,40.0]    |[0.0,1.0]                               |1.0       |
|(11,[0,1,4,5,7,10],[570.0,1.0,46.0,3.0,2.0,82046.0])    |0.0  |[188.0,46.0]  |[0.8034188034188035,0.19658119658119658]|0.0       |
|(11,[0,1,4,5,7,10],[571.0,1.0,47.0,7.0,2.0,1.1236698E7])|0.0  |[188.0,46.0]  |[0.8034188034188035,0.19658119658119658]|0.0       |
|(11,[0,1,4,5,7,10],[605.0,1.0,34.0,2.0,1.0,3598242.0])  |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[617.0,1.0,27.0,4.0,2.0,1.9026921E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[619.0,1.0,48.0,4.0,1.0,1809496.0])  |1.0  |[155.0,241.0] |[0.39141414141414144,0.6085858585858586]|1.0       |
|(11,[0,1,4,5,7,10],[620.0,1.0,41.0,9.0,2.0,8885247.0])  |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[625.0,1.0,28.0,3.0,1.0,1.8364641E7])|0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
|(11,[0,1,4,5,7,10],[626.0,1.0,35.0,3.0,1.0,8019036.0])  |0.0  |[4369.0,485.0]|[0.900082406262876,0.09991759373712403] |0.0       |
+--------------------------------------------------------+-----+--------------+----------------------------------------+----------+
only showing top 20 rows

```

* Avalidando a performance do Modelo.

```pyspark

avaliar = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label", metricName="areaUnderROC")

areaUnderROC = avaliar.evaluate(previsao)
print(areaUnderROC)
0.6910256677144702

```

### Pipelines

* Exemplo simples (Etapa de tranformação e fit)

* Importando as bibliotecas

```pyspark

rom pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

```

* Obtendo os dados

```pyspark

Carros_temp = spark.read.csv ("/home/manoel/download/Carros.csv", inferSchema=True, header=True, sep=";")
Carros = Carros_temp.select("Consumo", "Cilindros", "Cilindradas", "HP")
Carros.show(4)
+-------+---------+-----------+---+
|Consumo|Cilindros|Cilindradas| HP|
+-------+---------+-----------+---+
|     21|        6|        160|110|
|     21|        6|        160|110|
|    228|        4|        108| 93|
|    214|        6|        258|110|
+-------+---------+-----------+---+
only showing top 4 rows


```

* Criando o vetor de caracteristicas

```pyspark
veccaracteristicas = VectorAssembler(inputCols=[("Consumo"),("Cilindros"),("Cilindradas")], outputCol="caracteristicas")
vec_CarrosTreino = veccaracteristicas.transform(Carros)
vec_CarrosTreino.show(4)
+-------+---------+-----------+---+-----------------+
|Consumo|Cilindros|Cilindradas| HP|  caracteristicas|
+-------+---------+-----------+---+-----------------+
|     21|        6|        160|110| [21.0,6.0,160.0]|
|     21|        6|        160|110| [21.0,6.0,160.0]|
|    228|        4|        108| 93|[228.0,4.0,108.0]|
|    214|        6|        258|110|[214.0,6.0,258.0]|
+-------+---------+-----------+---+-----------------+
only showing top 4 rows


```

* Criando o Modelo

```pyspark

reglin = LinearRegression(featuresCol="caracteristicas", labelCol="HP")
modelo = reglin.fit(vec_CarrosTreino)

```

* Funcionamento do pipeline
* Importando as bibliotecas


```pyspark
from pyspark.ml import Pipeline

```

* Passando os parametros

```pyspark
pipeline = Pipeline(stages=[veccaracteristicas, reglin])

```

* Criando o Modelo

```pyspark
pipelineModel = pipeline.fit(Carros)

```

* Fazendo uma previsao

```pyspark
previsao = pipelineModel.transform(Carros)
previsao.show()
+-------+---------+-----------+---+------------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|   caracteristicas|        prediction|
+-------+---------+-----------+---+------------------+------------------+
|     21|        6|        160|110|  [21.0,6.0,160.0]|162.32154816816646|
|     21|        6|        160|110|  [21.0,6.0,160.0]|162.32154816816646|
|    228|        4|        108| 93| [228.0,4.0,108.0]| 82.51715587712931|
|    214|        6|        258|110| [214.0,6.0,258.0]|141.86680518718754|
|    187|        8|        360|175| [187.0,8.0,360.0]|202.93528239714834|
|    181|        6|        225|105| [181.0,6.0,225.0]| 145.4980634611832|
|    143|        8|        360|245| [143.0,8.0,360.0]|   207.41448530972|
|    244|        4|       1467| 62|[244.0,4.0,1467.0]| 69.69282676584851|
|    228|        4|       1408| 95|[228.0,4.0,1408.0]| 71.80767356085781|
|    192|        6|       1676|123|[192.0,6.0,1676.0]|132.42483285541724|
|    178|        6|       1676|123|[178.0,6.0,1676.0]| 133.8500337821446|
|    164|        8|       2758|180|[164.0,8.0,2758.0]|185.52180807776818|
|    173|        8|       2758|180|[173.0,8.0,2758.0]|184.60560748201488|
|    152|        8|       2758|180|[152.0,8.0,2758.0]| 186.7434088721059|
|    104|        8|        472|205| [104.0,8.0,472.0]| 210.4620247994542|
|    104|        8|        460|215| [104.0,8.0,460.0]|210.56088155929672|
|    147|        8|        440|230| [147.0,8.0,440.0]|206.34823997932406|
|    324|        4|        787| 66| [324.0,4.0,787.0]| 67.15070452800569|
|    304|        4|        757| 52| [304.0,4.0,757.0]| 69.43384775150815|
|    339|        4|        711| 65| [339.0,4.0,711.0]| 66.24979634741939|
+-------+---------+-----------+---+------------------+------------------+
only showing top 20 rows

```