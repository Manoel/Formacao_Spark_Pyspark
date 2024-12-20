## Atividade

* Importando bibliotecas

```pyspark

from pyspark.ml.feature import RFormula
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

```

* Obtendo os dados

```pyspark

iris = spark.read.csv("/home/manoel/download/iris.csv", inferSchema=True, header=True)
iris.show(3)
+-----------+----------+-----------+----------+-----------+
|sepallength|sepalwidth|petallength|petalwidth|      class|
+-----------+----------+-----------+----------+-----------+
|        5.1|       3.5|        1.4|       0.2|Iris-setosa|
|        4.9|       3.0|        1.4|       0.2|Iris-setosa|
|        4.7|       3.2|        1.3|       0.2|Iris-setosa|
+-----------+----------+-----------+----------+-----------+
only showing top 3 rows

```

* Criando a formula

```pyspark

formula = RFormula(formula="class ~ . ", featuresCol="features", labelCol="label", handleInvalid="skip")

```

* Transformar os dados

```pyspark

iris_trans = formula.fit(iris).transform(iris).select("features","label")
iris_trans.show(5)
+-----------------+-----+
|         features|label|
+-----------------+-----+
|[5.1,3.5,1.4,0.2]|  0.0|
|[4.9,3.0,1.4,0.2]|  0.0|
|[4.7,3.2,1.3,0.2]|  0.0|
|[4.6,3.1,1.5,0.2]|  0.0|
|[5.0,3.6,1.4,0.2]|  0.0|
+-----------------+-----+
only showing top 5 rows

```


*  Dividir os dados em Treino e Teste

```pyspark

irisTreino, irisTeste = iris_trans.randomSplit([.7,.3])

```

* Criando o modelo

```pyspark

nb = NaiveBayes(labelCol="label", featuresCol="features")
modelo = nb.fit(irisTreino)

```

*  Previsão Teste

```pyspark
previsao = modelo.transform(irisTeste)
previsao.show()
+-----------------+-----+--------------------+--------------------+----------+
|         features|label|       rawPrediction|         probability|prediction|
+-----------------+-----+--------------------+--------------------+----------+
|[4.3,3.0,1.1,0.1]|  0.0|[-10.002540989152...|[0.70116974105914...|       0.0|
|[4.6,3.2,1.4,0.2]|  0.0|[-11.368966738487...|[0.66309819853156...|       0.0|
|[4.7,3.2,1.6,0.2]|  0.0|[-11.824754971088...|[0.63389749199653...|       0.0|
|[4.8,3.0,1.4,0.1]|  0.0|[-10.935292062967...|[0.67479927233059...|       0.0|
|[4.8,3.0,1.4,0.3]|  0.0|[-11.649735939607...|[0.61588941290050...|       0.0|
|[4.8,3.4,1.6,0.2]|  0.0|[-12.114695186785...|[0.66471400016390...|       0.0|
|[4.8,3.4,1.9,0.2]|  0.0|[-12.691633796439...|[0.61360428059770...|       0.0|
|[4.9,3.1,1.5,0.1]|  0.0|[-11.308156287115...|[0.67559053061156...|       0.0|
|[5.0,3.0,1.6,0.2]|  0.0|[-11.819464726721...|[0.62075624599908...|       0.0|
|[5.0,3.2,1.2,0.2]|  0.0|[-11.268990970045...|[0.71179073171020...|       0.0|
|[5.0,3.5,1.3,0.3]|  0.0|[-12.146692362547...|[0.70570762105530...|       0.0|
|[5.0,3.5,1.6,0.6]|  0.0|[-13.795296787162...|[0.56616305235734...|       0.0|
|[5.0,3.6,1.4,0.2]|  0.0|[-12.091172155543...|[0.72848957888532...|       0.0|
|[5.1,2.5,3.0,1.1]|  1.0|[-17.251060535662...|[0.12035323281509...|       1.0|
|[5.2,4.1,1.5,0.1]|  0.0|[-12.615532379932...|[0.79709355576890...|       0.0|
|[5.3,3.7,1.5,0.2]|  0.0|[-12.606361365356...|[0.73687079774546...|       0.0|
|[5.4,3.7,1.5,0.2]|  0.0|[-12.677523858188...|[0.74066366010639...|       0.0|
|[5.5,2.3,4.0,1.3]|  1.0|[-19.954505359615...|[0.04580644861088...|       1.0|
|[5.5,2.4,3.7,1.0]|  1.0|[-18.415289796432...|[0.08636181748810...|       1.0|
|[5.6,3.0,4.1,1.3]|  1.0|[-20.983702752356...|[0.06436907578397...|       1.0|
+-----------------+-----+--------------------+--------------------+----------+
only showing top 20 rows


```

* Avaliar a performance

```pyspark

avaliar = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")
resultado = avaliar.evaluate(previsao)
print(resultado)
0.9166666666666666

```