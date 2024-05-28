
## Atividade 1

### Importando os arquivos parquet

```pyspark

>>> clientes = spark.read.load("/home/manoel/download/Atividades/Clientes.parquet")
>>> vendas = spark.read.load("/home/manoel/download/Atividades/Vendas.parquet")
>>> itensvendas = spark.read.load("/home/manoel/download/Atividades/ItensVendas.parquet")
>>> produtos = spark.read.load("/home/manoel/download/Atividades/Produtos.parquet")
>>> vendedores = spark.read.load("/home/manoel/download/Atividades/Vendedores.parquet")

```

### Criando o database

```pyspark

>>> spark.sql("create database vendasvarejo")

```

### Acessando o database vendasvarejo

```pyspark

>>> spark.sql("use vendasvarejo").show()

```

### Criando as tabelas

```pyspark

>>> clientes.write.saveAsTable("clientes")
>>> vendas.write.saveAsTable("vendas")
>>> itensvendas.write.saveAsTable("itensvendas")
>>> produtos.write.saveAsTable("produtos"
>>> vendedores.write.saveAsTable("vendedores")

```

### Mostrando as tabelas criadas
```pyspark
>>> spark.sql("show tables").show()
+------------+-----------+-----------+
|   namespace|  tableName|isTemporary|
+------------+-----------+-----------+
|vendasvarejo|   clientes|      false|
|vendasvarejo|itensvendas|      false|
|vendasvarejo|   produtos|      false|
|vendasvarejo|     vendas|      false|
|vendasvarejo| vendedores|      false|
+------------+-----------+-----------+

```

### Visualisando os dados da tabela produtos.

```pyspark
>>> spark.sql("select * from produtos").show()

+---------+--------------------+---------+
|ProdutoID|             Produto|    Preco|
+---------+--------------------+---------+
|        1|Bicicleta Aro 29 ...|8.852,00 |
|        2|Bicicleta Altools...|9.201,00 |
|        3|Bicicleta Gts Adv...|4.255,00 |
|        4|Bicicleta Trinc C...|7.658,00 |
|        5|Bicicleta Gometws...|2.966,00 |
|        6|Bicicleta Gometws...|2.955,00 |
|        7|Capacete Gometws ...|  155,00 |
|        8|Luva De Ciclismo ...|  188,00 |
|        9|Bermuda Predactor...|  115,00 |
|       10|Camiseta Predacto...|  135,00 |
+---------+--------------------+---------+

```

## Atividade 2

```pyspark

>>> spark.sql("select clientes.Cliente, vendas.Data, produtos.Produto, vendedores.vendedor, itensvendas.valortotal from clientes inner join vendas on (clientes.clienteid = vendas.clienteid) inner join itensvendas on (vendas.vendasid = itensvendas.vendasid) inner join produtos on (itensvendas.produtoid = produtos.produtoid) inner join vendedores on (vendas.vendedorid = vendedores.vendedorid)").show()
+-----------------+--------+--------------------+----------------+----------+
|          Cliente|    Data|             Produto|        vendedor|valortotal|
+-----------------+--------+--------------------+----------------+----------+
|   Cosme Zambujal|1/1/2019|Bicicleta Altools...|    Armando Lago|   7820.85|
|   Cosme Zambujal|1/1/2019|Bermuda Predactor...|    Armando Lago|     97.75|
|   Cosme Zambujal|1/1/2019|Camiseta Predacto...|    Armando Lago|     135.0|
|Gertrudes Hidalgo|1/1/2020|Luva De Ciclismo ...|   Iberê Lacerda|     150.4|
| Antão Corte-Real|2/1/2020|Capacete Gometws ...|Jéssica Castelão|     155.0|
| Antão Corte-Real|2/1/2020|Bicicleta Gometws...|Jéssica Castelão|    5932.0|
| Antão Corte-Real|2/1/2019|Bicicleta Altools...|  Hélio Liberato|   7820.85|
| Antão Corte-Real|2/1/2019|Bermuda Predactor...|  Hélio Liberato|     97.75|
| Antão Corte-Real|2/1/2019|Bicicleta Gometws...|  Hélio Liberato|    5910.0|
| Antão Corte-Real|3/1/2018|Bicicleta Gometws...|  Hélio Liberato|    2955.0|
| Antão Corte-Real|3/1/2018|Bicicleta Trinc C...|  Hélio Liberato|    7658.0|
| Antão Corte-Real|3/1/2018|Bicicleta Aro 29 ...|  Hélio Liberato|    8852.0|
| Antão Corte-Real|3/1/2018|Camiseta Predacto...|  Hélio Liberato|     121.5|
| Antão Corte-Real|3/1/2018|Bicicleta Gts Adv...|  Hélio Liberato|   6510.16|
| Antão Corte-Real|4/1/2020|Bicicleta Altools...|  Hélio Liberato|   18402.0|
| Antão Corte-Real|6/1/2019|Bicicleta Aro 29 ...|  Hélio Liberato|    7524.2|
|Gertrudes Infante|6/1/2019|Luva De Ciclismo ...|  Hélio Liberato|     376.0|
|Gertrudes Infante|6/1/2019|Bicicleta Gts Adv...|  Hélio Liberato|   3616.75|
|Gertrudes Infante|6/1/2019|Camiseta Predacto...|  Hélio Liberato|     108.0|
|Gertrudes Infante|6/1/2019|Bermuda Predactor...|  Hélio Liberato|     115.0|
+-----------------+--------+--------------------+----------------+----------+
only showing top 20 rows

```

```pyspark

>>> spark.sql("select count(*) from itensvendas").show()
+--------+
|count(1)|
+--------+
|     940|
+--------+

>>> spark.sql("select clientes.Cliente, vendas.Data, produtos.Produto, vendedores.vendedor, itensvendas.valortotal from clientes inner join vendas on (clientes.clienteid = vendas.clienteid) inner join itensvendas on (vendas.vendasid = itensvendas.vendasid) inner join produtos on (itensvendas.produtoid = produtos.produtoid) inner join vendedores on (vendas.vendedorid = vendedores.vendedorid)").count()
940


```