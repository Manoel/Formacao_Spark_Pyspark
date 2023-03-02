spark-sql> show databases;
default
desp
Time taken: 2.334 seconds, Fetched 2 row(s)
spark-sql> use desp;
23/03/02 18:52:42 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Time taken: 0.034 seconds
spark-sql> use desp;
Time taken: 0.026 seconds
spark-sql> show tables;
despachantes
despachantes_ng
reclamacoes
Time taken: 0.105 seconds, Fetched 3 row(s)
spark-sql> select * from despachantes;
1	Carminda Pestana	Ativo	Santa Maria	23	2020-08-11
2	Deolinda Vilela	Ativo	Novo Hamburgo	34	2020-03-05
3	Emídio Dornelles	Ativo	Porto Alegre	34	2020-02-05
4	Felisbela Dornelles	Ativo	Porto Alegre	36	2020-02-05
5	Graça Ornellas	Ativo	Porto Alegre	12	2020-02-05
6	Matilde Rebouças	Ativo	Porto Alegre	22	2019-01-05
7	Noêmia   Orriça	Ativo	Santa Maria	45	2019-10-05
8	Roque Vásquez	Ativo	Porto Alegre	65	2020-03-05
9	Uriel Queiroz	Ativo	Porto Alegre	54	2018-05-05
10	Viviana Sequeira	Ativo	Porto Alegre	0	2020-09-05
Time taken: 2.37 seconds, Fetched 10 row(s)
spark-sql> select nome, vendas from despachantes;
Carminda Pestana	23
Deolinda Vilela	34
Emídio Dornelles	34
Felisbela Dornelles	36
Graça Ornellas	12
Matilde Rebouças	22
Noêmia   Orriça	45
Roque Vásquez	65
Uriel Queiroz	54
Viviana Sequeira	0
Time taken: 0.159 seconds, Fetched 10 row(s)
spark-sql> select * from despachantes where vendas > 20;
1	Carminda Pestana	Ativo	Santa Maria	23	2020-08-11
2	Deolinda Vilela	Ativo	Novo Hamburgo	34	2020-03-05
3	Emídio Dornelles	Ativo	Porto Alegre	34	2020-02-05
4	Felisbela Dornelles	Ativo	Porto Alegre	36	2020-02-05
6	Matilde Rebouças	Ativo	Porto Alegre	22	2019-01-05
7	Noêmia   Orriça	Ativo	Santa Maria	45	2019-10-05
8	Roque Vásquez	Ativo	Porto Alegre	65	2020-03-05
9	Uriel Queiroz	Ativo	Porto Alegre	54	2018-05-05
Time taken: 0.283 seconds, Fetched 8 row(s)
spark-sql> select cidade, sum(vendas) from despachantes group by cidade order by 2 desc;
Porto Alegre	223
Santa Maria	68
Novo Hamburgo	34
Time taken: 1.453 seconds, Fetched 3 row(s)
spark-sql> select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on (despachantes.id = reclamacoes.iddesp);
1	2020-09-12	2	Deolinda Vilela
2	2020-09-11	2	Deolinda Vilela
3	2020-10-05	4	Felisbela Dornelles
4	2020-10-02	5	Graça Ornellas
5	2020-12-06	5	Graça Ornellas
6	2020-01-09	5	Graça Ornellas
7	2020-01-05	9	Uriel Queiroz
Time taken: 0.452 seconds, Fetched 7 row(s)
spark-sql> select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes on (despachantes.id = reclamacoes.iddesp);
NULL	NULL	NULL	Carminda Pestana
2	2020-09-11	2	Deolinda Vilela
1	2020-09-12	2	Deolinda Vilela
NULL	NULL	NULL	Emídio Dornelles
3	2020-10-05	4	Felisbela Dornelles
6	2020-01-09	5	Graça Ornellas
5	2020-12-06	5	Graça Ornellas
4	2020-10-02	5	Graça Ornellas
NULL	NULL	NULL	Matilde Rebouças
NULL	NULL	NULL	Noêmia   Orriça
NULL	NULL	NULL	Roque Vásquez
7	2020-01-05	9	Uriel Queiroz
NULL	NULL	NULL	Viviana Sequeira
Time taken: 0.34 seconds, Fetched 13 row(s)
spark-sql>