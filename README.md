<h3>SPARK API : Manipulação de dados e operações básicas em Delta table</h3>

**OBJETIVOS**: Realizar operações básicas em DataFrame.





:heavy_check_mark:**WORKFLOW:**

----------------------------



<table>
  <thead>
    <tr>
      <th>Etapa</th>
      <th>Descrição</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Carregar Dados Brutos</td>
      <td>Importar dados de um arquivo CSV (clientes_raw.csv) para staging area (sgt_clientes).</td>
    </tr>
    <tr>
      <td>Transformar Dados</td>
      <td>Limpar, validar ou e enriquecer os dados conforme necessário usando a API do Spark.</td>
    </tr>
    <tr>
      <td>Salvar Dados</td>
      <td>Armazenar os dados transformados em uma Delta Table (delta_clientes).</td>
    </tr>
  </tbody>
</table>



:heavy_check_mark:**PROJETO**

----------------------------------------------------------



Vamos fazer upload de arquivo em csv para Databricks File System (DBFS), converter os dados brutos em delta format. Realizar tranformações utilizando operações básicas do SPARK e salvar na staging area. Carregar os dados limpos e transformados na camada final para análise, relatório ou ingestão em database.



:heavy_check_mark:**ARQUITETURA E NOMENCLATURA**


--------------------------

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdb6X3QmmbC-V3gupmiW0KJYSfxMqmGo-K8RrKdsKjESzlNf5ZrqreEtBb9N6CTxjRDaUK90S9Sa1xKOaIwbMekmaYtz1ipa0lMoGq6YdJ3o69PsT3xScQtu1f6cR3WDj9MHvD5aA9o1FAC-jJudT4pbmGK?key=CuBAMnRiidRX8nJElFVlpA)



:heavy_check_mark:**​ARQUIVOS DA TAREFA:**

:pushpin:[Notebook](https://github.com/RegiMaria/SPARK_API_operacoes_basicas/blob/main/notebook/delta_clientes.ipynb)

:pushpin:[How to do](https://github.com/RegiMaria/SPARK_API_operacoes_basicas/blob/main/How_to_do.md)​



**FERRAMENTAS:**

:pushpin:[Apache Spark](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)

:pushpin:[Databricks-communit](https://community.cloud.databricks.com/?o=3957069414199263)y 



**REFERÊNCIAS:**

:pushpin:[DeltaLake](https://www.databricks.com/sites/default/files/2023-10/oreilly-delta-lake_-up-and-running.pdf) -Modern Data Lakehouse Architectures with Delta Lake

:pushpin:[5-hours-understanding-more-about-the-delta-lake-table](https://blog.det.life/i-spent-5-hours-understanding-more-about-the-delta-lake-table-format-b8516c5091eb)

:pushpin:[Github Delta Examples](https://github.com/delta-io/delta-examples/tree/master/notebooks/pyspark)

:pushpin: [Spark_Documentation](https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#dataframe-apis)

:pushpin:[Como funciona o sistema de arquivos do databricks](https://docs.databricks.com/pt/dbfs/index.html)
