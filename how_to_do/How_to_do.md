<h3>HOW TO DO: </h3>

<h3> SPARK API : MANIPULAÇÃO E OPERAÇÕES BÁSICAS EM DELTA TABLES</h3></h3>



:heavy_check_mark:**OBJETIVO:** Aplicar transformações e operações básicas em *Delta Tables* usando a **API do Spark**.



Relembrando o pipeline:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdb6X3QmmbC-V3gupmiW0KJYSfxMqmGo-K8RrKdsKjESzlNf5ZrqreEtBb9N6CTxjRDaUK90S9Sa1xKOaIwbMekmaYtz1ipa0lMoGq6YdJ3o69PsT3xScQtu1f6cR3WDj9MHvD5aA9o1FAC-jJudT4pbmGK?key=CuBAMnRiidRX8nJElFVlpA)

**ARQUITETURA**

-------------------------------------------------

- **Medallion architecture**: Para essa tarefa, escolhemos trabalhar com a [Arquitetura Medalhão](https://www.databricks.com/glossary/medallion-architecture). O objetivo desse designe é melhorar a estrutura e qualidade dos dados de forma progressiva à medida que eles fluem por cada camada da arquitetura.

  :white_check_mark:**BRONZE LAYER**: Na primeira camada os dados são carregados "como estão".

  :white_check_mark: **SILVER LAYER** : Na segunda camada, os dados são limpos, normalizados, mesclados, conformados, etc. Nessa fase, os dados tomam forma para se alinhar às necessidades da organização.

  :white_check_mark:**GOLD LAYER:**  Essa camada reúne os dados "prontos pra consumo".

:pushpin: Essa arquitetura de dados é profundamente estudada no capítulo 10 do livro [Delta Lake: Up & Running](https://www.databricks.com/resources/ebook/delta-lake-running-oreilly?scid=7018Y000001Fi0cQAC&utm_medium=paid+search&utm_source=google&utm_campaign=19774681807&utm_adgroup=146097674305&utm_content=ebook&utm_offer=delta-lake-running-oreilly&utm_ad=665998512207&utm_term=databricks%20delta%20table&gad_source=1&gclid=Cj0KCQjwq_G1BhCSARIsACc7Nxr_fRUKlfFNwdEJcSQialAIdzaQKJOkCaB1IfL28GnNWE4SBAfyA2IaAs_BEALw_wcB).



<h3>DATABRICKS </h3>

-----------------------

Para essa tarefa utilizamos um *notebook* na plataforma *Databricks*. 

Obs: Na versão *community* após certo tempo de inatividade o cluster é '*Terminated*'- o que significa que  terá que ser iniciado um novo cluster.

Depois de cria o **CLUSTER** e o seu **WORKSPACE** na plataforma:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdvqeyQhzc-LJgYnJinfCoWIgcrFbQhIvsmDpjGB4Mxkes6wYW3OZN_3RS5P2qnmRp8kmN54iFLZE2VEJVRUS6qZy1tCHB67yizWpSINKBhTK275TOeV_yXO9lkB5JCc5mSAnKz7Rj1k4s5TXmDT_Hh8PA?key=CuBAMnRiidRX8nJElFVlpA)

Realizamos o upload dos arquivos brutos no DataBricks File System:

<h3> NOTEBOOK </h3>

-----------------------------------

**1.Upload do arquivo no *Databricks File System*:**![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXerPz3HtxObzlRiLS5P3tZf1DV2lz_7X3TrKKTefJbZ1Ls1lNk0JXpWxPhIRVxdf8TIwKCVYiIR-ARKjVBkfE-D4OLzUWw2KImtLvTOzgFajv2ngAEeuJ0i-yYb2a9OAjvsr2GVG8uSybOGEll8_5WbCS7M?key=CuBAMnRiidRX8nJElFVlpA)

*OBS: Atenção ao endereço no DBFS. O spark exige caminho absoluto para localizar o arquivo.*

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdoD2--ULkUfcZQOZB89z3VyegFuopvIOkS_d2mSKRSI3n7krkJHdPFRgLqW7tFklGMwYWhl3Cq5CvbLHz129DdWS_Z4IiDKy6XIgH426WALVSFXx3n4dwxCDPiLp5KhFbww-20S3ZXowPnxNtl4vwiHeZW?key=CuBAMnRiidRX8nJElFVlpA)

**2.Criamos um novo notebook:**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdw-2jiHZqxExxXV5HjOKYa99VgGq9VsLjkrZVWMrVVacWelX1DjCpU78Q6w5mO2duLD9z2wdyClk5Z5NV3j83lAluCLOnebtuBwoXdfP4LWqoSveLWp9CDT_AFPrbzWKuzdWfEVYq1VuG_XjXpsUhQcMuy?key=CuBAMnRiidRX8nJElFVlpA)



<h3>BROZE: PRIMEIRA CAMADA</h3>

----------------------------------

Na primeira camada armazenamos os dados brutos, exatamente como foram recebidos, sem transformações. Além da preservação dos dados originais, caso ocorra algum erro nas etapas posteriores poderemos reprocessar os dados a parti dos originais.

**3. Na primeira célula localizamos o arquivo indicando o caminho absoluto no DBFS e carregamos:**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcXXfUpUQalryoGiRAh9qEJv7cyEJF543LgDKEP51IMBs9HYu5s7qP6ZBIeUob3FcNXX-cfKCXQP6ljqK61gQbmmZTUrqkXJlez_E8jWf_RqpMrmob6KKYLNrKDU0lpmcypcTzJcg7VWqM6ndCrKWvmFeDo?key=CuBAMnRiidRX8nJElFVlpA)

**OPERAÇÕES COM CSV: NÃO PERSISTE DADOS**

---------------------------

:small_orange_diamond: **SPARK API + CSV :** Podemos realizar operações com csv ( filtros, agregações e transformações) usando SPARK diretamente, mas quando transformamos e salvamos de volta em CSV, os tipos de dados e outras informações de <u>metadados **NÃO são preservados**</u>. 

:small_orange_diamond: **SQL + SPARK + CSV : **Para usar SQL em um DataFrame carregado de um CSV, precisamos registrar esse DataFrame como uma **view temporária**. Somente **depois de registrar a view temporária**, podemos executar consultas SQL diretamente nela. A view temporária **existe apenas durante a sessão atual do Spark e não persiste além dela**. Se a sessão é reiniciada ou encerrada, a view desaparece e perdemos os dados e metadados da view. Novamente, informações de <u>metadados **NÃO são preservados**</u>.

HAELEN,B.DAVIS, D. Delta Lake Up and Running. O'Reilly, 2024.





![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcPcd2tqlCxg2EpCwYUTHZie3jU7KcupipeCfQtAJ-SNco6MLp9rvn1zRt8nG2ZugWNlIkijpXd7aYHQJmpZU0R5r0pHXEUvfK0Epm73s5Et154p-8L0ZwJ9-8eL-tqTfAvTtTv6HkZhkSl94gwHESTdJio?key=CuBAMnRiidRX8nJElFVlpA)



<h3>SILVER : SEGUNDA CAMADA </h3>

Na segunda camada salvamos os dados e um formato intermediário**(stg_clientes)**. Como dito anteriormente, essa camada é onde ocorre a transformação dos dados. Aqui, limpamos, filtramos e transformamos os dados para prepará-los para as análises ou cargas finais.

**4. Salvamos os dados na camada intermediária em formato delta (stg_clientes):**


![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXfzSPqggnmqsTOPi3tRKMkt24DLHFP9rbKCoJVR9d5hvpyqQS3-E0fwqfsUT-he2KoypBPNqaTh2L6BQpvUE0CyuVjn-w6pCBZFTcN5i8Zz7C5v0_4qS4sX3Q_x21iLAQZMJhFNBXa6SdmRiotjuoeHdHY?key=CuBAMnRiidRX8nJElFVlpA)

De acordo com Davis e Haelen(2024), o uso do **formato delta** traz inúmeras vantagens como:

- **Delta Lake:** Usar o formato *Delta* permite maior controle sobre transações, versionamento de dados, e a capacidade de realizar operações ACID (Atomicidade, Consistência, Isolamento, Durabilidade).

- **Auditoria e Rastreamento:** O formato *Delta* permite que você rastreie mudanças nos dados, o que é útil para auditoria e controle de qualidade.

- **Performance:** Os dados em *Delta* podem ser indexados e particionados, melhorando a performance das consultas em comparação com formatos brutos como CSV.

**5. OPERAÇÕES BÁSICAS:**

- Filter
- GroupBy
- agg
- Count
- OrderBy

Localizamos e carregamos os dados na camada intermediária:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXe-Rg_83EOisk3AgIuIynNHSpWuH3fiz3DYvwbrbm7pEiEStODe08QpAG4wijdtlX2iZQ_HBc704Bp6GIlIG8f-xE5g-pHH0LSRiDK8wIPI7I-8y9JLDHaV3GsDolqpceoVdlSpSdahJQyxk9ZSFcTpZvM?key=CuBAMnRiidRX8nJElFVlpA)



**Filtrando informações:**

:black_medium_small_square: Filtrar pelos CPFs que começam com dígito 5:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXecOMbfuihBLjmXwkaFMy6AIqRwqmyo6JruHT_o1Op2Tch1fnbjdJ3FNVKOOx2VTHHk1X1f3m_WRstt4kTP3uE7pMTFULNTCvw6m1oUDxLlFZasRHKowvQABv4YGL-UXPqq49hYaLBMf4VclrZ5LNjaqTQ?key=CuBAMnRiidRX8nJElFVlpA)

:black_medium_small_square: Filtrar nomes que começam com A ou B e que tenham CPF iniciado com 5:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdI9Gk9BN4BID-fy1icA8sj0L-kouaJUnZC-Ak2lJST_EsUEesV1124h6PPLHuVBjevWZDQ5NiFW6PCVK2FyWuc7vBdW94t82rsqtyEUWoky4WFV82jjuTCqpfqB8au30DGvid9mT1M0h542bxlHv_ggskZ?key=CuBAMnRiidRX8nJElFVlpA)

Resposta:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXf3JyiQTxafj73sWHmFx3KgZlK1Ka182FekpCCTTcrhJ_9emDHwGMNCOoBPcMJFduuRLuh0SoQ3M8m14RT9xs4-tXO1inQdOI2qfFt2hrR1lJoUqrHnjpgIJG3EeZEYKaoY1sZU7WOfIrS9LNMtKx6uyD0?key=CuBAMnRiidRX8nJElFVlpA)

**APLICANDO TRANSFORMAÇÕES NO CAMPO 'CPF'**

- **Agrupando e contando : GroupBy/agg /count:**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXeZRuPVVX3O39Ogjz1QM9xwIzPVNwLNazpVW5m53e1xgsAszl212S-BhcXFZBAcSuLGZ5LYM447OTxPYVrlooKMIM6MHkVOCCD-n_zdBjq5JsMH1Bobj9-ZLIEbDwT9N-dtlUN0o0i7QvMCsMXvnSLIyP4?key=CuBAMnRiidRX8nJElFVlpA)

:black_medium_small_square: Filtrando quantidade de CPFs duplicados:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdgO_GVgj1dN2FsJgCCOEQ-b7gXq_IoBHhlqI-vJAkGoyOfDS-A7gn8vsMfxo-1Bix4E0YjOPTUAuV13zQP5LjKypt8O8VGfKv5ap1ybxcznPyo2q8yIujT6twGjLmGoN0EFVlfBSSC0shAStKWpTZxbL7d?key=CuBAMnRiidRX8nJElFVlpA)

:black_medium_small_square: Encontrando registros duplicados:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcByCmzXmtQVL_55GAccempBw2PYJEhlSghsLk_42B4xH6dfm6ljmmHy6ImzPgiqMasah3yN89ZcLLWtj0QCgGC_WDYTyyE_7J6ZqdgLe7ceG_qVvc_Md8U9RYF7nUpw0ckdyniNU6X3nQm_UVo1dP--v22?key=CuBAMnRiidRX8nJElFVlpA)

**Atualizando o registro :**

:black_medium_small_square: Alterando o cpf de cliente específico: Luiz Miguel Pires,  cpf 637.824.051-53 para cpf 637.824.059-59.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXeaxbZergt4Lj6f6jfZdlwbk7cuXWOwHKtS3XZHbPM9_uaytKz1y0mHzLjbCpVVZNDy-_hOfaYi2_dI4n0LKKxRcIZiucP3P5afUg2rl5tyObvcAVQKUNa2v2W6-Wc2SUJmdE6FQ-Hn_erGqYWp_WMV5TeF?key=CuBAMnRiidRX8nJElFVlpA)



**Verificando alteração :**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdeBgcEPqf_Gzly8TgB0AhysQqaq0Fk8KgasKEP-Hb0t-s-qim5JQyo_9dUyJ7VSnO4myxXIP0_x9Fo1ytFZAJPSnI1XfVOrwwTaQBHQSQpMI0_O3pPInVondJnyAtbPgMWpUwubObl_NQfyRY56zCxoiA?key=CuBAMnRiidRX8nJElFVlpA)

**Salvamos as alterações realizadas:**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXd3K9GlTchTVRZWdp8p9datFZEnm-AfHVfPe1aYx39aZ6avXo72V5axSZcFQ2uNuZ-c8BFIUIpEFoh2wMve74yLGB94II4GjK7-nyrHSje2-Pvt68SkYBQbxHTs3gfoWjvup6J3in94tYVqr9t8USdS4Gw3?key=CuBAMnRiidRX8nJElFVlpA)

**Verificamos novamente :**

O cpf do cliente 'Luiz Miguel Pires' deverá retornar como '637.824.059-59'

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcHD9YMoQgClGefIlO3BfQJmDqajrLtKqEB_O_PF3vtg3QvXObBs_p5HyOscBAbrBmzlTD9TPrz2oKCZn8G8wiGpmxSmkkQkMw1awSZzc46OytRc54RWPwfZKObTGiJKoZQX4iOrzgGcJVivupBMJ6yKZ0?key=CuBAMnRiidRX8nJElFVlpA)

**TRANSFORMAÇÃO DO CAMPO 'TELEFONE':**

Ao inspecionarmos o campo, deveremos aplicar as seguintes transformações:

- Limpeza do campos dos caracteres especiais não numéricos;
- Padronização dos números de acordo com padrão brasileiro (DDD) XXXX - XXXX;

Limpeza do campo:

:heavy_check_mark: Função `regex_replace`:

A função `regexp_replace`remove caracteres não numéricos.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcp4lAmsar7MBjF4KVgKVOjdkkTuFfiwqB4tGYvhs8UQv1KvJDZeVpVMy2NNdNtKXi0u4mzT8ihLlOVCZp1goX3VeD_5qXHrE93rvKhSd67dEU8bdQvWH5_Jm4lnh3-LE1INPlYXYxwpBZ302NAaDqJZ6g3?key=CuBAMnRiidRX8nJElFVlpA)

Padronizando o grupo de números telefônicos  para (DDD) XXXX - XXXX.

Inspecionando o campos  `Telefone` é possível perceber que o comprimento dos número variam, alguns tem 10 dígitos, outros  11, 12 e 13 dígitos. 

:heavy_check_mark:Função `length`:

Vamos usar a função `length` do PySpark para calcular o comprimento dos números e então agrupar e contar os diferentes comprimentos.

Criando coluna `telefone_length`para armazenar o comprimento dos dígitos:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXea8wMt_51SKPQoc7au0KuMEEom6a9vmWxhlk-foxCqpy9hRYqF26-od61ZZ9bljqJ31UB1ZTkxBclowE4--4VB64CqWeb90PZEyYifswqaasAqvYc9KySrhqh8xU5J7lEmzjTIVno4ZHlGyan8YYi6NWWB?key=CuBAMnRiidRX8nJElFVlpA)

Agora vamos usar a função GroupBy e Count para agrupar os comprimentos encontrados:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXeJ4Uz8RIwJ5qcWxpJQ3ebNPsQpt0jDhD-MhfvEuTVghnnL08pqZP26MEdgn3dvUwEdBwFDOST7DMA9toa2BkiQxEGioDbIRVvH8ZvwXylWrJo2tP1EYllM5ydZbGQPg_yeD6Ldv-H7eI8XKRtOtaR-DRhs?key=CuBAMnRiidRX8nJElFVlpA)

Padronizando o formato do número de telefone:

Vamos aplicar a lógica de padronização para o comprimento de cada grupo.

:black_medium_small_square: Comprimento com 13 dígitos:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcrhNPypV48cm-rEUQiVwlOwbEFRScE_ZBQnZW5uX2gjGxiHwyX7dHBMRCM3oEZyTedC6xbKer3TbqIL_W6ycmy-uP84KW77SAGNTzzOSTm5Nq4KXrSP9Y2NjvQZtIt-85aYP3zFvfIPgX0Mmq0rxPfdTa6?key=CuBAMnRiidRX8nJElFVlpA)

Resposta:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcRX4SVB5pDUhqed6vakUe95mAzkh76mNm19rXamUe38jn7y-c5zdOWIIqiDdOES0ePfDEHfIIpZmV7-cjHa3Lj8KWP-yLQeyfWoWk8jp_wekCt1iyyRdDgFtPFE_MHEORMzdlTDrujmN9ZzjcfO8s95I1m?key=CuBAMnRiidRX8nJElFVlpA)

:heavy_check_mark: Salvamos as alterações até aqui.

Nesse transformação usamos as funções **Substring, lit e  concat.**

**Consulte a documentação do `pyspark.sql.functions` sobre substring, lit e concat.**

:pushpin: [PySpark API Documentation: Substring](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.substring.html)

:pushpin: [PySpark API Documentation - concat](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.concat.html#pyspark.sql.functions.concat)

:pushpin: [PySpark API Documentation - lit](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.lit.html#pyspark.sql.functions.lit)



Também aplicamos os seguintes componentes:

- **`rlike("^[0-9]{10}$")`** : Essa expressão regular (regex) aplica um filtro para certificar que as linhas onde o valor da coluna `Telefone` possui exatamente 10 dígitos.

- **`^ e  $`:** Sinaliza o começo e o fim da *string*.
- **`[0-9]{10}`**: Define exatamente 10 dígitos numéricos.



**APLICANDO TRANSFORMAÇÕES AO CAMPO 'EMAIL'**

Vamos aplicar as seguintes alterações:

- Extrair o primeiro nome do cliente;
- Especificar ele como minúsculo;
- Limpar caracteres não alfabéticos;
- Concatenar o primeiro nome minúsculo com `@exemplo.net`

Para isso vamos usar as funções:

- split
- regex_replace
- concat_ws (concatenação com separador)
- lit



![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXeAue4u2sf3ltPKrDfFJQrkDnLnmk-JuG7sspm0qHjrBVkN2c3J-OxocwV_uwryk35cBSm9L4_TVySrbAO5A36ham9g9zgsM7SYv31fjEUCeeFMNTwqIeycE29wh-XxpdbsQyWwNxhpEANHHDcSPpo0jQA_?key=CuBAMnRiidRX8nJElFVlpA)

Resposta:

Ao inspecionarmos os campos novamente, foi possível perceber que alguns nomes traziam pronomes de tratamento como 'Sr.' ou 'Dr.'.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXfwv8FU7y4VlMKwGR4oKfmtrQJfWfrQVqizEbGdIiJOuDFQ5NK-mh_ctpgSlUWXWhkXM0z0_KQYtxZoLFENcApI746NrQHxwDpCltza3ucPioCIdtrG6rNAqkzmyjNlTOeG1Fyh8j2aNUL2lJkV9niqPhgu?key=CuBAMnRiidRX8nJElFVlpA)



Vamos realizar a contagem dos prefixos:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXfZqySCvUqvgOjiPMNP4SzjoS6BGe-wBrq8KSiThA9-ZEQiv98-sfmM2V9qS-d3KDzCLLfOhztsHbHrrGt3aVbLGFWRGxPrCYIvCFgQK35C0UtrDRaRnfc2_7YQjkmmoDmFdlO8KcjeTmjEnvbXPIKXupxv?key=CuBAMnRiidRX8nJElFVlpA)

A expressão atribuída a `contagem_prefixos` : 

```                          
contagem_prefixos = stg_clientes.filter (
		col("Nome").rlike(f"^({'|'.join(prefixos)})")
).count()
```

Essa expressão verifica se o texto na coluna "Nome" começa com "sr.", "dr.", "sra." ou "dra.".

- `rlike` É uma função do pyspark que, quando aplicado a uma coluna, verifica se os valores da coluna correspondem ao padrão definido na expressão regular e retorna um DataFrame com as linhas que atendem a essa condição.

  `col("column_name").rlike("regex_pattern")` , onde `col("column_name")` especifica a coluna do DataFrame em que desejamos aplicar a expressão regular e  `("regex_pattern")` é a string que será usada pra buscar correspondências nos valores da coluna.

- Criamos uma lista com os prefixos buscados: **`prefixos = ["sr.", "dr.", "sra.", "dra."]`**:

- **`{'|'.join(prefixos)}`**: Cria uma única string a partir dos prefixos, separando-os com o caractere `|`, que é o operador "ou" nas expressões regulares.
- **`f"^({'|'.join(prefixos)})"`**:  O prefixo `^` indica que a expressão regular deve corresponder ao início do texto.

:pushpin: Saiba mais sobre a expressão regular `rlike` na documentação oficial [aqui](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.rlike.html) e aqui [Understanding Regex and the rlike Function](https://sparktpoint.com/spark-rlike-regex-matching/).



![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXcAf58jgQ2TS6J18TE67ff0O9i2OpCFkZZSWHdhgef5CK70mCteXlRgNz7OBX90kF8DMPyh62A092Ud4Bsmw1XuHle0CjkQD2pL5To3efp-bjDjn-W5rMiqBa2akvwT5Ufe6AOaXfgP_8KKW7gisMD__wWP?key=CuBAMnRiidRX8nJElFVlpA)



Agora invertemos a expressão , para retornar os valores sem o prefixos, e atribuímos a `clientes_sem_prefixos`. Para inverter a operação usamos **`~`**.

- `Clientes_email_normalizados`: Nessa expressão usamos o `split`que divide/fatia a coluna por espaços em branco e `[0]`  para recuperar o primeiro nome.

- `concat_ws` Essa concatenação `com separador` definimos `@` entre 'Nome' e `exemplo.net`;

- `lit` é usado pra concatenar o nome com o valor literal definido que é `exemplo.net`.



Resposta:

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXfrBgtZ38J9x5tfBTgCpGiMAXJQRBBFEAUdoB9K949wFM77AKtky--xnN8KYf4Ma9LoHtZfhs0aLCT1Y2uZpwRD5lHIzOlKPCCQMEo5honHJkcdvRXjP5IIML6JJTxkGPa6HcJUTVwt-B-IBQT06sX97YIL?key=CuBAMnRiidRX8nJElFVlpA)

**Salvamos as alterações **

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXc2HPy0WPe9HoTgyCC-4q9ZB8qX48DfKt9ourXrBMRoWexY-_am-TPrNmOWp-HZEflG2J7En7du_n-u1srvElCbiCsH2zELXaQx6S9OOJplXTkHEnoDQqUzYMpOZCxHRVE4zap6sHz_ZbYVUaw8j7sXnDc?key=CuBAMnRiidRX8nJElFVlpA)

<h3> GOLD : TERCEIRA CAMADA </h3>

-----------------------

Além de conter os dados prontos pra consumo, para Haelen e Davis (2024), os dados dessa camada devem:

- Conter as transformações específicas de negócios pre calculadas;

- Pode ter visualizações separadas dos dados para diferentes casos de uso;

- Estar em um formato fácil de navegar para os usuários de negócios;
- Alto desempenho;

Salvamos as alterações e os dados estarão prontos pra serem injetados em DataWarehouse.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXe_o_R4i_-5AKFdeHR8yuwg3K6c4mBDJFrO33rTsNdigLWB0FP9ECdE1MaypuA4_9dnH1lcMuxlr0zmAD5b1OIgnoDu_HrkP1sSEknHo-a8jEnChtzepBK75vGpd22pG4E2CV1yo6Dm_dqnglNPZYqMTYI?key=CuBAMnRiidRX8nJElFVlpA)

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXf_BdY6-G_WGzFLzxvkhO2dzZc3SgxOuv2vpEABhUHOnWo3SX6H_hLsRhed2ZGCNJI6Fl010zSgj-S2ZbrhPrIzfrRStMxKGfDNpvrjLl4nrIlNOovkjoNaSGfXkY7wjiPCAZI39I22reXckACaTwwwq5i0?key=CuBAMnRiidRX8nJElFVlpA)



<h3> HISTÓRICO DE VERSÕES </h3>

-----------------------------------------

De acordo com a [documentação oficial](https://docs.databricks.com/en/delta/history.html), cada operação que modifica uma tabela Delta Lake cria uma **nova versão da tabela**. Isso significa que podemos usar as informações do histórico para auditar operações, reverter uma tabela  para um estado anterior ou auditar mudanças nos dados e consultar uma tabela em um ponto específico no tempo usando a funcionalidade de *"[time travel](https://docs.databricks.com/en/delta/history.html#time-travel)"*.

**Quando foi feito, quem fez, quais operações** foram feitas são algumas das informações que o histórico de versões da tabela apresenta.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXe7ZwYRfgD44lP-ZCjXQhijRPcVSQ6-T1mTV9kH_MHTTQJDY7UN3OfC2pGRDAF8qEuhK3NWRsJq3-eBvABDDy6k7X_P5SZ-xEegyp-hswG549IoGQ1NI8nmgQsm_94MtuzyboNsEwkV7FhFvSdycObdZxuk?key=CuBAMnRiidRX8nJElFVlpA)



![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdJDmRtZNU5lLOQCJ359TPIGv8cp2Ty9TMdso-DqueuZntxWX3LtYX645x682PKiOas_LoquviwgegOaXBr7GFlacpPK75kxFv157bOG0PsuBjnBx1JGGtO3Z1Ybf9lotVRr89Ml3_5CHoE7ZCVtRruTnbe?key=CuBAMnRiidRX8nJElFVlpA)



<h3> TIME TRAVEL</h3>

-------------------------

- `versionAsOf` da API Delta: Baseado no histórico de versões podemos fazer um 'time travel' para outras versões, como por exemplo a versão 11 da tabela.

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdwekjql8NyP0kRUc54-AIluufGnMSwKvMbQ-2OJnAoYTVRT_lWdLf6CNPfZLVkGWZe3-Q_pr71nvMYml3FMyfzPMbCVIroSX99U1eO0AoLrxctGa64F3aOsAZQgLU4AEw3XKrBlUb7obSYpYqcangG7iJ9?key=CuBAMnRiidRX8nJElFVlpA)

Como dito, o *time travel* é útil pra explorar histórico, verificar mudanças que ocorreram em outras versões, restauração de um estado específico. Enfim, `versionAsOf` nos permite uma visão da tabela em diferentes momentos.

Existem ainda muitos outros métodos e funções para tratar dados com pyspark e Delta Lake API, assim como funções e métodos para aplicar transformações em DataFrames. Essas transformações ajudam a preparar os dados para análises mais avançadas ou para serem carregados em um Data Warehouse.



<h3> ENCONTRAR VERSÃO BASEADA NO TIMESTAMP </h3>

-----------------------------

**Exibir histórico de versões**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXfb1feitXnA0f2bnpwQE9deRjh6f9F9Kt0HSp4ZDy1MetjEMBE2cgiOCk9GGO68YAlnjGcylRhoUArQ_Bl_wZAQ4RpcdZ2BuCelybC34efqcppFgfN5UduNY7tE6yIo_AmVp_pzZxtBV9WySpqm4E4Ksvs?key=CuBAMnRiidRX8nJElFVlpA)

**Encontre a versão baseado no timestamp**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXeAl2Rpy5SdP5AQ1_h_NFmu-jNofInmWE-ku4BntXLL423JUA0st4rzNqcDekDF0KbyRPJFxpQbXRGfGO_zzBSx3cj44U3ukh04BNEGbKYwyAnawleVX3zeu8IqbGtlyhOo4Bntj_GZOR6aCLy1ssu93kmU?key=CuBAMnRiidRX8nJElFVlpA)



**Carregue a tabela que corresponde a versão identificada**

![img](https://lh7-rt.googleusercontent.com/docsz/AD_4nXd9moYUvqqpQSsX-HQipHWhWA_7Kdf6TH-3q-a3g6LA9tr0IG4jomQ5f0QsC5rzo6uzuTOvofQWcbcdl4o2AKSYNTNtqwlw43DUnFB4asm8hrkOHJewW1uT30EvWm5f7I3f58XyQuBA4xz0BA6cF4l8eE4?key=CuBAMnRiidRX8nJElFVlpA)

------------------------------

:pushpin: [Notebook dessa tarefa aqui](https://github.com/RegiMaria/SPARK_API_operacoes_basicas/blob/main/notebook/delta_clientes.ipynb).
