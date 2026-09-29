
###ro
### 1. *Introdução à Engenharia de Dados
- **Objetivo:** Entender o papel do engenheiro de dados e o ambiente de trabalho
  - **Conteúdo:**
    - Visão geral do fluxo de dados no Mercadoe breve introdução à Engenharia de dados e Datamesh.
    - Engenharia de Software X Engenharia de Dados: diferenças e como uma complementa a outra.
    - Ferramentas e tecnologias utilizadas no Mercadopara Engenharia de Dados.
    - Responsabilidades de um engenheiro de dados .
  - **Atividade:** Discussão sobre como os dados são usados no banco para tomada de decisão e conformidade.

---

### 2. *SQL e Manipulação de Dados*
- **Objetivo:** Fortalecer habilidades em SQL para manipulação e extração de dados.
  - **Conteúdo:**
    - Consultas SQL básicas e avançadas.
    - Operações de transformação e limpeza de dados com SQL.
    - Introdução ao SQLITE3, DuckDB e como ele pode ser utilizado em pipelines de dados.
    - Integração do SQL com Python, DuckDB e PySpark.
    - Introdução ao Athena como ferramenta principal de consulta no Datamesh.
  - **Atividade:** Exercícios práticos de SQL com amostras de dados.

---

### 3. *Python para Engenharia de Dados*
- **Objetivo:** Ensinar Python aplicado ao desenvolvimento de pipelines de dados.
  - **Conteúdo:**
    - Introdução a DataFrames com bibliotecas como Pandas, Polars, Dask e integração com Spark.
    - Boas práticas de programação e modularização de código em Python para ETL.
    - Testes unitários: como garantir a qualidade do código mesmo em pipelines de dados.
  - **Atividade:** Criação de scripts Python para manipulação básica de dados e testes unitários.

---

### 4. *DataFrames e Manipulação de Dados em Spark*
- **Objetivo:** Dominar DataFrames para processamento em larga escala.
  - **Conteúdo:**
    - Introdução ao Spark e seu funcionamento.
    - Introdução a DataFrames: conceitos e operações básicas.
    - Comparação entre DataFrames em Pandas e PySpark.
    - Manipulação de grandes volumes de dados com PySpark.
  - **Atividade:** Exercícios de manipulação de dados com DataFrames em PySpark.

---

### 5. *Processos de ETL usando AWS Glue, Lambda e Step Functions*
- **Objetivo:** Ensinar o uso do AWS Glue e Step Functions para ETL.
  - **Conteúdo:**
    - Arquitetura do AWS Glue e principais componentes.
    - Desenvolvimento de pipelines ETL com AWS Glue e PySpark.
    - Introdução às Step Functions para orquestração de workflows complexos.
  - **Atividade:** Criação de um pipeline ETL completo com Glue, integrado a uma Step Function para monitoramento do fluxo.

---

### 6. *Arquitetura de Dados no Banco: DataMesh e AWS*
- **Objetivo:** Compreender a arquitetura de dados baseada em DataMesh e AWS.
  - **Conteúdo:**
    - Conceitos de DataMesh: governança e descentralização de dados.
    - Data Lake e Data Warehouse no AWS: S3, Glue Catalog e Athena.
    - Particionamento, versionamento e gerenciamento de dados no AWS.
  - **Atividade:** Exercício prático de modelagem de dados para um cenário do Itaú.

---

### 7. *Terraform e Automação de Infraestrutura no AWS*
- **Objetivo:** Capacitar na criação e automação de infraestrutura na AWS com Terraform.
  - **Conteúdo:**
    - Introdução ao Terraform: conceitos básicos e estrutura de código.
    - Provisionamento de infraestrutura AWS: S3, Glue e Step Functions.
    - Práticas de versionamento e reutilização de módulos no Terraform.
    - Como criar ambientes replicáveis para desenvolvimento e produção.
  - **Atividade:** Configuração de buckets S3 e permissões usando Terraform.

---

### 8. *Qualidade de Dados e LGPD*
- **Objetivo:** Aplicar práticas de qualidade e segurança de dados.
  - **Conteúdo:**
    - Ferramentas de qualidade de dados (AWS Glue Data Quality).
    - Validação e conformidade com LGPD em pipelines de dados.
  - **Atividade:** Configuração de rotinas de validação de qualidade e conformidade em um pipeline.

---

### 9. *Projeto Final e Integração de Conceitos*
- **Objetivo:** Aplicar todos os conceitos em um projeto realista.
  - **Conteúdo:**
    - Desenvolvimento de um pipeline end-to-end que integra todas as ferramentas e práticas aprendidas.
    - Uso de Terraform para provisionar infraestrutura do pipeline (S3, Glue e Step Functions).
    - Implementação de uma Step Function para orquestrar a execução de ETL e validação de dados.
    - Documentação, monitoramento e ajuste do pipeline.
  - **Atividade:** Projeto de dados integrando Python, PySpark, Glue, Terraform e monitoramento com Glue Data Quality.

---
