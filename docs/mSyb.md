A seguir está um **exemplo prático e recomendado** de como **encapsular código no Databricks usando notebooks**, no padrão utilizado em projetos profissionais de Engenharia de Dados (especialmente compatível com **Unity Catalog, Repos e Jobs**).

O objetivo é:

* Centralizar lógica reutilizável
* Evitar código duplicado
* Facilitar versionamento, testes e automação

---

## Arquitetura proposta (por notebooks)

```text
Repos/
└── data-platform/
    ├── libs/
    │   ├── config/
    │   │   └── env_config.py
    │   ├── ingestion/
    │   │   └── reader.py
    │   ├── validation/
    │   │   └── schema.py
    │   └── utils/
    │       └── logging.py
    └── pipelines/
        └── bronze/
            └── ingest_bronze_sales.py
```

---

## Opção 1 (RECOMENDADA): Encapsular como **módulos Python** importáveis

Esse é o padrão mais limpo e profissional.

### 1. Notebook de função reutilizável

**`libs/ingestion/reader.py`**

```python
from pyspark.sql import DataFrame

def read_jdbc(
    spark,
    jdbc_url: str,
    table: str,
    user: str,
    password: str,
    predicates: list | None = None
) -> DataFrame:
    """
    Leitura genérica JDBC com suporte a carga incremental.
    """
    reader = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
    )

    if predicates:
        reader = reader.option("predicates", predicates)

    return reader.load()
```

---

### 2. Notebook pipeline (consumidor)

**`pipelines/bronze/ingest_bronze_sales.py`**

```python
from libs.ingestion.reader import read_jdbc

jdbc_url = dbutils.secrets.get("kv", "jdbc-url")
user = dbutils.secrets.get("kv", "jdbc-user")
password = dbutils.secrets.get("kv", "jdbc-password")

df = read_jdbc(
    spark=spark,
    jdbc_url=jdbc_url,
    table="dbo.sales",
    user=user,
    password=password
)

(
    df.write
      .format("delta")
      .mode("append")
      .saveAsTable("bronze.sales")
)
```

### Por que este modelo é o melhor

* Compatível com **Repos**
* Funciona com **Databricks Jobs**
* Fácil de empacotar depois em `.whl`
* Compatível com **Unity Catalog**

---

## Opção 2: Encapsular usando `%run` (válido, mas menos escalável)

Útil para times iniciantes ou protótipos.

---

### 1. Notebook utilitário

**`libs/ingestion/read_jdbc_notebook`**

```python
def read_jdbc(spark, table):
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()
    )
```

---

### 2. Notebook consumidor

```python
# Carrega funções e variáveis
%run ../../libs/ingestion/read_jdbc_notebook

df = read_jdbc(spark, "dbo.sales")
```

### Limitações do `%run`

* Variáveis globais
* Difícil versionar e testar
* Não recomendado para grandes plataformas

---

## Padrão avançado (alinhado ao que você já vem fazendo)

Você pode evoluir para algo como:

```python
def ingest(
    source_config: dict,
    target_table: str,
    layer: str
):
    df = read_source(source_config)
    df_valid = validate_schema(df, layer)
    write_delta(df_valid, target_table)
```

Chamado por Jobs via parâmetros JSON.

---

## Recomendação final (best practice Databricks)

| Situação               | Abordagem                    |
| ---------------------- | ---------------------------- |
| Projeto corporativo    | **Módulos Python (Opção 1)** |
| Protótipo rápido       | `%run`                       |
| Framework reutilizável | `.whl` + Repos               |
| Unity Catalog          | Imports Python               |

---

Se quiser, no próximo passo posso:

* Transformar isso em **framework de ingestão completo**
* Mostrar **como empacotar em `.whl` direto no Databricks**
* Adaptar o exemplo para **Bronze / Silver / Gold**
* Integrar com **Jobs JSON + CI/CD (Azure DevOps)**
