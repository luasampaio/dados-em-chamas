

* 📘 Aula
* 💼 Portfólio
* 🧪 Laboratório prático

Tudo simples, mas tecnicamente correto.

---

# 🎓 Projeto Didático — Arquitetura Medallion no Databricks

## 📌 Nome do Projeto

**dados_em_chamas – Pipeline Medallion com Unity Catalog**

---

# 1️⃣ Objetivo do Projeto

Demonstrar:

* Separação de camadas (Bronze, Silver, Gold)
* Uso de Volume para arquivos brutos
* Uso de Delta Lake como padrão
* Evolução da qualidade do dado
* Governança básica com Unity Catalog

---

# 2️⃣ Estrutura Final Esperada

```
dados_em_chamas
│
├── bronze
│   ├── landing (volume)
│   ├── clientes (delta)
│   ├── produtos (delta)
│   └── vendas (delta)
│
├── silver
│   ├── clientes
│   ├── produtos
│   └── vendas
│
└── gold
    └── vendas_por_categoria
```

---

# 3️⃣ Dataset Simples para Aula

## 📄 clientes.csv

```
id,nome,email,cidade
1,Ana Silva,ana@email.com,São Paulo
2,Bruno Lima,bruno@email.com,Rio de Janeiro
3,,carla@email.com,Belo Horizonte
```

## 📄 produtos.csv

```
id,nome,categoria,preco
1,Notebook,Eletrônicos,3500
2,Cadeira,Moveis,800
3,Mouse,Eletrônicos,150
```

## 📄 vendas.csv

```
id,cliente_id,produto_id,quantidade
1,1,1,1
2,2,3,2
3,3,2,1
```

Salvar em:

```
/Volumes/dados_em_chamas/bronze/landing/
```

---

# 4️⃣ Notebook 1 — Bronze (Ingestão)

## 🎯 Objetivo

Converter CSV bruto em tabela Delta.

```python
path = "/Volumes/dados_em_chamas/bronze/landing/clientes.csv"

df = spark.read.option("header", True).csv(path)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dados_em_chamas.bronze.clientes")
```

📌 Conceito ensinado:

* Volume ≠ Tabela
* Delta como padrão
* Camada Bronze mantém dado quase bruto

---

# 5️⃣ Notebook 2 — Silver (Tratamento)

## 🎯 Objetivo

Melhorar qualidade.

```python
from pyspark.sql.functions import col, upper

df = spark.table("dados_em_chamas.bronze.clientes")

df_silver = (
    df
    .dropna(subset=["id", "nome"])
    .withColumn("nome", upper(col("nome")))
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dados_em_chamas.silver.clientes")
```

📌 Conceitos ensinados:

* Data cleansing
* Padronização
* Remoção de dados inválidos

---

# 6️⃣ Notebook 3 — Gold (Agregação)

```python
from pyspark.sql.functions import sum

df_vendas = spark.table("dados_em_chamas.silver.vendas")
df_produtos = spark.table("dados_em_chamas.silver.produtos")

df_join = df_vendas.join(
    df_produtos,
    df_vendas.produto_id == df_produtos.id
)

df_gold = df_join.groupBy("categoria").agg(
    sum("quantidade").alias("total_vendido")
)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dados_em_chamas.gold.vendas_por_categoria")
```

📌 Conceitos ensinados:

* Join
* Agregação
* Modelagem analítica

---

# 7️⃣ Conceitos que Você Demonstra no Portfólio

✔ Arquitetura Medallion
✔ Organização de catálogo
✔ Governança básica
✔ Separação de responsabilidade por camada
✔ Uso correto de Delta Lake

Isso já é suficiente para um projeto júnior/pleno.

---

# 8️⃣ Como Explicar em Entrevista

Você pode dizer:

> "Implementei uma arquitetura Medallion utilizando Unity Catalog.
> Separei ingestão física via Volume da modelagem lógica em Delta Tables.
> Estruturei pipeline Bronze, Silver e Gold demonstrando evolução da qualidade do dado."

Isso já mostra maturidade técnica.

---

# 🔎 Se quiser evoluir um nível acima

Podemos adicionar:

* Validação de anonimização na Silver
* Controle de schema
* Data Quality check
* Organização por notebooks modularizados
* Simulação de erro e quarentena

---
