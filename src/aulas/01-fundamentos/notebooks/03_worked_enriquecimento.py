# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Camada Worked - Enriquecimento de Dados
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Aplicar regras de negócio e enriquecer os dados
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Características da Camada Worked
# MAGIC 
# MAGIC - ✅ JOINs entre tabelas relacionadas
# MAGIC - ✅ Cálculos de métricas derivadas
# MAGIC - ✅ Classificações de negócio
# MAGIC - ✅ Enriquecimento com dados externos
# MAGIC - ✅ Dados prontos para agregações

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configurações

# COMMAND ----------

# Importações
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# Configurações
CATALOGO = "dados_em_chamas"
SCHEMA_SILVER = f"{CATALOGO}.silver"
SCHEMA_WORKED = f"{CATALOGO}.worked"

spark.sql(f"USE CATALOG {CATALOGO}")

print(f"⚙️ Enriquecimento Worked")
print(f"📥 Fonte: {SCHEMA_SILVER}")
print(f"📤 Destino: {SCHEMA_WORKED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Leitura dos Dados Silver

# COMMAND ----------

# Lendo tabelas silver
df_vendas = spark.table(f"{SCHEMA_SILVER}.vendas")
df_produtos = spark.table(f"{SCHEMA_SILVER}.produtos")
df_clientes = spark.table(f"{SCHEMA_SILVER}.clientes")

print(f"📊 Registros lidos do Silver:")
print(f"  Vendas: {df_vendas.count()}")
print(f"  Produtos: {df_produtos.count()}")
print(f"  Clientes: {df_clientes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 JOINs: Vendas Enriquecidas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1️⃣ JOIN Vendas + Produtos

# COMMAND ----------

# Enriquecendo vendas com informações de produtos
df_vendas_produtos = df_vendas.alias("v") \
    .join(
        df_produtos.alias("p"),
        F.col("v.id_produto") == F.col("p.id_produto"),
        "left"
    ) \
    .select(
        # Dados da venda
        F.col("v.id_venda"),
        F.col("v.data_venda"),
        F.col("v.id_cliente"),
        F.col("v.id_produto"),
        F.col("v.quantidade"),
        F.col("v.preco_unitario"),
        F.col("v.desconto_aplicado"),
        # Dados do produto
        F.col("p.nome_produto"),
        F.col("p.categoria"),
        F.col("p.custo").alias("custo_unitario")
    )

print("✅ JOIN Vendas + Produtos realizado")
display(df_vendas_produtos.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2️⃣ JOIN Vendas + Clientes

# COMMAND ----------

# Enriquecendo com informações de clientes
df_vendas_completas = df_vendas_produtos.alias("vp") \
    .join(
        df_clientes.alias("c"),
        F.col("vp.id_cliente") == F.col("c.id_cliente"),
        "left"
    ) \
    .select(
        # Tudo de vendas + produtos
        F.col("vp.*"),
        # Dados do cliente
        F.col("c.nome").alias("nome_cliente"),
        F.col("c.cidade"),
        F.col("c.estado"),
        F.col("c.data_cadastro").alias("cliente_desde")
    )

print("✅ JOIN Vendas + Clientes realizado")
display(df_vendas_completas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📐 Cálculos de Métricas Derivadas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Valores Calculados

# COMMAND ----------

# Calculando métricas financeiras
df_vendas_calculadas = df_vendas_completas \
    .withColumn(
        # Valor bruto: quantidade * preço unitário
        "valor_bruto",
        F.round(F.col("quantidade") * F.col("preco_unitario"), 2)
    ) \
    .withColumn(
        # Valor do desconto
        "valor_desconto",
        F.round(F.col("quantidade") * F.col("preco_unitario") * F.col("desconto_aplicado"), 2)
    ) \
    .withColumn(
        # Valor líquido: após desconto
        "valor_liquido",
        F.round(F.col("quantidade") * F.col("preco_unitario") * (1 - F.col("desconto_aplicado")), 2)
    ) \
    .withColumn(
        # Custo total
        "custo_total",
        F.round(F.col("quantidade") * F.coalesce(F.col("custo_unitario"), F.lit(0)), 2)
    ) \
    .withColumn(
        # Margem bruta
        "margem_bruta",
        F.round(
            F.col("quantidade") * F.col("preco_unitario") * (1 - F.col("desconto_aplicado")) 
            - F.col("quantidade") * F.coalesce(F.col("custo_unitario"), F.lit(0)), 2
        )
    ) \
    .withColumn(
        # Margem percentual
        "margem_percentual",
        F.round(
            F.when(F.col("valor_liquido") > 0,
                (F.col("margem_bruta") / F.col("valor_liquido")) * 100
            ).otherwise(0), 2
        )
    )

print("✅ Métricas financeiras calculadas")
display(df_vendas_calculadas.select(
    "id_venda", "nome_produto", "quantidade", "preco_unitario", 
    "desconto_aplicado", "valor_bruto", "valor_desconto", 
    "valor_liquido", "custo_total", "margem_bruta", "margem_percentual"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4️⃣ Classificações de Negócio

# COMMAND ----------

# Classificação de ticket médio da venda
df_vendas_classificadas = df_vendas_calculadas \
    .withColumn(
        "faixa_valor",
        F.when(F.col("valor_liquido") < 100, "Baixo")
         .when(F.col("valor_liquido") < 500, "Médio")
         .when(F.col("valor_liquido") < 1000, "Alto")
         .otherwise("Premium")
    ) \
    .withColumn(
        # Classificação de margem
        "faixa_margem",
        F.when(F.col("margem_percentual") < 20, "Baixa")
         .when(F.col("margem_percentual") < 40, "Média")
         .when(F.col("margem_percentual") < 60, "Alta")
         .otherwise("Muito Alta")
    )

print("✅ Classificações aplicadas")
display(df_vendas_classificadas.groupBy("faixa_valor").count().orderBy("faixa_valor"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5️⃣ Dimensões Temporais

# COMMAND ----------

# Adicionando dimensões de tempo para análises
df_vendas_temporal = df_vendas_classificadas \
    .withColumn("ano", F.year(F.col("data_venda"))) \
    .withColumn("mes", F.month(F.col("data_venda"))) \
    .withColumn("dia", F.dayofmonth(F.col("data_venda"))) \
    .withColumn("dia_semana", F.dayofweek(F.col("data_venda"))) \
    .withColumn("nome_dia_semana", F.date_format(F.col("data_venda"), "EEEE")) \
    .withColumn("semana_ano", F.weekofyear(F.col("data_venda"))) \
    .withColumn("trimestre", F.quarter(F.col("data_venda"))) \
    .withColumn(
        "periodo_dia",
        F.lit("Dia Inteiro")  # Placeholder - em dados reais teria hora
    ) \
    .withColumn(
        "is_fim_semana",
        F.when(F.col("dia_semana").isin(1, 7), True).otherwise(False)
    )

print("✅ Dimensões temporais adicionadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6️⃣ Ranking de Clientes

# COMMAND ----------

# Calculando métricas por cliente para classificação
window_cliente = Window.partitionBy("id_cliente")

df_vendas_final = df_vendas_temporal \
    .withColumn(
        "total_compras_cliente",
        F.sum("valor_liquido").over(window_cliente)
    ) \
    .withColumn(
        "qtd_pedidos_cliente",
        F.count("id_venda").over(window_cliente)
    ) \
    .withColumn(
        # Classificação RFM simplificada
        "classificacao_cliente",
        F.when(F.col("total_compras_cliente") >= 5000, "VIP")
         .when(F.col("total_compras_cliente") >= 2000, "Gold")
         .when(F.col("total_compras_cliente") >= 500, "Silver")
         .otherwise("Bronze")
    )

print("✅ Ranking de clientes calculado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Salvando na Camada Worked

# COMMAND ----------

# Adicionando metadados de processamento
df_worked_final = df_vendas_final \
    .withColumn("_processado_em", F.current_timestamp()) \
    .withColumn("_versao_regras", F.lit("1.0"))

# Schema final
print("📋 Schema da tabela Worked:")
df_worked_final.printSchema()

# COMMAND ----------

# Salvando tabela de vendas enriquecidas
df_worked_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ano", "mes") \
    .saveAsTable(f"{SCHEMA_WORKED}.vendas_enriquecidas")

print(f"✅ Tabela {SCHEMA_WORKED}.vendas_enriquecidas criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Análise Exploratória dos Dados Worked

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visão geral das vendas enriquecidas
# MAGIC SELECT 
# MAGIC     categoria,
# MAGIC     COUNT(*) as qtd_vendas,
# MAGIC     ROUND(SUM(valor_liquido), 2) as receita_total,
# MAGIC     ROUND(AVG(valor_liquido), 2) as ticket_medio,
# MAGIC     ROUND(SUM(margem_bruta), 2) as margem_total,
# MAGIC     ROUND(AVG(margem_percentual), 2) as margem_media_pct
# MAGIC FROM worked.vendas_enriquecidas
# MAGIC GROUP BY categoria
# MAGIC ORDER BY receita_total DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por classificação de cliente
# MAGIC SELECT 
# MAGIC     classificacao_cliente,
# MAGIC     COUNT(DISTINCT id_cliente) as qtd_clientes,
# MAGIC     COUNT(*) as qtd_pedidos,
# MAGIC     ROUND(SUM(valor_liquido), 2) as receita_total
# MAGIC FROM worked.vendas_enriquecidas
# MAGIC GROUP BY classificacao_cliente
# MAGIC ORDER BY receita_total DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vendas por dia da semana
# MAGIC SELECT 
# MAGIC     nome_dia_semana,
# MAGIC     dia_semana,
# MAGIC     COUNT(*) as qtd_vendas,
# MAGIC     ROUND(SUM(valor_liquido), 2) as receita
# MAGIC FROM worked.vendas_enriquecidas
# MAGIC GROUP BY nome_dia_semana, dia_semana
# MAGIC ORDER BY dia_semana

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo da Camada Worked
# MAGIC 
# MAGIC | Campo Enriquecido | Descrição |
# MAGIC |-------------------|-----------|
# MAGIC | `valor_bruto` | Quantidade × Preço Unitário |
# MAGIC | `valor_desconto` | Valor do desconto aplicado |
# MAGIC | `valor_liquido` | Valor após desconto |
# MAGIC | `custo_total` | Custo total dos produtos |
# MAGIC | `margem_bruta` | Lucro bruto da venda |
# MAGIC | `margem_percentual` | % de margem sobre a venda |
# MAGIC | `faixa_valor` | Classificação por valor (Baixo/Médio/Alto/Premium) |
# MAGIC | `faixa_margem` | Classificação por margem |
# MAGIC | `classificacao_cliente` | Segmentação do cliente (VIP/Gold/Silver/Bronze) |
# MAGIC | Dimensões temporais | Ano, mês, dia, semana, etc. |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ▶️ **Próximo passo**: Execute o notebook `04_gold_agregacoes` para criar tabelas agregadas
