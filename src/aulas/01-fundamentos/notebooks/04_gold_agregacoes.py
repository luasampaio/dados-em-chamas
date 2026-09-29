# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Camada Gold - Agregações e Métricas
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Criar tabelas agregadas prontas para consumo por dashboards e analistas
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Características da Camada Gold
# MAGIC 
# MAGIC - ✅ Agregações pré-calculadas
# MAGIC - ✅ Métricas de negócio consolidadas
# MAGIC - ✅ Otimizado para consultas rápidas
# MAGIC - ✅ Pronto para BI e reporting
# MAGIC - ✅ Granularidades específicas por caso de uso

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
SCHEMA_WORKED = f"{CATALOGO}.worked"
SCHEMA_GOLD = f"{CATALOGO}.gold"

spark.sql(f"USE CATALOG {CATALOGO}")

print(f"🥇 Agregações Gold")
print(f"📥 Fonte: {SCHEMA_WORKED}")
print(f"📤 Destino: {SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Leitura dos Dados Worked

# COMMAND ----------

# Lendo tabela worked
df_vendas = spark.table(f"{SCHEMA_WORKED}.vendas_enriquecidas")

print(f"📊 Registros lidos do Worked: {df_vendas.count()}")
print(f"📅 Período: {df_vendas.agg(F.min('data_venda'), F.max('data_venda')).collect()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tabela Gold 1: Vendas Diárias

# COMMAND ----------

# Agregação diária de vendas
df_vendas_diarias = df_vendas.groupBy(
    "data_venda",
    "ano",
    "mes",
    "dia",
    "dia_semana",
    "nome_dia_semana",
    "is_fim_semana"
).agg(
    # Contadores
    F.countDistinct("id_venda").alias("qtd_pedidos"),
    F.countDistinct("id_cliente").alias("qtd_clientes_unicos"),
    F.sum("quantidade").alias("qtd_itens_vendidos"),
    
    # Valores financeiros
    F.round(F.sum("valor_bruto"), 2).alias("receita_bruta"),
    F.round(F.sum("valor_desconto"), 2).alias("total_descontos"),
    F.round(F.sum("valor_liquido"), 2).alias("receita_liquida"),
    F.round(F.sum("custo_total"), 2).alias("custo_total"),
    F.round(F.sum("margem_bruta"), 2).alias("margem_bruta"),
    
    # Médias
    F.round(F.avg("valor_liquido"), 2).alias("ticket_medio"),
    F.round(F.avg("margem_percentual"), 2).alias("margem_media_pct"),
    
    # Desconto médio
    F.round(F.avg("desconto_aplicado") * 100, 2).alias("desconto_medio_pct")
).withColumn(
    # KPI: Margem sobre receita
    "roi_pct",
    F.round((F.col("margem_bruta") / F.col("receita_liquida")) * 100, 2)
).orderBy("data_venda")

print("✅ Vendas diárias agregadas")
display(df_vendas_diarias)

# COMMAND ----------

# Salvando tabela gold de vendas diárias
df_vendas_diarias.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.vendas_diarias")

print(f"✅ Tabela {SCHEMA_GOLD}.vendas_diarias criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tabela Gold 2: Vendas por Categoria

# COMMAND ----------

# Agregação por categoria
df_vendas_categoria = df_vendas.groupBy(
    "categoria"
).agg(
    # Contadores
    F.countDistinct("id_venda").alias("qtd_pedidos"),
    F.countDistinct("id_produto").alias("qtd_produtos_vendidos"),
    F.countDistinct("id_cliente").alias("qtd_clientes"),
    F.sum("quantidade").alias("qtd_itens_vendidos"),
    
    # Valores financeiros
    F.round(F.sum("valor_liquido"), 2).alias("receita_liquida"),
    F.round(F.sum("margem_bruta"), 2).alias("margem_bruta"),
    
    # Médias
    F.round(F.avg("valor_liquido"), 2).alias("ticket_medio"),
    F.round(F.avg("margem_percentual"), 2).alias("margem_media_pct")
).withColumn(
    # Ranking por receita
    "ranking_receita",
    F.row_number().over(Window.orderBy(F.col("receita_liquida").desc()))
).withColumn(
    # % do total (será calculado após)
    "pct_receita_total",
    F.round(
        F.col("receita_liquida") / F.sum("receita_liquida").over(Window.partitionBy()) * 100, 
        2
    )
).orderBy(F.col("receita_liquida").desc())

print("✅ Vendas por categoria agregadas")
display(df_vendas_categoria)

# COMMAND ----------

# Salvando tabela gold de vendas por categoria
df_vendas_categoria.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.vendas_por_categoria")

print(f"✅ Tabela {SCHEMA_GOLD}.vendas_por_categoria criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tabela Gold 3: Ranking de Clientes

# COMMAND ----------

# Ranking de clientes por valor gasto
df_ranking_clientes = df_vendas.groupBy(
    "id_cliente",
    "nome_cliente",
    "cidade",
    "estado",
    "cliente_desde",
    "classificacao_cliente"
).agg(
    # Contadores
    F.countDistinct("id_venda").alias("qtd_pedidos"),
    F.sum("quantidade").alias("qtd_itens_comprados"),
    F.countDistinct("categoria").alias("qtd_categorias_diferentes"),
    
    # Valores financeiros
    F.round(F.sum("valor_liquido"), 2).alias("valor_total_compras"),
    F.round(F.sum("margem_bruta"), 2).alias("margem_gerada"),
    
    # Médias
    F.round(F.avg("valor_liquido"), 2).alias("ticket_medio"),
    
    # Datas
    F.min("data_venda").alias("primeira_compra"),
    F.max("data_venda").alias("ultima_compra")
).withColumn(
    # Dias desde última compra (recência)
    "dias_desde_ultima_compra",
    F.datediff(F.current_date(), F.col("ultima_compra"))
).withColumn(
    # Ranking por valor
    "ranking_valor",
    F.row_number().over(Window.orderBy(F.col("valor_total_compras").desc()))
).withColumn(
    # LTV estimado (simplificado)
    "ltv_estimado",
    F.round(F.col("valor_total_compras") * 12 / 
            F.greatest(
                F.months_between(F.current_date(), F.col("primeira_compra")),
                F.lit(1)
            ), 2)
).orderBy(F.col("valor_total_compras").desc())

print("✅ Ranking de clientes calculado")
display(df_ranking_clientes.limit(10))

# COMMAND ----------

# Salvando tabela gold de ranking de clientes
df_ranking_clientes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.ranking_clientes")

print(f"✅ Tabela {SCHEMA_GOLD}.ranking_clientes criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tabela Gold 4: Vendas por Estado

# COMMAND ----------

# Agregação geográfica
df_vendas_estado = df_vendas.groupBy(
    "estado"
).agg(
    # Contadores
    F.countDistinct("id_venda").alias("qtd_pedidos"),
    F.countDistinct("id_cliente").alias("qtd_clientes"),
    F.countDistinct("cidade").alias("qtd_cidades"),
    
    # Valores financeiros
    F.round(F.sum("valor_liquido"), 2).alias("receita_liquida"),
    F.round(F.sum("margem_bruta"), 2).alias("margem_bruta"),
    
    # Médias
    F.round(F.avg("valor_liquido"), 2).alias("ticket_medio")
).withColumn(
    "pct_receita_total",
    F.round(
        F.col("receita_liquida") / F.sum("receita_liquida").over(Window.partitionBy()) * 100, 
        2
    )
).orderBy(F.col("receita_liquida").desc())

print("✅ Vendas por estado agregadas")
display(df_vendas_estado)

# COMMAND ----------

# Salvando tabela gold de vendas por estado
df_vendas_estado.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.vendas_por_estado")

print(f"✅ Tabela {SCHEMA_GOLD}.vendas_por_estado criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tabela Gold 5: Resumo Executivo

# COMMAND ----------

# KPIs executivos consolidados
df_resumo_executivo = df_vendas.agg(
    # Período
    F.min("data_venda").alias("data_inicio"),
    F.max("data_venda").alias("data_fim"),
    F.countDistinct("data_venda").alias("dias_com_vendas"),
    
    # Volume
    F.countDistinct("id_venda").alias("total_pedidos"),
    F.countDistinct("id_cliente").alias("total_clientes"),
    F.countDistinct("id_produto").alias("total_produtos_vendidos"),
    F.sum("quantidade").alias("total_itens_vendidos"),
    
    # Financeiro
    F.round(F.sum("valor_bruto"), 2).alias("receita_bruta_total"),
    F.round(F.sum("valor_desconto"), 2).alias("descontos_total"),
    F.round(F.sum("valor_liquido"), 2).alias("receita_liquida_total"),
    F.round(F.sum("custo_total"), 2).alias("custo_total"),
    F.round(F.sum("margem_bruta"), 2).alias("margem_bruta_total"),
    
    # Médias
    F.round(F.avg("valor_liquido"), 2).alias("ticket_medio"),
    F.round(F.avg("margem_percentual"), 2).alias("margem_media_pct"),
    F.round(F.avg("desconto_aplicado") * 100, 2).alias("desconto_medio_pct")
).withColumn(
    # ROI geral
    "roi_geral_pct",
    F.round((F.col("margem_bruta_total") / F.col("receita_liquida_total")) * 100, 2)
).withColumn(
    # Receita média por dia
    "receita_media_diaria",
    F.round(F.col("receita_liquida_total") / F.col("dias_com_vendas"), 2)
).withColumn(
    # Timestamp de atualização
    "_atualizado_em",
    F.current_timestamp()
)

print("✅ Resumo executivo calculado")
display(df_resumo_executivo)

# COMMAND ----------

# Salvando tabela gold de resumo executivo
df_resumo_executivo.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.resumo_executivo")

print(f"✅ Tabela {SCHEMA_GOLD}.resumo_executivo criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Verificação das Tabelas Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listando todas as tabelas gold criadas
# MAGIC SHOW TABLES IN gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualizando o resumo executivo
# MAGIC SELECT * FROM gold.resumo_executivo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Dashboards de Exemplo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📈 Evolução de Vendas Diárias
# MAGIC SELECT 
# MAGIC     data_venda,
# MAGIC     receita_liquida,
# MAGIC     ticket_medio,
# MAGIC     qtd_pedidos
# MAGIC FROM gold.vendas_diarias
# MAGIC ORDER BY data_venda

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 🏆 Top 5 Clientes por Valor
# MAGIC SELECT 
# MAGIC     ranking_valor,
# MAGIC     nome_cliente,
# MAGIC     cidade,
# MAGIC     estado,
# MAGIC     classificacao_cliente,
# MAGIC     valor_total_compras,
# MAGIC     qtd_pedidos,
# MAGIC     ticket_medio
# MAGIC FROM gold.ranking_clientes
# MAGIC WHERE ranking_valor <= 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📦 Performance por Categoria
# MAGIC SELECT 
# MAGIC     categoria,
# MAGIC     receita_liquida,
# MAGIC     pct_receita_total,
# MAGIC     margem_media_pct,
# MAGIC     qtd_pedidos
# MAGIC FROM gold.vendas_por_categoria
# MAGIC ORDER BY receita_liquida DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo da Camada Gold
# MAGIC 
# MAGIC | Tabela | Descrição | Granularidade |
# MAGIC |--------|-----------|---------------|
# MAGIC | `gold.vendas_diarias` | Métricas diárias de vendas | 1 linha/dia |
# MAGIC | `gold.vendas_por_categoria` | Performance por categoria de produto | 1 linha/categoria |
# MAGIC | `gold.ranking_clientes` | Ranking e métricas por cliente | 1 linha/cliente |
# MAGIC | `gold.vendas_por_estado` | Distribuição geográfica | 1 linha/estado |
# MAGIC | `gold.resumo_executivo` | KPIs consolidados do negócio | 1 linha total |
# MAGIC 
# MAGIC ### 📊 Casos de Uso
# MAGIC 
# MAGIC - **Dashboards de BI**: Power BI, Tableau, Metabase
# MAGIC - **Relatórios executivos**: Métricas prontas para consumo
# MAGIC - **Análises ad-hoc**: SQL simples para analistas
# MAGIC - **Machine Learning**: Features pré-calculadas
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ▶️ **Próximo passo**: Execute o notebook `05_pipeline_completo` para ver a orquestração end-to-end
