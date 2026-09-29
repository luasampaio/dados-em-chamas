# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Camada Bronze - Ingestão de Dados Brutos
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Ingerir dados brutos sem nenhuma transformação
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Características da Camada Bronze
# MAGIC 
# MAGIC - ✅ Dados exatamente como chegaram da fonte
# MAGIC - ✅ Metadados de ingestão (timestamp, arquivo fonte)
# MAGIC - ✅ Sem validações ou transformações
# MAGIC - ✅ Histórico completo (append-only)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configurações

# COMMAND ----------

# Importações
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# Configurações - Unity Catalog
CATALOGO = "dados_em_chamas"
SCHEMA_BRONZE = f"{CATALOGO}.bronze"

# Paths - Unity Catalog Volumes
VOLUME_BRONZE = f"/Volumes/{CATALOGO}/bronze/bronze"  # Volume para arquivos CSV
PATH_CHECKPOINT = f"/Volumes/{CATALOGO}/bronze/_checkpoints"

# Usando catálogo
spark.sql(f"USE CATALOG {CATALOGO}")

print(f"🥉 Ingestão Bronze")
print(f"📁 Volume Bronze: {VOLUME_BRONZE}")
print(f"📁 Checkpoint: {PATH_CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Upload dos Arquivos de Exemplo
# MAGIC 
# MAGIC > ⚠️ **Nota**: Se você está rodando localmente, faça upload dos CSVs para o FileStore
# MAGIC > ou ajuste o PATH_LANDING para o local correto dos arquivos.

# COMMAND ----------

# Lendo CSVs diretamente do Unity Catalog Volume
# Em produção, você usaria Auto Loader com streaming

# Verificando arquivos disponíveis no Volume
print("📂 Arquivos disponíveis no Volume Bronze:")
display(dbutils.fs.ls(VOLUME_BRONZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Vendas

# COMMAND ----------

# Schema de entrada para vendas (todos como string para preservar dados brutos)
schema_vendas_raw = StructType([
    StructField("id_venda", StringType(), True),
    StructField("data_venda", StringType(), True),
    StructField("id_cliente", StringType(), True),
    StructField("id_produto", StringType(), True),
    StructField("quantidade", StringType(), True),
    StructField("preco_unitario", StringType(), True),
    StructField("desconto_aplicado", StringType(), True)
])

# COMMAND ----------

# Função para adicionar metadados de ingestão
def adicionar_metadados_ingestao(df, nome_arquivo: str):
    """
    Adiciona colunas de metadados para rastreabilidade.
    
    Colunas adicionadas:
    - data_hora_ingestao: Momento da ingestão
    - arquivo_fonte: Nome do arquivo de origem
    - data_ingestao: Data da ingestão (para particionamento)
    """
    return df.withColumn("data_hora_ingestao", F.current_timestamp()) \
             .withColumn("arquivo_fonte", F.lit(nome_arquivo)) \
             .withColumn("data_ingestao", F.current_date())

# COMMAND ----------

# Lendo vendas do Unity Catalog Volume
df_vendas_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(schema_vendas_raw) \
    .load(f"{VOLUME_BRONZE}/vendas.csv")

print(f"✅ Arquivo vendas.csv carregado: {df_vendas_raw.count()} registros")

# COMMAND ----------

# Adicionando metadados
df_vendas_bronze = adicionar_metadados_ingestao(df_vendas_raw, "vendas.csv")

# Visualizando dados
print("📊 Preview dos dados de vendas (Bronze):")
display(df_vendas_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Produtos

# COMMAND ----------

schema_produtos_raw = StructType([
    StructField("id_produto", StringType(), True),
    StructField("nome_produto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco_base", StringType(), True),
    StructField("custo", StringType(), True)
])

# Lendo produtos do Unity Catalog Volume
df_produtos_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(schema_produtos_raw) \
    .load(f"{VOLUME_BRONZE}/produtos.csv")

df_produtos_bronze = adicionar_metadados_ingestao(df_produtos_raw, "produtos.csv")
print(f"✅ Arquivo produtos.csv carregado: {df_produtos_raw.count()} registros")

print("📊 Preview dos dados de produtos (Bronze):")
display(df_produtos_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Clientes

# COMMAND ----------

schema_clientes_raw = StructType([
    StructField("id_cliente", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("email", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_cadastro", StringType(), True)
])

# Lendo clientes do Unity Catalog Volume
df_clientes_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(schema_clientes_raw) \
    .load(f"{VOLUME_BRONZE}/clientes.csv")

df_clientes_bronze = adicionar_metadados_ingestao(df_clientes_raw, "clientes.csv")
print(f"✅ Arquivo clientes.csv carregado: {df_clientes_raw.count()} registros")

print("📊 Preview dos dados de clientes (Bronze):")
display(df_clientes_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Salvando na Camada Bronze (Delta)

# COMMAND ----------

# Salvando vendas
df_vendas_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("data_ingestao") \
    .saveAsTable(f"{SCHEMA_BRONZE}.vendas")

print(f"✅ Tabela {SCHEMA_BRONZE}.vendas criada")

# COMMAND ----------

# Salvando produtos
df_produtos_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_BRONZE}.produtos")

print(f"✅ Tabela {SCHEMA_BRONZE}.produtos criada")

# COMMAND ----------

# Salvando clientes
df_clientes_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_BRONZE}.clientes")

print(f"✅ Tabela {SCHEMA_BRONZE}.clientes criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Estatísticas da Ingestão

# COMMAND ----------

# Contagem de registros por tabela
stats = [
    ("vendas", df_vendas_bronze.count()),
    ("produtos", df_produtos_bronze.count()),
    ("clientes", df_clientes_bronze.count())
]

print("📈 Estatísticas da Ingestão Bronze:")
print("-" * 40)
for tabela, qtd in stats:
    print(f"  {tabela}: {qtd} registros")
print("-" * 40)
print(f"  TOTAL: {sum(q for _, q in stats)} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Validação: Verificando Tabelas Criadas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listando tabelas no schema bronze
# MAGIC SHOW TABLES IN bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificando dados de vendas
# MAGIC SELECT * FROM bronze.vendas LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo da Camada Bronze
# MAGIC 
# MAGIC | Tabela | Registros | Particionamento |
# MAGIC |--------|-----------|-----------------|
# MAGIC | `bronze.vendas` | ~50 | `_data_ingestao` |
# MAGIC | `bronze.produtos` | ~20 | Nenhum |
# MAGIC | `bronze.clientes` | ~30 | Nenhum |
# MAGIC 
# MAGIC ### ⚠️ Problemas Identificados nos Dados (propositais para demonstração)
# MAGIC 
# MAGIC - Registros com `id_cliente` nulo
# MAGIC - Registros com `id_produto` nulo
# MAGIC - Registros duplicados
# MAGIC - Quantidade negativa em uma venda
# MAGIC - Data nula em uma venda
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ▶️ **Próximo passo**: Execute o notebook `02_silver_limpeza` para limpar esses dados
