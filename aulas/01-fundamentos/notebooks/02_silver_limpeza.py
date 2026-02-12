# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Camada Silver - Limpeza e Validação
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Limpar, validar e tipificar os dados vindos do Bronze
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Características da Camada Silver
# MAGIC 
 - ✅ Schema enforcement (tipagem correta)
 - ✅ Remoção de duplicatas
 - ✅ Validação de campos obrigatórios
 - ✅ Filtro de registros inválidos (quarentena)
 - ✅ Padronização de formatos

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
SCHEMA_BRONZE = f"{CATALOGO}.bronze"
SCHEMA_SILVER = f"{CATALOGO}.silver"
PATH_QUARENTENA = f"/tmp/{CATALOGO}/_quarentena"

spark.sql(f"USE CATALOG {CATALOGO}")

print(f"🥈 Limpeza Silver")
print(f"📥 Fonte: {SCHEMA_BRONZE}")
print(f"📤 Destino: {SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Leitura dos Dados Bronze

# COMMAND ----------

# Lendo tabelas bronze
df_vendas_bronze = spark.table(f"{SCHEMA_BRONZE}.vendas")
df_produtos_bronze = spark.table(f"{SCHEMA_BRONZE}.produtos")
df_clientes_bronze = spark.table(f"{SCHEMA_BRONZE}.clientes")

print(f"📊 Registros lidos do Bronze:")
print(f"  Vendas: {df_vendas_bronze.count()}")
print(f"  Produtos: {df_produtos_bronze.count()}")
print(f"  Clientes: {df_clientes_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Limpeza: Vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1️⃣ Tipagem Correta

# COMMAND ----------

# Convertendo tipos de dados
df_vendas_tipado = df_vendas_bronze \
    .withColumn("id_venda", F.col("id_venda").cast(IntegerType())) \
    .withColumn("data_venda", F.to_date(F.col("data_venda"), "yyyy-MM-dd")) \
    .withColumn("quantidade", F.col("quantidade").cast(IntegerType())) \
    .withColumn("preco_unitario", F.col("preco_unitario").cast(DecimalType(10, 2))) \
    .withColumn("desconto_aplicado", F.col("desconto_aplicado").cast(DecimalType(5, 2)))

print("✅ Tipagem aplicada")
df_vendas_tipado.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2️⃣ Identificação de Registros Inválidos

# COMMAND ----------

# Definindo regras de validação
df_vendas_com_validacao = df_vendas_tipado \
    .withColumn("_is_id_valido", F.col("id_venda").isNotNull()) \
    .withColumn("_is_data_valida", F.col("data_venda").isNotNull()) \
    .withColumn("_is_cliente_valido", F.col("id_cliente").isNotNull()) \
    .withColumn("_is_produto_valido", F.col("id_produto").isNotNull()) \
    .withColumn("_is_quantidade_valida", F.col("quantidade") > 0) \
    .withColumn("_is_preco_valido", F.col("preco_unitario") > 0)

# Flag de registro válido
df_vendas_com_validacao = df_vendas_com_validacao \
    .withColumn("_is_valido", 
        F.col("_is_id_valido") & 
        F.col("_is_data_valida") & 
        F.col("_is_cliente_valido") & 
        F.col("_is_produto_valido") & 
        F.col("_is_quantidade_valida") & 
        F.col("_is_preco_valido")
    )

# Contagem de válidos vs inválidos
print("📊 Resultado da Validação:")
display(df_vendas_com_validacao.groupBy("_is_valido").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Separação: Válidos vs Quarentena

# COMMAND ----------

# Registros válidos
df_vendas_validas = df_vendas_com_validacao.filter(F.col("_is_valido") == True)

# Registros para quarentena (com motivo do erro)
df_vendas_quarentena = df_vendas_com_validacao \
    .filter(F.col("_is_valido") == False) \
    .withColumn("_motivo_rejeicao", 
        F.concat_ws(", ",
            F.when(~F.col("_is_id_valido"), F.lit("ID nulo")),
            F.when(~F.col("_is_data_valida"), F.lit("Data nula")),
            F.when(~F.col("_is_cliente_valido"), F.lit("Cliente nulo")),
            F.when(~F.col("_is_produto_valido"), F.lit("Produto nulo")),
            F.when(~F.col("_is_quantidade_valida"), F.lit("Quantidade inválida")),
            F.when(~F.col("_is_preco_valido"), F.lit("Preço inválido"))
        )
    )

print(f"✅ Válidos: {df_vendas_validas.count()}")
print(f"❌ Quarentena: {df_vendas_quarentena.count()}")

# Visualizando registros rejeitados
print("\n⚠️ Registros enviados para quarentena:")
display(df_vendas_quarentena.select("id_venda", "id_cliente", "id_produto", "quantidade", "_motivo_rejeicao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4️⃣ Remoção de Duplicatas

# COMMAND ----------

# Identificando duplicatas por chave de negócio
window_dedup = Window.partitionBy("id_venda").orderBy(F.col("_ingestao_timestamp").desc())

df_vendas_dedup = df_vendas_validas \
    .withColumn("_rank", F.row_number().over(window_dedup)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

qtd_antes = df_vendas_validas.count()
qtd_depois = df_vendas_dedup.count()
duplicatas_removidas = qtd_antes - qtd_depois

print(f"🔄 Deduplicação:")
print(f"  Antes: {qtd_antes}")
print(f"  Depois: {qtd_depois}")
print(f"  Duplicatas removidas: {duplicatas_removidas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5️⃣ Seleção de Colunas Finais

# COMMAND ----------

# Removendo colunas de validação temporárias
colunas_silver = [
    "id_venda",
    "data_venda", 
    "id_cliente",
    "id_produto",
    "quantidade",
    "preco_unitario",
    "desconto_aplicado",
    "_ingestao_timestamp",
    "_arquivo_fonte"
]

df_vendas_silver = df_vendas_dedup.select(colunas_silver) \
    .withColumn("_processado_em", F.current_timestamp())

print("✅ Schema Silver - Vendas:")
df_vendas_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Limpeza: Produtos

# COMMAND ----------

# Tipagem e limpeza de produtos
df_produtos_silver = df_produtos_bronze \
    .withColumn("preco_base", F.col("preco_base").cast(DecimalType(10, 2))) \
    .withColumn("custo", F.col("custo").cast(DecimalType(10, 2))) \
    .filter(F.col("id_produto").isNotNull()) \
    .withColumn("nome_produto", F.trim(F.col("nome_produto"))) \
    .withColumn("categoria", F.upper(F.trim(F.col("categoria")))) \
    .withColumn("_processado_em", F.current_timestamp()) \
    .dropDuplicates(["id_produto"])

print("✅ Produtos limpos")
display(df_produtos_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Limpeza: Clientes

# COMMAND ----------

# Tipagem e limpeza de clientes
df_clientes_silver = df_clientes_bronze \
    .withColumn("data_cadastro", F.to_date(F.col("data_cadastro"), "yyyy-MM-dd")) \
    .filter(F.col("id_cliente").isNotNull()) \
    .withColumn("nome", F.initcap(F.trim(F.col("nome")))) \
    .withColumn("email", F.lower(F.trim(F.col("email")))) \
    .withColumn("cidade", F.initcap(F.trim(F.col("cidade")))) \
    .withColumn("estado", F.upper(F.trim(F.col("estado")))) \
    .withColumn("_processado_em", F.current_timestamp()) \
    .dropDuplicates(["id_cliente"])

print("✅ Clientes limpos")
display(df_clientes_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Salvando na Camada Silver

# COMMAND ----------

# Salvando vendas silver
df_vendas_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_SILVER}.vendas")

print(f"✅ Tabela {SCHEMA_SILVER}.vendas criada")

# COMMAND ----------

# Salvando produtos silver
df_produtos_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_SILVER}.produtos")

print(f"✅ Tabela {SCHEMA_SILVER}.produtos criada")

# COMMAND ----------

# Salvando clientes silver
df_clientes_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_SILVER}.clientes")

print(f"✅ Tabela {SCHEMA_SILVER}.clientes criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗑️ Salvando Quarentena

# COMMAND ----------

# Salvando registros rejeitados para análise posterior
if df_vendas_quarentena.count() > 0:
    df_vendas_quarentena.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{PATH_QUARENTENA}/vendas")
    
    print(f"⚠️ {df_vendas_quarentena.count()} registros salvos em quarentena")
else:
    print("✅ Nenhum registro em quarentena")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Estatísticas da Limpeza

# COMMAND ----------

# Comparativo Bronze vs Silver
print("📈 Comparativo Bronze → Silver:")
print("-" * 50)

tabelas = ["vendas", "produtos", "clientes"]
for tabela in tabelas:
    qtd_bronze = spark.table(f"{SCHEMA_BRONZE}.{tabela}").count()
    qtd_silver = spark.table(f"{SCHEMA_SILVER}.{tabela}").count()
    removidos = qtd_bronze - qtd_silver
    pct = (removidos / qtd_bronze * 100) if qtd_bronze > 0 else 0
    
    print(f"  {tabela}:")
    print(f"    Bronze: {qtd_bronze} → Silver: {qtd_silver}")
    print(f"    Removidos: {removidos} ({pct:.1f}%)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validação Final

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificando tabelas no schema silver
# MAGIC SHOW TABLES IN silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra de vendas limpas
# MAGIC SELECT * FROM silver.vendas LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo da Camada Silver
# MAGIC 
# MAGIC | Operação | Resultado |
# MAGIC |----------|-----------|
# MAGIC | Tipagem | ✅ Aplicada |
# MAGIC | Validações | ✅ Campos obrigatórios verificados |
# MAGIC | Deduplicação | ✅ Duplicatas removidas |
# MAGIC | Quarentena | ✅ Registros inválidos separados |
# MAGIC 
# MAGIC ### Qualidade dos Dados
# MAGIC 
# MAGIC | Tabela | Bronze | Silver | Taxa de Aprovação |
# MAGIC |--------|--------|--------|-------------------|
# MAGIC | `vendas` | ~50 | ~45 | ~90% |
# MAGIC | `produtos` | ~20 | ~20 | 100% |
# MAGIC | `clientes` | ~30 | ~30 | 100% |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ▶️ **Próximo passo**: Execute o notebook `03_worked_enriquecimento` para enriquecer os dados
