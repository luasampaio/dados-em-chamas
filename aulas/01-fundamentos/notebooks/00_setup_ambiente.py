# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Setup do Ambiente - POC Arquitetura Medallion
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Configurar o ambiente para a POC de ingestão com arquitetura Medallion
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Configurações do Notebook

# COMMAND ----------

# Widgets para parametrização
dbutils.widgets.text("catalogo", "dados_em_chamas", "Nome do Catálogo")
dbutils.widgets.text("ambiente", "dev", "Ambiente (dev/prod)")

# COMMAND ----------

# Capturando parâmetros
CATALOGO = dbutils.widgets.get("catalogo")
AMBIENTE = dbutils.widgets.get("ambiente")

print(f"🔥 Dados em Chamas - POC Medallion")
print(f"📁 Catálogo: {CATALOGO}")
print(f"🌍 Ambiente: {AMBIENTE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏛️ Definição das Camadas

# COMMAND ----------

# Schemas para cada camada da arquitetura Medallion
SCHEMA_BRONZE = f"{CATALOGO}.bronze"
SCHEMA_SILVER = f"{CATALOGO}.silver"
SCHEMA_WORKED = f"{CATALOGO}.worked"
SCHEMA_GOLD = f"{CATALOGO}.gold"

# Paths de armazenamento (ajuste conforme seu ambiente)
# Para DBFS
PATH_BASE = f"/mnt/{CATALOGO}" if AMBIENTE == "prod" else f"/tmp/{CATALOGO}"
PATH_BRONZE = f"{PATH_BASE}/bronze"
PATH_SILVER = f"{PATH_BASE}/silver"
PATH_WORKED = f"{PATH_BASE}/worked"
PATH_GOLD = f"{PATH_BASE}/gold"
PATH_CHECKPOINT = f"{PATH_BASE}/_checkpoints"
PATH_QUARENTENA = f"{PATH_BASE}/_quarentena"

print("📍 Paths configurados:")
print(f"  Bronze:     {PATH_BRONZE}")
print(f"  Silver:     {PATH_SILVER}")
print(f"  Worked:     {PATH_WORKED}")
print(f"  Gold:       {PATH_GOLD}")
print(f"  Checkpoint: {PATH_CHECKPOINT}")
print(f"  Quarentena: {PATH_QUARENTENA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ Criação do Catálogo e Schemas
# MAGIC 
# MAGIC > ⚠️ **Nota**: Esta seção requer permissões de administrador no Unity Catalog.
# MAGIC > Se você não tem permissões, peça ao administrador para criar as estruturas.

# COMMAND ----------

# Criação do catálogo (requer permissão de METASTORE ADMIN)
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
    print(f"✅ Catálogo '{CATALOGO}' criado/verificado")
except Exception as e:
    print(f"⚠️ Não foi possível criar o catálogo: {e}")
    print("   Usando catálogo 'hive_metastore' como fallback")
    CATALOGO = "hive_metastore"

# COMMAND ----------

# Definindo catálogo padrão
spark.sql(f"USE CATALOG {CATALOGO}")

# Criando schemas para cada camada
schemas = [
    ("bronze", "Dados brutos, sem transformação"),
    ("silver", "Dados limpos e validados"),
    ("worked", "Dados enriquecidos com regras de negócio"),
    ("gold", "Dados agregados prontos para consumo")
]

for schema_nome, descricao in schemas:
    try:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {schema_nome}
            COMMENT '{descricao}'
        """)
        print(f"✅ Schema '{schema_nome}' criado - {descricao}")
    except Exception as e:
        print(f"❌ Erro ao criar schema '{schema_nome}': {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Criação das Pastas no Storage

# COMMAND ----------

# Criando estrutura de diretórios
pastas = [PATH_BRONZE, PATH_SILVER, PATH_WORKED, PATH_GOLD, PATH_CHECKPOINT, PATH_QUARENTENA]

for pasta in pastas:
    try:
        dbutils.fs.mkdirs(pasta)
        print(f"✅ Pasta criada: {pasta}")
    except Exception as e:
        print(f"⚠️ Aviso para pasta {pasta}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Configurações Globais do Spark

# COMMAND ----------

# Configurações otimizadas para Delta Lake
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# Configuração para melhor performance com arquivos pequenos
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

print("✅ Configurações do Spark aplicadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Salvando Configurações para Outros Notebooks

# COMMAND ----------

# Dicionário com todas as configurações
CONFIG = {
    "catalogo": CATALOGO,
    "ambiente": AMBIENTE,
    "schemas": {
        "bronze": SCHEMA_BRONZE,
        "silver": SCHEMA_SILVER,
        "worked": SCHEMA_WORKED,
        "gold": SCHEMA_GOLD
    },
    "paths": {
        "base": PATH_BASE,
        "bronze": PATH_BRONZE,
        "silver": PATH_SILVER,
        "worked": PATH_WORKED,
        "gold": PATH_GOLD,
        "checkpoint": PATH_CHECKPOINT,
        "quarentena": PATH_QUARENTENA
    }
}

# Exibindo configuração final
import json
print("📋 Configuração Final:")
print(json.dumps(CONFIG, indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validação do Setup

# COMMAND ----------

# Verificando schemas criados
print("🔍 Verificando schemas no catálogo...")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo do Setup
# MAGIC 
# MAGIC | Item | Valor |
# MAGIC |------|-------|
# MAGIC | Catálogo | `dados_em_chamas` |
# MAGIC | Schema Bronze | `dados_em_chamas.bronze` |
# MAGIC | Schema Silver | `dados_em_chamas.silver` |
# MAGIC | Schema Worked | `dados_em_chamas.worked` |
# MAGIC | Schema Gold | `dados_em_chamas.gold` |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ▶️ **Próximo passo**: Execute o notebook `01_bronze_ingestao`
