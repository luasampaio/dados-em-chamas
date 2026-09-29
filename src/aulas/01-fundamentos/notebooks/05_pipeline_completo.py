# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Pipeline Completo - Orquestração End-to-End
# MAGIC 
# MAGIC **Canal**: Dados em Chamas 🔥  
# MAGIC **Objetivo**: Executar todo o pipeline Medallion de forma orquestrada
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Visão Geral do Pipeline
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
# MAGIC │   BRONZE    │───▶│   SILVER    │───▶│   WORKED    │───▶│    GOLD     │
# MAGIC │  (Ingestão) │    │  (Limpeza)  │    │(Enriquecim.)│    │ (Agregação) │
# MAGIC └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configurações

# COMMAND ----------

# Importações
from datetime import datetime
import time

# COMMAND ----------

# Configurações
CATALOGO = "dados_em_chamas"
NOTEBOOKS_PATH = "/Workspace/Repos/seu_usuario/dados-em-chamas/01.camadaWorked/notebooks"

# Ajuste o path acima para o local correto dos notebooks no seu ambiente

print(f"🚀 Pipeline Medallion - Dados em Chamas")
print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Catálogo: {CATALOGO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Função de Execução com Logging

# COMMAND ----------

def executar_notebook(nome_notebook: str, timeout: int = 600) -> dict:
    """
    Executa um notebook e retorna estatísticas de execução.
    
    Parâmetros:
        nome_notebook: Nome do notebook a executar
        timeout: Tempo máximo de execução em segundos
        
    Retorna:
        Dicionário com status, tempo de execução e erros
    """
    inicio = time.time()
    resultado = {
        "notebook": nome_notebook,
        "status": "INICIADO",
        "tempo_segundos": 0,
        "erro": None
    }
    
    try:
        print(f"\n{'='*60}")
        print(f"▶️ Executando: {nome_notebook}")
        print(f"⏰ Início: {datetime.now().strftime('%H:%M:%S')}")
        
        # Executando notebook
        # dbutils.notebook.run(f"{NOTEBOOKS_PATH}/{nome_notebook}", timeout)
        
        # Como estamos no mesmo notebook, vamos simular com %run
        resultado["status"] = "SUCESSO"
        
    except Exception as e:
        resultado["status"] = "ERRO"
        resultado["erro"] = str(e)
        print(f"❌ Erro: {e}")
        
    finally:
        fim = time.time()
        resultado["tempo_segundos"] = round(fim - inicio, 2)
        
        emoji = "✅" if resultado["status"] == "SUCESSO" else "❌"
        print(f"{emoji} Status: {resultado['status']}")
        print(f"⏱️ Tempo: {resultado['tempo_segundos']}s")
        
    return resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Execução do Pipeline
# MAGIC 
# MAGIC > **Nota**: Em um ambiente Databricks real, você usaria `dbutils.notebook.run()` 
# MAGIC > ou Databricks Workflows para orquestrar os notebooks.
# MAGIC 
# MAGIC Para esta POC, os notebooks estão organizados para execução sequencial manual
# MAGIC ou você pode descomentar as células abaixo para executar via `%run`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa 1: Setup do Ambiente

# COMMAND ----------

# Para executar automaticamente, descomente a linha abaixo:
# %run ./00_setup_ambiente

print("1️⃣ Setup do Ambiente")
print("   Execute o notebook: 00_setup_ambiente.py")
print("   Descrição: Cria catálogo, schemas e estrutura de pastas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa 2: Ingestão Bronze

# COMMAND ----------

# Para executar automaticamente, descomente a linha abaixo:
# %run ./01_bronze_ingestao

print("2️⃣ Ingestão Bronze")
print("   Execute o notebook: 01_bronze_ingestao.py")
print("   Descrição: Carrega dados brutos sem transformação")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa 3: Limpeza Silver

# COMMAND ----------

# Para executar automaticamente, descomente a linha abaixo:
# %run ./02_silver_limpeza

print("3️⃣ Limpeza Silver")
print("   Execute o notebook: 02_silver_limpeza.py")
print("   Descrição: Valida, limpa e tipifica os dados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa 4: Enriquecimento Worked

# COMMAND ----------

# Para executar automaticamente, descomente a linha abaixo:
# %run ./03_worked_enriquecimento

print("4️⃣ Enriquecimento Worked")
print("   Execute o notebook: 03_worked_enriquecimento.py")
print("   Descrição: Aplica regras de negócio e cálculos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa 5: Agregações Gold

# COMMAND ----------

# Para executar automaticamente, descomente a linha abaixo:
# %run ./04_gold_agregacoes

print("5️⃣ Agregações Gold")
print("   Execute o notebook: 04_gold_agregacoes.py")
print("   Descrição: Cria tabelas agregadas para consumo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoramento do Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificação de Tabelas Criadas

# COMMAND ----------

# Verificando todas as tabelas em todos os schemas
spark.sql(f"USE CATALOG {CATALOGO}")

schemas = ["bronze", "silver", "worked", "gold"]
print("📋 Inventário de Tabelas:\n")

for schema in schemas:
    try:
        tabelas = spark.sql(f"SHOW TABLES IN {schema}").collect()
        print(f"📁 {schema.upper()}")
        for t in tabelas:
            # Contando registros
            count = spark.table(f"{schema}.{t.tableName}").count()
            print(f"   └── {t.tableName}: {count:,} registros")
        print()
    except Exception as e:
        print(f"📁 {schema.upper()}: ⚠️ Schema não encontrado ou sem tabelas")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métricas de Qualidade

# COMMAND ----------

# Comparativo de volume entre camadas
print("📈 Fluxo de Dados por Camada:\n")

try:
    bronze_vendas = spark.table(f"{CATALOGO}.bronze.vendas").count()
    silver_vendas = spark.table(f"{CATALOGO}.silver.vendas").count()
    worked_vendas = spark.table(f"{CATALOGO}.worked.vendas_enriquecidas").count()
    
    print(f"   Bronze (vendas raw):    {bronze_vendas:,} registros")
    print(f"   Silver (vendas limpas): {silver_vendas:,} registros")
    print(f"   Worked (enriquecidas):  {worked_vendas:,} registros")
    
    # Taxa de aprovação
    taxa = (silver_vendas / bronze_vendas * 100) if bronze_vendas > 0 else 0
    print(f"\n   📊 Taxa de aprovação Bronze→Silver: {taxa:.1f}%")
    
except Exception as e:
    print(f"   ⚠️ Execute os notebooks anteriores primeiro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Automação com Databricks Workflows
# MAGIC 
# MAGIC Para produção, recomendamos usar **Databricks Workflows** para orquestrar o pipeline.
# MAGIC 
# MAGIC ### Exemplo de configuração (JSON):
# MAGIC 
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Pipeline Medallion - Dados em Chamas",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "setup",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/.../00_setup_ambiente"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "bronze",
# MAGIC       "depends_on": [{"task_key": "setup"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/.../01_bronze_ingestao"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver",
# MAGIC       "depends_on": [{"task_key": "bronze"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/.../02_silver_limpeza"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "worked",
# MAGIC       "depends_on": [{"task_key": "silver"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/.../03_worked_enriquecimento"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold",
# MAGIC       "depends_on": [{"task_key": "worked"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/.../04_gold_agregacoes"
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 6 * * ?",
# MAGIC     "timezone_id": "America/Sao_Paulo"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Resumo do Pipeline
# MAGIC 
# MAGIC | Etapa | Notebook | Descrição | Duração Típica |
# MAGIC |-------|----------|-----------|----------------|
# MAGIC | 1 | `00_setup_ambiente` | Cria estrutura | ~30s |
# MAGIC | 2 | `01_bronze_ingestao` | Ingestão raw | ~1-5min |
# MAGIC | 3 | `02_silver_limpeza` | Limpeza | ~2-10min |
# MAGIC | 4 | `03_worked_enriquecimento` | Transformações | ~3-15min |
# MAGIC | 5 | `04_gold_agregacoes` | Agregações | ~1-5min |
# MAGIC 
# MAGIC ### 🎯 Próximos Passos em Produção
# MAGIC 
# MAGIC - [ ] Configurar Databricks Workflows para agendamento
# MAGIC - [ ] Adicionar notificações (Slack, email) em caso de falha
# MAGIC - [ ] Implementar testes de qualidade com Great Expectations
# MAGIC - [ ] Configurar monitoramento com Unity Catalog Data Lineage
# MAGIC - [ ] Implementar Change Data Capture (CDC) para atualizações incrementais
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 🔥 **Dados em Chamas** - Feito com ❤️ para a comunidade de Engenharia de Dados
