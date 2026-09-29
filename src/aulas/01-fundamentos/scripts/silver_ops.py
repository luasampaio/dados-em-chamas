"""
Utilitários para Operações na Camada Silver
Canal: Dados em Chamas 🔥

Este módulo contém funções para facilitar a inserção e atualização de dados
na camada Silver, suportando diferentes estratégias de escrita (SCD Type 1, Overwrite, Append).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
from typing import List, Optional


class SilverManager:
    """
    Classe para gerenciar operações na camada Silver.
    
    Atributos:
        spark: Sessão Spark ativa
        catalog: Nome do catálogo (default: dados_em_chamas)
        schema: Nome do schema silver (default: silver)
    """
    
    def __init__(self, spark: SparkSession = None, catalog: str = "dados_em_chamas", schema: str = "silver"):
        self.spark = spark or SparkSession.getActiveSession()
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"

    def _table_exists(self, table_name: str) -> bool:
        """Verifica se a tabela existe no catálogo/schema."""
        return self.spark.catalog.tableExists(f"{self.full_schema}.{table_name}")

    def upsert(self, df: DataFrame, table_name: str, primary_keys: List[str]) -> None:
        """
        Realiza um Upsert (Merge) na tabela silver.
        Se o registro existir (baseado nas primary_keys), atualiza. Caso contrário, insere.
        
        Args:
            df: DataFrame com os novos dados
            table_name: Nome da tabela destino
            primary_keys: Lista de colunas que compõem a chave primária
        """
        target_path = f"{self.full_schema}.{table_name}"
        
        # Adiciona metadados de processamento
        df_to_save = df.withColumn("_processado_em", F.current_timestamp())
        
        if not self._table_exists(table_name):
            print(f"📦 Tabela {target_path} não existe. Criando nova tabela...")
            df_to_save.write.format("delta").mode("overwrite").saveAsTable(target_path)
            return

        print(f"🔄 Realizando upsert em {target_path}...")
        
        delta_table = DeltaTable.forName(self.spark, target_path)
        
        # Constrói a condição de merge
        join_condition = " AND ".join([f"target.{c} = source.{c}" for c in primary_keys])
        
        delta_table.alias("target").merge(
            df_to_save.alias("source"),
            join_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        print(f"✅ Upsert concluído com sucesso")

    def overwrite(self, df: DataFrame, table_name: str) -> None:
        """
        Sobrescreve a tabela silver com os dados do DataFrame.
        
        Args:
            df: DataFrame com os dados
            table_name: Nome da tabela destino
        """
        target_path = f"{self.full_schema}.{table_name}"
        
        # Adiciona metadados de processamento
        df_to_save = df.withColumn("_processado_em", F.current_timestamp())
        
        print(f"💾 Sobrescrevendo tabela {target_path}...")
        
        df_to_save.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_path)
            
        print(f"✅ Tabela {table_name} sobrescrita com sucesso")

    def append(self, df: DataFrame, table_name: str) -> None:
        """
        Adiciona registros à tabela silver.
        
        Args:
            df: DataFrame com os dados
            table_name: Nome da tabela destino
        """
        target_path = f"{self.full_schema}.{table_name}"
        
        # Adiciona metadados de processamento
        df_to_save = df.withColumn("_processado_em", F.current_timestamp())
        
        print(f"📥 Adicionando dados à {target_path}...")
        
        df_to_save.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_path)
            
        print(f"✅ Dados adicionados com sucesso")


def salvar_quarentena(df: DataFrame, table_name: str, path_base: str = "/Volumes/dados_em_chamas/bronze/temp") -> None:
    """
    Salva registros inválidos na pasta de quarentena dentro de um Volume do Unity Catalog.
    
    Args:
        df: DataFrame com os registros da quarentena
        table_name: Nome da tabela de origem
        path_base: Caminho base (UC Volume) para os arquivos de quarentena
    """
    if df.count() == 0:
        print(f"✅ Nenhum registro para quarentena da tabela {table_name}")
        return

    # Garante que o caminho termine com barra para concatenação correta
    if not path_base.endswith("/"):
        path_base += "/"
        
    full_path = f"{path_base}{table_name}"
    
    print(f"⚠️ Salvando {df.count()} registros em quarentena no Volume: {full_path}")
    
    df.withColumn("_rejeitado_em", F.current_timestamp()) \
      .write \
      .format("delta") \
      .mode("append") \
      .save(full_path)


# Exemplo de uso
if __name__ == "__main__":
    print("🔥 Biblioteca Silver Ops - Dados em Chamas")
    print("Use a classe SilverManager para gerenciar suas tabelas na camada Silver.")
    print(f"Padrão de Quarentena: /Volumes/dados_em_chamas/bronze/temp")
