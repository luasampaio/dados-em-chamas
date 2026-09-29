"""
Utilitários de Validação de Qualidade de Dados
Canal: Dados em Chamas 🔥

Este módulo contém funções reutilizáveis para validação de qualidade de dados
que podem ser utilizadas em qualquer camada do pipeline Medallion.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ResultadoValidacao:
    """Resultado de uma validação de qualidade."""
    regra: str
    passou: bool
    total_registros: int
    registros_validos: int
    registros_invalidos: int
    percentual_valido: float
    detalhes: Optional[str] = None


class QualityChecker:
    """
    Classe para validação de qualidade de dados.
    
    Exemplo de uso:
        checker = QualityChecker(df)
        resultado = checker.check_not_null("id_cliente")
        print(resultado)
    """
    
    def __init__(self, df: DataFrame):
        """
        Inicializa o validador com um DataFrame.
        
        Args:
            df: DataFrame PySpark a ser validado
        """
        self.df = df
        self.total_registros = df.count()
        self.resultados: List[ResultadoValidacao] = []
    
    def check_not_null(self, coluna: str) -> ResultadoValidacao:
        """
        Valida se uma coluna não possui valores nulos.
        
        Args:
            coluna: Nome da coluna a validar
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        nulos = self.df.filter(F.col(coluna).isNull()).count()
        validos = self.total_registros - nulos
        percentual = (validos / self.total_registros * 100) if self.total_registros > 0 else 0
        
        resultado = ResultadoValidacao(
            regra=f"NOT_NULL({coluna})",
            passou=nulos == 0,
            total_registros=self.total_registros,
            registros_validos=validos,
            registros_invalidos=nulos,
            percentual_valido=round(percentual, 2),
            detalhes=f"Encontrados {nulos} valores nulos"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def check_unique(self, colunas: List[str]) -> ResultadoValidacao:
        """
        Valida se uma ou mais colunas são únicas (sem duplicatas).
        
        Args:
            colunas: Lista de colunas que formam a chave única
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        total_distintos = self.df.select(colunas).distinct().count()
        duplicatas = self.total_registros - total_distintos
        percentual = (total_distintos / self.total_registros * 100) if self.total_registros > 0 else 0
        
        resultado = ResultadoValidacao(
            regra=f"UNIQUE({', '.join(colunas)})",
            passou=duplicatas == 0,
            total_registros=self.total_registros,
            registros_validos=total_distintos,
            registros_invalidos=duplicatas,
            percentual_valido=round(percentual, 2),
            detalhes=f"Encontradas {duplicatas} duplicatas"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def check_range(self, coluna: str, min_val: float = None, max_val: float = None) -> ResultadoValidacao:
        """
        Valida se os valores de uma coluna estão dentro de um range.
        
        Args:
            coluna: Nome da coluna a validar
            min_val: Valor mínimo permitido (opcional)
            max_val: Valor máximo permitido (opcional)
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        condicao = F.lit(True)
        
        if min_val is not None:
            condicao = condicao & (F.col(coluna) >= min_val)
        if max_val is not None:
            condicao = condicao & (F.col(coluna) <= max_val)
        
        validos = self.df.filter(condicao).count()
        invalidos = self.total_registros - validos
        percentual = (validos / self.total_registros * 100) if self.total_registros > 0 else 0
        
        range_str = f"[{min_val if min_val else '-∞'}, {max_val if max_val else '+∞'}]"
        
        resultado = ResultadoValidacao(
            regra=f"RANGE({coluna}) in {range_str}",
            passou=invalidos == 0,
            total_registros=self.total_registros,
            registros_validos=validos,
            registros_invalidos=invalidos,
            percentual_valido=round(percentual, 2),
            detalhes=f"Encontrados {invalidos} valores fora do range"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def check_referential_integrity(
        self, 
        coluna_fk: str, 
        df_referencia: DataFrame, 
        coluna_pk: str
    ) -> ResultadoValidacao:
        """
        Valida integridade referencial entre DataFrames.
        
        Args:
            coluna_fk: Coluna de chave estrangeira no DataFrame principal
            df_referencia: DataFrame de referência (tabela pai)
            coluna_pk: Coluna de chave primária na tabela de referência
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        # Valores únicos da FK
        valores_fk = self.df.select(coluna_fk).distinct()
        
        # Valores que não existem na referência
        valores_pk = df_referencia.select(F.col(coluna_pk).alias(coluna_fk)).distinct()
        orfaos = valores_fk.join(valores_pk, on=coluna_fk, how="left_anti")
        
        qtd_orfaos = orfaos.count()
        qtd_total_fk = valores_fk.count()
        validos = qtd_total_fk - qtd_orfaos
        percentual = (validos / qtd_total_fk * 100) if qtd_total_fk > 0 else 100
        
        resultado = ResultadoValidacao(
            regra=f"FK({coluna_fk}) -> PK({coluna_pk})",
            passou=qtd_orfaos == 0,
            total_registros=qtd_total_fk,
            registros_validos=validos,
            registros_invalidos=qtd_orfaos,
            percentual_valido=round(percentual, 2),
            detalhes=f"Encontrados {qtd_orfaos} valores órfãos"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def check_regex(self, coluna: str, pattern: str, descricao: str = None) -> ResultadoValidacao:
        """
        Valida se os valores de uma coluna correspondem a um padrão regex.
        
        Args:
            coluna: Nome da coluna a validar
            pattern: Padrão regex para validação
            descricao: Descrição do padrão (opcional)
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        validos = self.df.filter(F.col(coluna).rlike(pattern)).count()
        invalidos = self.total_registros - validos
        percentual = (validos / self.total_registros * 100) if self.total_registros > 0 else 0
        
        resultado = ResultadoValidacao(
            regra=f"REGEX({coluna}): {descricao or pattern}",
            passou=invalidos == 0,
            total_registros=self.total_registros,
            registros_validos=validos,
            registros_invalidos=invalidos,
            percentual_valido=round(percentual, 2),
            detalhes=f"Encontrados {invalidos} valores não correspondentes"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def check_enum(self, coluna: str, valores_permitidos: List[str]) -> ResultadoValidacao:
        """
        Valida se os valores de uma coluna estão em uma lista de valores permitidos.
        
        Args:
            coluna: Nome da coluna a validar
            valores_permitidos: Lista de valores aceitos
            
        Returns:
            ResultadoValidacao com o resultado da validação
        """
        validos = self.df.filter(F.col(coluna).isin(valores_permitidos)).count()
        invalidos = self.total_registros - validos
        percentual = (validos / self.total_registros * 100) if self.total_registros > 0 else 0
        
        resultado = ResultadoValidacao(
            regra=f"ENUM({coluna})",
            passou=invalidos == 0,
            total_registros=self.total_registros,
            registros_validos=validos,
            registros_invalidos=invalidos,
            percentual_valido=round(percentual, 2),
            detalhes=f"Valores permitidos: {valores_permitidos[:5]}{'...' if len(valores_permitidos) > 5 else ''}"
        )
        
        self.resultados.append(resultado)
        return resultado
    
    def get_summary(self) -> DataFrame:
        """
        Retorna um DataFrame com o resumo de todas as validações executadas.
        
        Returns:
            DataFrame PySpark com o resumo das validações
        """
        if not self.resultados:
            raise ValueError("Nenhuma validação foi executada ainda")
        
        spark = SparkSession.getActiveSession()
        
        dados = [
            (
                r.regra,
                "✅ PASSOU" if r.passou else "❌ FALHOU",
                r.total_registros,
                r.registros_validos,
                r.registros_invalidos,
                r.percentual_valido,
                r.detalhes
            )
            for r in self.resultados
        ]
        
        schema = StructType([
            StructField("regra", StringType(), False),
            StructField("status", StringType(), False),
            StructField("total_registros", IntegerType(), False),
            StructField("registros_validos", IntegerType(), False),
            StructField("registros_invalidos", IntegerType(), False),
            StructField("percentual_valido", FloatType(), False),
            StructField("detalhes", StringType(), True)
        ])
        
        return spark.createDataFrame(dados, schema)
    
    def all_passed(self) -> bool:
        """
        Verifica se todas as validações passaram.
        
        Returns:
            True se todas as validações passaram, False caso contrário
        """
        return all(r.passou for r in self.resultados)


# Funções auxiliares para uso direto
def validar_tabela(df: DataFrame, regras: Dict) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica múltiplas regras de validação e separa registros válidos dos inválidos.
    
    Args:
        df: DataFrame a validar
        regras: Dicionário com regras de validação
            {
                "not_null": ["coluna1", "coluna2"],
                "range": {"coluna3": {"min": 0, "max": 100}},
                "enum": {"coluna4": ["valor1", "valor2"]}
            }
    
    Returns:
        Tupla (df_validos, df_invalidos)
    """
    condicao_valido = F.lit(True)
    
    # Aplicando regras NOT NULL
    for coluna in regras.get("not_null", []):
        condicao_valido = condicao_valido & F.col(coluna).isNotNull()
    
    # Aplicando regras de RANGE
    for coluna, limites in regras.get("range", {}).items():
        min_val = limites.get("min")
        max_val = limites.get("max")
        
        if min_val is not None:
            condicao_valido = condicao_valido & (F.col(coluna) >= min_val)
        if max_val is not None:
            condicao_valido = condicao_valido & (F.col(coluna) <= max_val)
    
    # Aplicando regras de ENUM
    for coluna, valores in regras.get("enum", {}).items():
        condicao_valido = condicao_valido & F.col(coluna).isin(valores)
    
    # Separando registros
    df_validos = df.filter(condicao_valido)
    df_invalidos = df.filter(~condicao_valido)
    
    return df_validos, df_invalidos


def gerar_relatorio_qualidade(
    df: DataFrame, 
    nome_tabela: str,
    validacoes: List[str] = None
) -> Dict:
    """
    Gera um relatório de qualidade completo para uma tabela.
    
    Args:
        df: DataFrame a analisar
        nome_tabela: Nome da tabela para o relatório
        validacoes: Lista de colunas para validar (opcional, usa todas se não especificado)
    
    Returns:
        Dicionário com métricas de qualidade
    """
    colunas = validacoes or df.columns
    total = df.count()
    
    metricas = {
        "tabela": nome_tabela,
        "timestamp": datetime.now().isoformat(),
        "total_registros": total,
        "total_colunas": len(df.columns),
        "colunas_analisadas": len(colunas),
        "metricas_por_coluna": {}
    }
    
    for coluna in colunas:
        nulos = df.filter(F.col(coluna).isNull()).count()
        distintos = df.select(coluna).distinct().count()
        
        metricas["metricas_por_coluna"][coluna] = {
            "nulos": nulos,
            "nulos_pct": round(nulos / total * 100, 2) if total > 0 else 0,
            "valores_distintos": distintos,
            "cardinalidade_pct": round(distintos / total * 100, 2) if total > 0 else 0
        }
    
    # Score geral de qualidade
    total_nulos = sum(m["nulos"] for m in metricas["metricas_por_coluna"].values())
    total_celulas = total * len(colunas)
    metricas["score_qualidade"] = round(
        (1 - total_nulos / total_celulas) * 100, 2
    ) if total_celulas > 0 else 100
    
    return metricas


# Exemplo de uso
if __name__ == "__main__":
    # Este código só roda quando executado diretamente
    print("🔥 Módulo de Quality Checks - Dados em Chamas")
    print("Importe este módulo para usar as funções de validação")
    print()
    print("Exemplo:")
    print("  from quality_checks import QualityChecker")
    print("  checker = QualityChecker(df)")
    print("  checker.check_not_null('id_cliente')")
    print("  display(checker.get_summary())")
