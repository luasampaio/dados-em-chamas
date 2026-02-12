# Plano de Implementação - Melhorias no Módulo de Ingestão

## Objetivo
Expandir o módulo `dnx_analytics.ingestion` com suporte a múltiplos formatos de arquivo e funcionalidades de validação/qualidade de dados.

---

## Proposed Changes

### Módulo de Ingestão

#### [NEW] [data_loaders.py](file:///d:/dataops-dnx-coe-analytics/dnx_analytics/ingestion/data_loaders.py)

Módulo unificado com funções de carregamento:

```python
# Funções principais
def load_csv(path, sep=None, encoding=None, **options) -> DataFrame
def load_parquet(path, **options) -> DataFrame  
def load_excel(path, sheet_name=0, **options) -> DataFrame
def load_json(path, multiline=False, **options) -> DataFrame
def load_delta(table_or_path, **options) -> DataFrame
def load_auto(path, **options) -> DataFrame  # Detecta formato automaticamente
```

**Características:**
- Fallback de encodings (UTF-8, ISO-8859-1, Windows-1252)
- Detecção automática de separador para CSV
- Limpeza de nomes de colunas (BOM, espaços)
- Logging estruturado opcional

---

#### [NEW] [data_quality.py](file:///d:/dataops-dnx-coe-analytics/dnx_analytics/ingestion/data_quality.py)

Módulo de validação e métricas:

```python
# Funções de qualidade
def validate_schema(df, expected_schema) -> ValidationResult
def get_quality_metrics(df) -> dict  # nulos, duplicatas, contagens
def validate_not_empty(df) -> bool
def validate_no_nulls(df, columns) -> ValidationResult
def log_ingestion(df, source, **metadata) -> None
```

**Métricas retornadas:**
- `row_count`: Total de linhas
- `column_count`: Total de colunas
- `null_counts`: Contagem de nulos por coluna
- `null_percentages`: % de nulos por coluna
- `duplicate_count`: Linhas duplicadas
- `memory_usage`: Estimativa de memória

---


Exportar as novas funções:

```python
from dnx_analytics.ingestion.data_loaders import (
    load_csv, load_parquet, load_excel, load_json, load_delta, load_auto
)
from dnx_analytics.ingestion.data_quality import (
    validate_schema, get_quality_metrics, validate_not_empty, log_ingestion
)
```

---

## Exemplo de Uso

```python
from dnx_analytics.ingestion import load_csv, load_parquet, get_quality_metrics

# Carregar CSV com detecção automática
df = load_csv("/mnt/data/arquivo.csv")

# Carregar Parquet
df = load_parquet("/mnt/data/tabela.parquet")

# Verificar qualidade
metrics = get_quality_metrics(df)
print(f"Linhas: {metrics['row_count']}, Nulos: {metrics['null_counts']}")
```

---

## Verification Plan

### Testes Automatizados
- Testes unitários para cada função de load
- Testes de validação de schema
- Testes de métricas de qualidade

### Verificação Manual
- Testar com arquivos reais no Databricks
- Validar compatibilidade com Unity Catalog
