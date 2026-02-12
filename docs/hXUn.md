# Framework de Ingestão de Dados - Plano de Implementação (Databricks)

Um framework reutilizável e pronto para produção para ingestão de dados no Databricks, consolidando carregamento inteligente de CSV, processamento de texto e padrões de qualidade de dados. Otimizado para Unity Catalog, Delta Lake e workflows baseados em notebooks.

## Revisão do Usuário Necessária

> [!IMPORTANT]
> **Framework Nativo do Databricks**: Este framework foi projetado especificamente para Databricks com:
> - Integração com Unity Catalog (suporte a catalog, schema, volume)
> - Delta Lake como formato de armazenamento primário
> - Estrutura baseada em notebooks para fácil deploy
> - Suporte a Databricks Volumes e DBFS
> - Integração com Databricks Workflows

> [!IMPORTANT]
> **Localização do Projeto**: O framework será criado em 
> PATHDATABRICKS 

> ` com estrutura compatível com Databricks Repos.

> [!NOTE]
> **Idioma**: Todo o código, comentários e documentação serão em **português**, incluindo nomes de funções, variáveis e mensagens de log.

> [!NOTE]
> **Deployment**: O framework pode ser implantado como:
> 1. **Databricks Repo** (recomendado): Importar diretamente no seu workspace
> 2. **Pacote Wheel**: Build e instalação nas bibliotecas do cluster
> 3. **Notebooks**: Copiar notebooks individuais para o workspace

## Mudanças Propostas

### Estrutura do Projeto

Criando uma estrutura nativa do Databricks compatível com Databricks Repos:

```
framework-ingestao-databricks/
├── LEIAME.md                              # Documentação e início rápido
├── setup.py                               # Para distribuição como wheel
├── pyproject.toml                         # Empacotamento Python moderno
├── notebooks/
│   ├── 00_Inicio_Rapido.py               # Guia de início rápido
│   ├── 01_Configuracao_Instalacao.py     # Setup do framework
│   ├── 02_Exemplos_Ingestao_CSV.py       # Exemplos de carga CSV
│   ├── 03_Exemplos_Processamento_Texto.py # Exemplos de UDFs de texto
│   ├── 04_Exemplos_Qualidade_Dados.py    # Validação e checks de qualidade
│   └── 05_Padroes_Avancados.py           # Padrões avançados de integração
├── src/
│   └── framework_ingestao/
│       ├── __init__.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── carregador_base.py        # Classe base abstrata
│       │   ├── unity_catalog.py          # Integração Unity Catalog
│       │   └── configuracao.py           # Gerenciamento de configuração
│       ├── carregadores/
│       │   ├── __init__.py
│       │   ├── carregador_csv.py         # CSV inteligente com auto-detecção
│       │   ├── carregador_delta.py       # Carregador Delta Lake
│       │   ├── carregador_json.py        # Carregador JSON
│       │   └── factory.py                # Auto-detecção e carga
│       ├── processadores/
│       │   ├── __init__.py
│       │   ├── udfs_texto.py             # UDFs de normalização de texto
│       │   ├── validadores.py            # Validação de schema e dados
│       │   ├── qualidade.py              # Métricas de qualidade
│       │   └── transformadores.py        # Transformações comuns
│       ├── escritores/
│       │   ├── __init__.py
│       │   ├── escritor_delta.py         # Escritor Delta com merge
│       │   └── escritor_catalog.py       # Escritor para Unity Catalog
│       └── utils/
│           ├── __init__.py
│           ├── dbutils_helper.py         # Wrapper para utilitários Databricks
│           ├── logging.py                # Logging estruturado
│           ├── encoding.py               # Detecção de encoding
│           └── erros.py                  # Exceções customizadas
├── config/
│   ├── config_padrao.yaml                # Configuração padrão
│   └── regras_qualidade.yaml             # Regras de qualidade
└── tests/
    ├── test_carregador_csv.py
    ├── test_udfs_texto.py
    └── test_unity_catalog.py
```

---

### Componentes Core



Documentação completa incluindo:
- Instalação via Databricks Repos
- Instalação de pacote wheel nos clusters
- Guia de início rápido com exemplos Unity Catalog
- Referência da API
- Boas práticas para Databricks



Utilitários de integração com Unity Catalog:
- Gerenciamento de catalogs e schemas
- Verificação de existência de tabelas
- Resolução de paths de volumes
- Recuperação de metadados



Classe base abstrata para todos os carregadores com otimizações Databricks:
- Interface padrão `carregar()`
- Suporte a paths Unity Catalog
- Compatibilidade com DBFS e Volumes
- Coleta de logs e métricas

---

### Componentes de Carregamento de Dados



Carregador CSV inteligente com auto-detecção:
- Detecção automática de encoding (UTF-8, Latin-1, Windows-1252, etc.)
- Detecção automática de separador (vírgula, ponto e vírgula, tab, pipe)
- Inferência de cabeçalho
- Inferência de schema e coerção de tipos
- Suporte para Volumes (`/Volumes/catalog/schema/volume/`)
- Suporte para paths DBFS



Carregador Delta Lake otimizado para Databricks:
- Carregamento de tabelas Unity Catalog (`catalog.schema.tabela`)
- Suporte a time travel (versão, timestamp)
- Otimização de partition pruning
- Tratamento de evolução de schema


Carregador de arquivos JSON:
- JSON de linha única e multi-linha
- Inferência de schema
- Tratamento de estruturas aninhadas
- Suporte para Volumes e DBFS



Factory de carregadores com auto-detecção:
```python
# Detecta automaticamente o formato e carrega
df = FrameworkIngestao.carregar(spark, "/Volumes/catalog/schema/volume/dados.csv")
df = FrameworkIngestao.carregar(spark, "catalog.schema.tabela")  # Tabela Delta
```

---

### Escritores de Dados



Escritor Delta Lake com padrões avançados:
- Operações de upsert/merge com lógica customizada
- SCD Tipo 2 (Slowly Changing Dimensions)
- Gerenciamento e otimização de partições
- Z-Order clustering
- Override de schema na escrita



Escritor de tabelas Unity Catalog:
- Criação de tabelas gerenciadas e externas
- Criação automática de catalog/schema
- Propriedades e tags de tabelas
- Comentários e metadados de colunas

---

### Componentes de Processamento de Dados


UDFs de processamento de texto:
- `remover_acentos_udf`: Remove acentuação
- `normalizar_texto_udf`: Normalização completa (minúsculas, acentos, espaços)
- `limpar_espacos_udf`: Remove espaços excessivos
- Otimizado para execução distribuída



Utilitários de validação de dados:
- Validação de schema contra schemas esperados
- Verificação de colunas obrigatórias
- Validação de tipos de dados
- Políticas de valores nulos
- Regras de validação customizadas


Métricas e checks de qualidade de dados:
- Completude (contagem de nulos, taxas de preenchimento)
- Detecção de unicidade e duplicados
- Análise de distribuição de valores
- Regras de qualidade customizadas via YAML
- Geração de relatórios de qualidade

---

### Utilitários e Configuração




Wrapper para utilitários Databricks:
- Acesso seguro ao dbutils (funciona em testes locais)
- Helpers para gerenciamento de widgets
- Recuperação de secrets
- Operações de arquivo DBFS


Gerenciamento de configuração:
- Configuração baseada em YAML
- Overrides baseados em ambiente (dev, staging, prod)
- Parâmetros de runtime via widgets
- Regras de qualidade padrão


Logging estruturado para Databricks:
- Logs em formato JSON
- Métricas de performance (linhas processadas, duração)
- Integração com monitoramento Databricks
- Estatísticas de carga e rastreamento de linhagem


Detecção de encoding:
- Detecção baseada em amostra (chardet)
- Estratégia de fallback multi-encoding
- Score de confiança



Exceções customizadas:
- `ErroIngestao`: Exceção base
- `ErroDeteccaoEncoding`: Problemas de encoding
- `ErroValidacaoSchema`: Incompatibilidades de schema
- `ErroQualidadeDados`: Falhas de qualidade
- `ErroUnityCatalog`: Erros relacionados ao Catalog

---

### Notebooks Databricks



Notebook de início rápido:
- Instalação do framework (wheel ou repo)
- Primeiro exemplo de ingestão CSV
- Escrita para Unity Catalog



Guia de configuração:
- Configuração do cluster
- Instalação de bibliotecas
- Pré-requisitos do Unity Catalog
- Setup de configuração



Padrões de ingestão CSV:
- Demonstração de auto-detecção
- Tratamento de diferentes encodings
- Separadores complexos
- Salvamento em tabelas Delta



Processamento de texto com UDFs:
- Remoção de acentos
- Pipelines de normalização de texto
- Transformações em múltiplas colunas



Workflows de qualidade de dados:
- Validação de schema
- Cálculo de métricas de qualidade
- Quality gates e alertas
- Relatórios de qualidade



Padrões avançados:
- Padrões de merge Delta (upsert)
- Implementação SCD Tipo 2
- Ingestão incremental
- Lógica de tratamento de erros e retry
- Integração com Databricks Workflows

## Plano de Verificação

### Testes Automatizados

1. **Testes Unitários**: Testar componentes core localmente
   ```bash
   pytest tests/ -v --cov=src/framework_ingestao
   ```

2. **Testes em Jobs Databricks**: Executar testes no cluster Databricks
   - Criar job de teste com notebook
   - Validar integração Unity Catalog
   - Testar com vários formatos de arquivo

### Verificação Manual no Databricks

1. **Execução de Notebooks**: Executar todos os notebooks de exemplo em sequência
   - Verificar auto-detecção CSV com arquivos de amostra
   - Testar operações de merge Delta
   - Validar checks de qualidade e alertas

2. **Integração Unity Catalog**:
   - Criar catalog/schema de teste
   - Carregar dados para tabelas gerenciadas
   - Verificar metadados e linhagem

3. **Validação de Performance**:
   - Testar com arquivos CSV grandes (>1GB)
   - Medir overhead da auto-detecção
   - Validar performance do merge Delta

4. **Integração com Workflows**:
   - Criar Databricks Workflow de exemplo
   - Testar execuções parametrizadas com widgets
   - Verificar tratamento de erros e retries

### Critérios de Sucesso

- ✅ Todos os notebooks executam com sucesso no workspace Databricks
- ✅ Auto-detecção CSV funciona com múltiplos encodings e separadores
- ✅ UDFs de texto produzem output normalizado correto
- ✅ Dados escritos com sucesso em tabelas Unity Catalog
- ✅ Padrões de merge Delta funcionam corretamente (upsert, SCD2)
- ✅ Checks de qualidade detectam problemas de dados com precisão
- ✅ Framework instalável como wheel no cluster
- ✅ Logging e métricas capturados adequadamente
