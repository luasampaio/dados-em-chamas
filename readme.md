# 🔥 Dados em Chamas

> Segue o canal: https://www.youtube.com/@lucianasampaio.engdados

> **Curso completo de Engenharia de Dados com Databricks**

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)

---

## 📖 Sobre o Curso

Curso completo de **Engenharia de Dados** focado em boas práticas, arquitetura moderna e implementações práticas utilizando o ecossistema Databricks. Cada módulo contém teoria, notebooks práticos e exemplos de código prontos para uso.

## 🎯 Público-Alvo

- Engenheiros de Dados
- Analistas de Dados em transição
- Desenvolvedores interessados em Data Engineering
- Profissionais que querem dominar Databricks

---

## 📚 Trilha de Aprendizado

### 🟢 Nível Básico

| # | Módulo | Descrição | Duração |
|---|--------|-----------|---------|
| 01 | [**Fundamentos**](./aulas/01-fundamentos/) | Arquitetura Medallion, Delta Lake, Unity Catalog | 2-3h |
| 02 | [**Ingestão de Dados**](./aulas/02-ingestao-dados/) | Leitura de arquivos, transformações, validação | 3-4h |

### 🟡 Nível Intermediário

| # | Módulo | Descrição | Duração |
|---|--------|-----------|---------|
| 03 | [**Camada Semântica**](./aulas/03-camada-semantica/) | dbt, métricas, governança de dados | 4-5h |
| 04 | [**Monitoramento**](./aulas/04-monitoramento/) | Self-healing pipelines, alertas, observabilidade | 3-4h |
| 05 | [**Pipelines CI/CD**](./aulas/05-pipelines-ci-cd/) | Azure Pipelines, Asset Bundles, deploy | 3-4h |
| 06 | [**Data Quality**](./aulas/06-data-quality/) | Great Expectations, DLT Expectations, quarentena | 3-4h |

### 🔴 Nível Avançado

| # | Módulo | Descrição | Duração |
|---|--------|-----------|---------|
| 07 | [**Orquestração**](./aulas/07-orquestracao/) | Databricks Workflows, Jobs API, dependências | 3-4h |
| 08 | [**Streaming**](./aulas/08-streaming/) | Structured Streaming, Kafka, Auto Loader | 4-5h |
| 09 | [**Otimização**](./aulas/09-otimizacao/) | Particionamento, Z-Order, Liquid Clustering, Photon | 4-5h |
| 10 | [**Governança**](./aulas/10-governanca/) | Unity Catalog, RLS, Lineage, Audit Logs | 4-5h |

---

## 📁 Estrutura do Repositório

```
dados-em-chamas/
├── 📁 aulas/                         # Conteúdo principal do curso
│   ├── 01-fundamentos/               # Arquitetura e conceitos base
│   ├── 02-ingestao-dados/            # Ingestão e transformação
│   ├── 03-camada-semantica/          # Modelagem semântica
│   ├── 04-monitoramento/             # Observabilidade e alertas
│   ├── 05-pipelines-ci-cd/           # Automação de deploys
│   ├── 06-data-quality/              # Validação de qualidade
│   ├── 07-orquestracao/              # Workflows e scheduling
│   ├── 08-streaming/                 # Processamento em tempo real
│   ├── 09-otimizacao/                # Performance tuning
│   └── 10-governanca/                # Unity Catalog e segurança
├── 📁 assets/                        # Recursos visuais
│   └── images/
├── 📁 docs/                          # Documentação geral
├── 📁 exemplos/                      # Códigos de exemplo
├── 📁 templates/                     # Templates reutilizáveis
├── 📁 recursos/                      # Links e materiais extras
└── 📄 README.md                      # Este arquivo
```

---

## 🚀 Como Começar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/dados-em-chamas.git
cd dados-em-chamas
```

### 2. Escolha seu ponto de partida
- **Iniciante?** Comece pelo [Módulo 01 - Fundamentos](./aulas/01-fundamentos/)
- **Já conhece o básico?** Pule para [Módulo 06 - Data Quality](./aulas/06-data-quality/)
- **Quer avançar?** Explore [Módulo 08 - Streaming](./aulas/08-streaming/)

### 3. Siga a ordem dos notebooks em cada módulo
Cada módulo contém notebooks numerados (01_, 02_, etc.) para seguir uma progressão lógica.

---

## 🛠️ Tecnologias Abordadas

| Categoria | Tecnologias |
|-----------|-------------|
| **Plataforma** | Databricks, Azure, AWS |
| **Processamento** | PySpark, Spark SQL, Photon |
| **Storage** | Delta Lake, Unity Catalog, Volumes |
| **Streaming** | Structured Streaming, Kafka, Auto Loader |
| **Qualidade** | Great Expectations, DLT Expectations |
| **Orquestração** | Databricks Workflows, Jobs API |
| **CI/CD** | Azure Pipelines, Asset Bundles |
| **Modelagem** | dbt, Camada Semântica |

---

## 📊 Indicadores do Curso

| Métrica | Valor |
|---------|-------|
| 📚 Total de Módulos | 10 |
| ⏱️ Carga Horária Total | ~35-45 horas |
| 📓 Notebooks Práticos | 15+ |
| 💻 Exemplos de Código | 50+ |

---

## 🤝 Contribuições

Contribuições são bem-vindas! Se você encontrar erros ou quiser adicionar conteúdo:

1. Faça um Fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-aula`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova aula sobre X'`)
4. Push para a branch (`git push origin feature/nova-aula`)
5. Abra um Pull Request

---

## 📝 Licença

Este projeto é destinado para fins educacionais.

---

<p align="center">
  <strong>🔥 Feito com paixão por dados!</strong>
  <br>
  <sub>Transformando dados em conhecimento, um módulo por vez.</sub>
</p>
