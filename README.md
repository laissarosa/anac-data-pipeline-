# 🛫 VoeBem — Pipeline de Dados da Aviação Civil Brasileira (ANAC)

> Pipeline completo de engenharia de dados construído sobre dados abertos da **ANAC** (Agência Nacional de Aviação Civil), orquestrado no **Databricks** com arquitetura **Medallion** (Bronze → Silver → Gold), ingestion automatizada, camada de Data Quality com expectations e modelo dimensional pronto para consumo analítico.

---

## 📋 Visão Geral

O projeto ingere, trata e modela dados públicos da ANAC sobre o transporte aéreo brasileiro, especificamente:

* **VRA (Voo Regular Ativo)** — registros de todos os voos regulares operados no Brasil (19 meses: jan/2025 a jul/2026, ~1,6M de registros)
* **Empresas Aéreas** — cadastro nacional e estrangeiro de operadores aéreos
* **Aeródromos Públicos** — cadastro de aeroportos com coordenadas geográficas
* **Códigos de Operação (DI)** — tabela de referência para tipo de etapa de voo

O pipeline é **idempotente** (pode ser re-executado sem duplicar dados) e **incremental** (detecta arquivos novos ou modificados via metadados HTTP).

---

## 🏗️ Arquitetura

```
Portal ANAC (Dados Abertos)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  INGESTÃO AUTOMATIZADA (Notebook Python)             │
  Web scraping (BeautifulSoup) + HEAD requests          │
  Download com hash SHA-256 → UC Volumes                 │
  Tabela de controle (Delta) com versionamento          │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE — Dado bruto, sem descarte                    │
│  Leitura CSV (Spark) → Delta Tables no UC              │
│  Rastreabilidade: _arquivo_origem + _ingerido_em      │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  DATA QUALITY — Contrato de Dados (SQL / SDP)          │
│  Temporary View marcada → Materialized View auditada   │
│  9 expectations (warn mode) + quarentena               │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  SILVER — Dado limpo, tipado e enriquecido             │
│  Cast de tipos, limpeza de strings 'null',            │
│  métricas derivadas (atraso, pontualidade,            │
│  minutos recuperados) + união de cadastros             │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  GOLD — Modelo dimensional prontíssimo para consumo    │
│  Fato: fct_voos (deduplicada + enriquecida)            │
│  Dimensões: dim_empresas, dim_aerodromos              │
│  Referência: codigos_operacao                          │
│  Liquid Clustering + documentação completa + Tags UC   │
└─────────────────────────────────────────────────────┘
```

---

## 📁 Estrutura do Projeto

```
anac-data-pipeline-
├── voebem/                              # Pipeline principal (Medallion)
│   ├── voebem_ingestao_automatica       # Ingestão automatizada da ANAC
│   ├── bronze_vra                       # Carga Bronze — VRA (voos)
│   ├── bronze_referencia                # Carga Bronze — Empresas + Aeródromos
│   ├── silver_espelho                   # Transformações Silver (tipagem + limpeza + uniões)
│   └── gold                             # Modelo dimensional Gold (fato + dimensões)
│
├── voebem-qualidade-silver/             # Camada de Data Quality (Spark Declarative Pipeline)
│   └── transformations/
│       ├── 01_vra_marcado.sql            # Temporary view — flags de integridade referencial + atraso
│       ├── 02_vra_auditado.sql          # Materialized View com 9 constraints (expectations)
│       └── 03_vra_quarentena.sql         # Materialized View de quarentena (registros reprovados + motivo)
│
└── README.md
```

---

## 🔄 Detalhamento das Camadas

### 1. Ingestão Automatizada (`voebem_ingestao_automatica`)

Notebook Python que automatiza a coleta de arquivos do portal de dados abertos da ANAC:

* **Descoberta dinâmica** via web scraping (`BeautifulSoup`) das páginas de diretório (Apache `Index of`)
* **Ingestão seletiva** por dataset — VRA (filtragem por ano), Empresas e Aeródromos
* **Idempotência** via tabela de controle Delta (`anac_ingestao_controle`) com versionamento por `content_length`, `last_modified` e `etag`
* **HEAD requests** para detectar mudanças sem baixar o arquivo inteiro
* **Download com streaming** e cálculo de **SHA-256** para integridade
* Armazenamento em **Unity Catalog Volumes** (`/Volumes/voebem/bronze/arquivos/`)

### 2. Bronze — Dado Bruto (`bronze_vra`, `bronze_referencia`)

* Leitura de CSVs com Spark (`spark.read.format("csv")`), modo `PERMISSIVE` (nada é descartado)
* Renomeação de colunas para snake_case em português
* Adição de metadados de rastreabilidade: `_arquivo_origem` e `_ingerido_em`
* Escrita como **Delta Tables** no Unity Catalog (`voebem.bronze.*`)
* Encoding: UTF-8 e ISO-8859-1 (aeródromos); separador `;` com skip de linha de header duplicado

**Tabelas criadas:**

| Tabela | Descrição | Linhas |
|--------|-----------|--------|
| `voebem.bronze.vra` | Voos do VRA (19 meses) | ~1.597.255 |
| `voebem.bronze.aerodromos` | Aeródromos públicos | 496 |
| `voebem.bronze.empresas_aereas_nacionais` | Empresas nacionais | 729 |
| `voebem.bronze.empresas_aereas_estrangeiras` | Empresas estrangeiras | 150 |
| `voebem.bronze.codigos_operacao` | Códigos DI + tipo de linha | 13 |

### 3. Data Quality — Contrato de Dados (`voebem-qualidade-silver/`)

Pipeline de Data Quality usando **Spark Declarative Pipelines (SDP)** com 3 transformações SQL:

**Passo 1 — `01_vra_marcado.sql`**: Temporary View que adiciona flags de integridade referencial (origem, destino e empresa no cadastro ANAC) e calcula atrasos de partida e chegada em minutos.

**Passo 2 — `02_vra_auditado.sql`**: Materialized View com **9 expectations** em modo `warn` (medem qualidade sem descartar linhas):

| Categoria | Expectation | Descrição |
|-----------|-------------|-----------|
| Completude | `horarios_previstos_presentes` | Partida e chegada prevista não nulas |
| Completude | `situacao_voo_conhecida` | Situação ∈ {REALIZADO, CANCELADO} |
| Coerência temporal | `chegada_prevista_depois_da_partida_prevista` | Ordem cronológica respeitada |
| Coerência temporal | `chegada_real_depois_da_partida_real` | Ordem cronológica respeitada |
| Plausibilidade | `atraso_partida_plausivel` | Atraso entre -120 e +1440 min |
| Plausibilidade | `atraso_chegada_plausivel` | Atraso entre -120 e +1440 min |
| Integridade referencial | `empresa_no_cadastro_anac` | Empresa existe no cadastro |
| Integridade referencial | `aeroporto_origem_no_cadastro_anac` | Origem existe no cadastro |
| Integridade referencial | `aeroporto_destino_no_cadastro_anac` | Destino existe no cadastro |

**Passo 3 — `03_vra_quarentena.sql`**: Materialized View de quarentena que espelha os registros que reprovaram em alguma expectation, com o **motivo específico** por registro. A silver mantém a contagem original — a decisão de excluir é de negócio (na Gold).

### 4. Silver — Dado Limpo (`silver_espelho`)

* **Tipagem**: `try_cast` de strings para `TIMESTAMP`, tratamento de strings `'null'` com `NULLIF`
* **Métricas derivadas**: `atraso_partida_min`, `atraso_chegada_min`, `minutos_recuperados` (diferença entre atraso de partida e chegada — indica se o voo recuperou tempo no ar)
* **Decomposição temporal**: separação de data e hora (`partida_prevista_data`, `partida_prevista_hora`)
* **Unificação de cadastros**: `UNION ALL` de empresas nacionais e estrangeiras em `voebem.silver.empresas` com coluna `origem_cadastro`
* **Conversão de coordenadas**: latitude/longitude de DMS para decimal
* **Validação**: verificação de contagem Bronze vs Silver (zero perda de linhas)

**Tabelas criadas:**

| Tabela | Descrição | Linhas |
|--------|-----------|--------|
| `voebem.silver.vra` | Voos tipados e com métricas | ~1.597.255 |
| `voebem.silver.empresas` | Empresas unificadas (nacional + estrangeira) | 879 |
| `voebem.silver.aerodromos` | Aeródromos com coordenadas decimais | 496 |
| `voebem.silver.codigos_operacao` | Códigos de referência | 13 |

### 5. Gold — Modelo Dimensional (`gold`)

Modelagem dimensional completa, pronta para consumo analítico:

| Tabela | Tipo | Granularidade | Linhas | Descrição |
|--------|------|---------------|--------|-----------|
| `fct_voos` | Fato | etapa de voo | ~1.596.861 | Voos deduplicados + enriquecidos + flags de qualidade |
| `dim_empresas` | Dimensão | empresa | 169 | Apenas operadores com código ICAO (que aparecem no VRA) |
| `dim_aerodromos` | Dimensão | aeródromo | 496 | Aeródromos com coordenadas decimais |
| `codigos_operacao` | Referência | código | 13 | Códigos DI + tipo de linha |

**Tratamentos na Gold:**

* **Deduplicação**: `ROW_NUMBER()` por `(empresa, voo, data, origem, destino)` mantendo o registro mais recente
* **Enriquecimento**: `LEFT JOIN` com dimensões (preserva voos sem match — ~0,2% sem empresa, ~10% sem aeroporto estrangeiro)
* **Flags de qualidade**: `tem_prevista`, `tem_partida_real`, `tem_chegada_real`, `atraso_suspeito`, `recuperou_tempo`
* **Métricas de pontualidade**: `pontualidade_partida`, `pontualidade_chegada` (limiar de 15 min)
* **Liquid Clustering** em `partida_prevista_data` para queries por data + `OPTIMIZE`
* **Documentação completa**: comentários em 73 colunas (10 + 17 + 4 + 42) + comentários de tabela + **Tags UC** (4 tags por tabela: domínio, camada, grão, fonte)

---

## 🛠️ Skills & Ferramentas

### Linguagens

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)

### Plataforma & Cloud

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white)

### Databricks — Funcionalidades Utilizadas

| Funcionalidade | Aplicação no Projeto |
|----------------|---------------------|
| **Unity Catalog** | Governança de dados — catálogos, schemas, tabelas, volumes e tags |
| **UC Volumes** | Armazenamento de arquivos CSV brutos da ANAC |
| **Delta Lake** | Camada de armazenamento transacional ACID em todas as tabelas |
| **Spark Declarative Pipelines (SDP)** | Pipeline de Data Quality com expectations e materialized views |
| **Liquid Clustering** | Otimização de clustering na fact table (substitui partitioning manual) |
| **Databricks Notebooks** | Orquestração de todo o pipeline em notebooks Python + SQL |
| **Serverless Compute** | Execução sem gestão de clusters |

### Bibliotecas Python

| Biblioteca | Aplicação |
|-----------|-----------|
| **PySpark** | Leitura, transformação e escrita de dados em escala distribuída |
| **Requests** | Download de arquivos via HTTP (com streaming) |
| **BeautifulSoup** | Web scraping do portal de dados abertos da ANAC |
| **hashlib** | Cálculo de SHA-256 para integridade de arquivos |
| **re** | Regex para descoberta e filtragem de arquivos por padrão |

### SQL Avançado

| Técnica SQL | Aplicação |
|------------|-----------|
| **CTE (WITH)** | Transformações em cadeia com legibilidade e performance |
| **Window Functions** | `ROW_NUMBER()` para deduplicação e controle de versão |
| **TRY_CAST + NULLIF** | Tipagem segura de timestamps com tratamento de strings `'null'` |
| **MATERIALIZED VIEW** | Views materializadas com expectations no pipeline de qualidade |
| **CONSTRAINT EXPECT** | 9 regras de qualidade de dados (completude, coerência, plausibilidade, integridade) |
| **CLUSTER BY** | Liquid Clustering para otimização de queries |
| **COMMENT ON** | Documentação de tabelas e colunas no Unity Catalog |

---

## 📊 Resultados

* **1.597.255** registros de voos processados (19 meses)
* **6** tabelas Bronze + **4** tabelas Silver + **4** tabelas Gold
* **9** regras de Data Quality com tracking de quarentena
* **394** registros deduplicados na Gold (0,02%)
* **73** colunas documentadas + **16** tags de governança
* Pipeline **idempotente** e **incremental** — pronto para automação contínua

---

## 🚀 Como Reproduzir

1. Criar catálogo e schemas no Unity Catalog: `voebem.bronze`, `voebem.silver`, `voebem.gold`
2. Criar volume `voebem.bronze.arquivos` no UC
3. Executar o notebook `voebem_ingestao_automatica` para baixar os arquivos da ANAC
4. Executar `bronze_vra` e `bronze_referencia` para carregar a Bronze
5. Executar o pipeline SDP em `voebem-qualidade-silver/transformations/` para Data Quality
6. Executar `silver_espelho` para transformações da Silver
7. Executar `gold` para criar o modelo dimensional final

---

## 📫 Contato
e-mail: laissa.tech@gmail.com
instagram: laissa.tech
**Autor:** Laissa Rosa, durante a Imersão em Engenharia de Dados na Alura. Projeto desenvolvido para portfólio de Engenharia de Dados.

---

> _Dados públicos da ANAC — Agência Nacional de Aviação Civil. Disponíveis em: https://sistemas.anac.gov.br/dadosabertos/_" 

