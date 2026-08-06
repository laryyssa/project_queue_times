# Project Queue Times — Data Lake de Parques de Diversão 🎡

Data lake pessoal que extrai dados de tempos de fila de parques de diversão a partir de uma API pública, orquestra o pipeline com **Apache Airflow** e organiza os dados seguindo a **arquitetura medalhão** (Bronze → Silver → Gold).

## Sobre o projeto

O objetivo é coletar periodicamente dados de parques (parques, atrações, tempos de espera e status de operação) via API, armazená-los de forma bruta e evoluí-los em camadas cada vez mais limpas e prontas para análise.

## Arquitetura (Medalhão)

```
API de Parques  ─▶  Bronze  ─▶  Silver  ─▶  Gold
                  (raw)      (limpo)    (agregado/analítico)
```

- **Bronze**: dados brutos extraídos da API, salvos como estão (formato `.json`), particionados por data de ingestão. Nenhuma transformação é aplicada — é a fonte de verdade histórica.
- **Silver**: dados limpos, tipados e normalizados (ex.: `explode`/`json_normalize` das listas de parques e atrações, tratamento de nulos, deduplicação).
- **Gold**: dados agregados e modelados para consumo final (ex.: tempo médio de fila por atração/dia, ranking de parques mais lotados), prontos para dashboards ou análises.

## Estrutura de pastas

```
project_queue_times/
├── dags/                      # DAGs do Airflow
│   └── extract_parks_queue_times.py
├── data/
│   ├── bronze/
│   │   └── parks_queue_times/
│   │       └── YYYY/MM/DD/
│   │           └── bronze_YYYY-MM-DD_HH-MM-SS.parquet
│   ├── silver/
│   └── gold/
├── notebooks/                 # notebooks de exploração e validação
├── src/
│   ├── extract/                # scripts de extração da API
│   ├── transform/               # scripts de transformação (bronze→silver→gold)
│   └── utils/
├── venv/                      # ambiente virtual Python (não versionado)
├── requirements.txt
├── .env.example
└── README.md
```

## Tecnologias

- **Python 3.14** 
- **Apache Airflow** — orquestração das DAGs de extração e transformação
- **pandas** — manipulação de dados
- **pyarrow** ou **fastparquet** — leitura/escrita de arquivos Parquet
- **requests** — chamadas HTTP à API de parques


## Como rodar o projeto

### 1. Clonar o repositório

```bash
git clone https://github.com/laryyssa/project_queue_times
cd project_queue_times
```

### 2. Criar e ativar o ambiente virtual

Linux

```bash
python3.12 -m venv venv
source venv/bin/activate        # Linux/Mac
```

### 3. Instalar as dependências

```bash
pip install -r requirements.txt
```


### 4. Configurar variáveis de ambiente 


### 5. Rodar Docker


### 6. Acessar Airflow
