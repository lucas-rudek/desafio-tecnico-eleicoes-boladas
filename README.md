# Projeto Eleições 2022 – Pipeline de Ingestão e Transformação

Este projeto implementa um **pipeline de dados** com **Apache Airflow**, **MinIO**, **PostgreSQL** e **dbt** para ingestão, armazenamento e transformação dos resultados das eleições de 2022 disponibilizados pelo TSE. README gerado pelo Chat GPT.

---
## 📂 Estrutura do Projeto

- **Airflow DAG (`dags/ingestao_csv_eleicoes.py`)**
  - Baixa os arquivos CSV oficiais do TSE (zip).
  - Extrai os arquivos para diretório temporário.
  - Envia os CSVs para o **MinIO** (data lake).
  - Carrega os dados do MinIO em staging no **PostgreSQL** em chunks (eficiente em memória).
  - Executa **dbt run** e **dbt test** para transformação e validação dos dados.

- **dbt/models**
  - **Staging (`stg_votacao.sql`)**  
    Consolida os arquivos em uma única tabela de votos, filtrando apenas cargos de interesse.
  - **Fato (`fct_eleicao.sql`)**  
    Base centralizada com os votos já tratados.
  - **Marts**  
    - `mart_resultado_geral_br.sql`: votos totais para Presidente no Brasil.  
    - `mart_resultado_geral_municipio.sql`: votos por município e cargo.  
    - `mart_resultado_por_estado.sql`: votos por estado para Presidente e Governador.  
  - **Testes (`schema.yml`)**  
    Inclui testes de integridade (valores aceitos, `not_null`, etc.).

- **Configurações**
  - `dbt_project.yml`: Configuração do projeto dbt.  
  - `profiles.yml`: Conexão com PostgreSQL (via usuário `airflow`).  

---

## ⚙️ Tecnologias Utilizadas

- [Apache Airflow](https://airflow.apache.org/) – Orquestração do pipeline
- [MinIO](https://min.io/) – Data Lake S3 compatível
- [PostgreSQL](https://www.postgresql.org/) – Banco de dados relacional
- [dbt](https://www.getdbt.com/) – Modelagem e transformação de dados
- [Pandas](https://pandas.pydata.org/) – Leitura de CSVs em chunks
- [SQLAlchemy](https://www.sqlalchemy.org/) – Conexão com PostgreSQL
- [Boto3](https://boto3.amazonaws.com/) – Integração com MinIO/S3

---

## 📊 Fluxo do Pipeline

1. **Download** → Obtém o zip do TSE.  
2. **Extração** → Extrai os CSVs para `/tmp`.  
3. **Upload** → Envia os CSVs para o bucket `raw-data` no MinIO.  
4. **Limpeza** → Remove arquivos temporários.  
5. **Carga Staging** → Cria banco `projeto_eleicao` (se não existir) e carrega os CSVs em tabelas no PostgreSQL.  
6. **Transformação** → Executa `dbt run` para consolidar os dados.  
7. **Testes** → Executa `dbt test` para validar consistência dos dados.  

---

## 🚀 Como Executar

1. Suba os serviços (Airflow, MinIO, Postgres, dbt) via **Docker Compose**.  
2. Acesse o Airflow UI (admin:admin) e rode a DAG `pipeline_eleicoes_2022`.  
3. No Linux, pode ser necessário dar permissao para o user do Airflow na pasta. (`sudo chown -R 50000:0 dags logs plugins dbt minio_data`)
3. Após execução:  
   - Dados crus estarão no MinIO (`raw-data`).  
   - Views de staging, fato e marts analíticos estarão no Postgres (`projeto_eleicao`).  

---

## 📈 Exemplos de Consultas

- **Resultado geral (Presidente no Brasil):**
  ```sql
  SELECT * FROM mart_resultado_geral_br;
