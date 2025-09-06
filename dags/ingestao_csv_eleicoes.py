import shutil
import subprocess

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import zipfile
import os
import boto3
from sqlalchemy import create_engine, text
import io

# Estrutura inicial das dags foi gerada com a ajuda do ChatGPT, fiz as alteracoes para deixar da forma que eu queria e evitar codigo redundante e sem objetivo.
# Senhas do MinIO expostas para facilitar pois é um teste, mas o correto seria usar .env com load_env() e ignorar o .env no .gitignore

def download_zip():
    os.makedirs("/tmp", exist_ok=True)
    local_file = "/tmp/resultados-2022.zip"

    if os.path.exists(local_file):
        print("Arquivo já existe, não será baixado novamente.")
        return

    url = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip"
    
    print("Baixando arquivo CSV do TSE...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Download concluído: {local_file}")

def extrai_zip():
    folder = "/tmp/resultados-2022"
    
    if os.path.exists(folder) and os.listdir(folder):
        print(f"Pasta {folder} já existe e contém arquivos. Extração pulada.")
        return
    
    os.makedirs(folder, exist_ok=True)
    with zipfile.ZipFile("/tmp/resultados-2022.zip", "r") as zip_ref:
        print("Extraindo zip ...")
        zip_ref.extractall(folder)
    
    print(f"Zip extraido com sucesso.")

def sobe_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio", 
        aws_secret_access_key="minio123", 
    )

    # Cria o bucket no minio caso nao exista
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if "raw-data" not in buckets:
        s3.create_bucket(Bucket="raw-data")
    
    # Sobe no minio
    for file in os.listdir("/tmp/resultados-2022"):
        if file.endswith(".csv"):
            print(f"Subindo CSV {file} no MinIO ...")
            file_path = os.path.join("/tmp/resultados-2022", file)
            s3.upload_file(file_path, "raw-data", f"{file}")

    print("Arquivos CSV enviados para o MinIO com sucesso.")

# Limpa o zip e a pasta dos arquivos pois sao bem pesados
def limpa_pasta():
    print("Limpando arquivos ...")
    if os.path.exists("/tmp/resultados-2022"):
        print(f"Removendo a pasta {"/tmp/resultados-2022"} ...")
        shutil.rmtree("/tmp/resultados-2022")
    
    if os.path.exists("/tmp/resultados-2022.zip"):
        print(f"Removendo o arquivo {"/tmp/resultados-2022.zip"} ...")
        os.remove("/tmp/resultados-2022.zip")
    
    print("Limpeza concluída!")

# Sobe em chunks pro Postgres pra nao sobrecarregar a memoria e tbm sobe direto do minio pra nao ocupar mais espaco em disco
def carrega_staging():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )

    bucket = "raw-data"
    objects = s3.list_objects_v2(Bucket=bucket)

    # conexão inicial para verificar/criar banco
    engine_master = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/postgres")
    
    db_name = "projeto_eleicao"

    # cria o banco somente se nao existir
    with engine_master.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": db_name}
        ).fetchone()

        if not result:
            conn.execute(text("COMMIT"))
            conn.execute(text(f"CREATE DATABASE {db_name};"))
            print(f"Database {db_name} criado com sucesso.")
        else:
            print(f"Database {db_name} já existe.")

    # Conexão com o banco
    engine = create_engine(
        f"postgresql+psycopg2://airflow:airflow@postgres:5432/{db_name}"
    )

    # le os arquivos do minio
    if "Contents" in objects:
        for obj in objects["Contents"]:
            key = obj["Key"]
            file_name = os.path.basename(key)     
            table_name = os.path.splitext(file_name)[0].lower()

            # Verifica se a tabela já existe, caso ja exista nao vai criar
            with engine.connect() as conn:
                table_exists = conn.execute(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name=:table_name)"
                    ),
                    {"table_name": table_name}
                ).scalar()

            if table_exists:
                print(f"Tabela {table_name} já existe.")
                continue

            print(f"Iniciando carga incremental de {key} na tabela {table_name} ...")

            # usa stream direto, sem carregar tudo em memória
            file_obj = s3.get_object(Bucket=bucket, Key=key)
            stream = io.BytesIO(file_obj["Body"].read())

            # o primeiro chunk e carregado usando to_sql pra criar a tabela e as colunas no banco
            first_chunk = True
            for chunk in pd.read_csv(stream, chunksize=100_000, sep=";", encoding="latin1"):
                if first_chunk:
                    # cria a tabela automaticamente 
                    chunk.to_sql(table_name, engine, if_exists="replace", index=False)
                    first_chunk = False
                else:
                    # Usa COPY que e mais rapido para os chunks seguintes
                    buffer = io.StringIO()
                    chunk.to_csv(buffer, index=False, header=False, sep=";")
                    buffer.seek(0)
                    conn = engine.raw_connection()
                    try:
                        cur = conn.cursor()
                        cur.copy_expert(
                            f"COPY {table_name} FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '')",
                            buffer
                        )
                        conn.commit()
                        cur.close()
                    finally:
                        conn.close()

            print(f"Carga de {key} concluída na tabela {table_name}.")

def dbt_run():
    dbt_project_dir = "/opt/dbt"
    profiles_dir = "/opt/dbt"

    subprocess.run(
        ["dbt", "run", "--profiles-dir", profiles_dir],
        check=True,
        cwd=dbt_project_dir
    )

def dbt_test():
    dbt_project_dir = "/opt/dbt"
    profiles_dir = "/opt/dbt"

    subprocess.run(
        ["dbt", "test", "--profiles-dir", profiles_dir],
        check=True,
        cwd=dbt_project_dir
    )

with DAG(
    dag_id="pipeline_eleicoes_2022",
    start_date=datetime(2025, 9, 4),
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="download_zip",
        python_callable=download_zip,
    )

    t2 = PythonOperator(
        task_id="extrai_zip",
        python_callable=extrai_zip,
    )

    t3 = PythonOperator(
        task_id="sobe_minio",
        python_callable=sobe_minio,
    )

    t4 = PythonOperator(
        task_id="limpa_pasta",
        python_callable=limpa_pasta,
    )

    t5 = PythonOperator(
        task_id="carrega_staging",
        python_callable=carrega_staging,
    )

    t6 = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run,
    )

    t7 = PythonOperator(
        task_id="dbt_test",
        python_callable=dbt_test,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
