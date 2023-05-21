from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests
import json
from os.path import join
from pathlib import Path

with DAG(
            'teste_ambinbev_dag',
            start_date=days_ago(1),
            schedule_interval='@daily'
) as dag:

        BASE_FOLDER = join(
             str(Path("~/Documents").expanduser()),
             "teste_ABInBev/datalake/{stage}/projeto_teste_abinbev"
        )
        
        SPARK_FOLDER = join(
             str(Path("~/Documents").expanduser()),
             "teste_ABInBev/src/spark"
        )

        criar_pasta = BashOperator(
            task_id = 'cria_pasta',
            bash_command = 'mkdir -p ' + BASE_FOLDER.format(stage="Bronze")
        )

        def extrai_dados_api():
            #URL da API
            url = "https://api.openbrewerydb.org/v1/breweries"

            # Variaveis
            file_path = BASE_FOLDER.format(stage="Bronze") + "/list_breweries.json"


            # Fazer request da API
            response = requests.get(url)
            # Testa se response da API foi OK
            if response.status_code == 200:
                # Converter response em JSON
                dados = response.json()
                # Salva os dados em JSON
                with open(file_path, "w") as file:
                    json.dump(dados, file)
            else:
                print("Erro ao acessar API: ", response.status_code)
        
        extrair_dados = PythonOperator(
            task_id = 'extrair_dados',
            python_callable = extrai_dados_api
        )

        estruturar_dados = SparkSubmitOperator(
             task_id = 'estruturar_dados',
             application = SPARK_FOLDER + "/transformation.py",
             name = "Spark Brewery Transformation",
             application_args = ["--arq_json", BASE_FOLDER.format(stage="Bronze"),
                                 "--arq_parquet", BASE_FOLDER.format(stage="Silver")]

        )

        criar_view = SparkSubmitOperator(
             task_id = 'criar_view',
             application = SPARK_FOLDER + "/aggregated_view.py",
             name = "Spark Brewery Transformation",
             application_args = ["--src", BASE_FOLDER.format(stage="Silver"),
                                 "--dest", BASE_FOLDER.format(stage="Gold")]
        )

        criar_pasta >> extrair_dados >> estruturar_dados >> criar_view