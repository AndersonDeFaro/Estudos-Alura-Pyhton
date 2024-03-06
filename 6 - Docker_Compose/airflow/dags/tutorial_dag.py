from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.bash import BashOperator

import pandas as pd
import requests
import json

def caputra_conta_dados():
    url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len (df.index)
    return qtd

def e_valido(ti):
    qtd = ti.xcom_pull(task_ids = 'caputra_conta_dados')
    if (qtd > 1000):
        return 'valido'
    return 'nvalido'

with DAG('tutorial_dag', start_date = datetime(2021, 12,1),
         schedule_interval = '30 * * * *', catchup = False) as dag:
    
    caputra_conta_dados = PythonOperator(
        task_id = 'caputra_conta_dados',
        python_callable = caputra_conta_dados
    )

    e_valido = BranchPythonOperator (
        task_id = 'e_valida',
        python_callable = e_valido
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )

    nvalido = BashOperator(
        task_id = 'nvalido',
        bash_command = "echo 'Quantidade não OK'"
    )

    caputra_conta_dados >> e_valido >> [valido, nvalido]