
import requests

import pandas as pd

import numpy as np

import pymysql

import boto3

import json

import os

import oci

import matplotlib.pyplot as plt

from io import StringIO

from io import BytesIO

from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine

from airflow.models import TaskInstance

from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults



# Definindo a chave de API

api_key = "iJC18iePTVhpv_2J58jPlBWJR8xVXWnS"

base_url = "https://api.polygon.io/v2/aggs/ticker"

start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

end_date = datetime.now().strftime('%Y-%m-%d')



# DAG Configuração

default_args = {

    'owner': 'Antonio_Silva',

    'depends_on_past': False,

    'start_date': datetime(2022, 1, 1),

    'email_on_failure': False,

    'email_on_retry': False,

    'retries': 1,

    'retry_delay': timedelta(seconds=5),

}



dag = DAG(

    'aoci_mercado_de_acoes',

    default_args=default_args,

    description='DAG para conectar à API da Polygon.io, coletar dados e armazenar na Oracle Cloud Object Storage',

    schedule=timedelta(minutes=1),

)



# Criando instâncias de tarefas para diferentes empresas

empresas = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'CSCO']



# Tarefa 1: Conexão com a API

def conectar_polygon_api(symbol='AAPL', **kwargs):

    url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"



    # Lógica para obter dados da API

    response = requests.get(url)

    data = response.json()

    df = pd.DataFrame(data['results'])



    ti = kwargs['ti']

    ti.xcom_push(key=f'conectar_api_{symbol}', value=df)  # Armazenar no XCom



    print(f'Dados obtidos e armazenados para {symbol}. DataFrame head:')

    print(df.head())



    return df  # Garantir que a função retorne o DataFrame



# Lista para armazenar as instâncias das tarefas

tarefas_conectar_api = []



for empresa in empresas:

    task_id = f'conectar_api_{empresa}'

    conectar_api_task = PythonOperator(

        task_id=task_id,

        python_callable=conectar_polygon_api,

        op_kwargs={'symbol': empresa},

        dag=dag,

    )

    tarefas_conectar_api.append(conectar_api_task)  # Adicionando à lista



# Tarefa 2: Coleta de Dados

def coletar_dados(**kwargs):

    ti = kwargs['ti']



    # Lista para armazenar DataFrames de cada empresa

    dados_por_empresa = []



    for empresa in empresas:

        # Obtém o DataFrame da XCom usando o task_id específico para cada empresa

        df = ti.xcom_pull(task_ids=f'conectar_api_{empresa}')



        if not df.empty:

            print(f'Dados coletados para {empresa}:\n{df.head()}')

            dados_por_empresa.append((empresa, df))  # Adiciona à lista



    return dados_por_empresa



coletar_dados_task = PythonOperator(

    task_id='coletar_dados',

    python_callable=coletar_dados,

    dag=dag,

)



# Tarefa 3: Armazenar na Oracle Cloud

def armazenar_na_oracle_cloud(**kwargs):

    # Obtém a lista de DataFrames normalizados da Tarefa 2

    dados_por_empresa = kwargs['ti'].xcom_pull(task_ids='coletar_dados')



    # Conecta-se ao serviço Object Storage da Oracle Cloud

    try:

        config = {

            'user': 'ocid1.user.oc1..aaaaaaaafonjbgfvlyhnbfen7ouzlfwodbq657z6bof44r47vta4sewhncwq',

            'key_file': '/usr/local/lib/python3.10/dist-packages/oci/config/eddie.silva_6@hotmail.com_2024-02-15T04 32 39.350Z.pem',

            'fingerprint': 'f9:87:f2:05:af:3a:2a:85:d4:b4:c7:06:51:24:3e:d8',

            'tenancy': 'ocid1.tenancy.oc1..aaaaaaaatxqv5l7ozoimucnw3kxbf25oycmqsyi6mmq5xvkvismirqggxa2a',

            'region': 'sa-saopaulo-1'

        }

        object_storage = oci.object_storage.ObjectStorageClient(config)



    except Exception as e:

        print(f"Erro ao conectar ao Oracle Cloud Object Storage: {e}")

        raise



    # Adiciona lógica de salvamento no Oracle Cloud Object Storage para cada empresa

    for empresa, df in dados_por_empresa:

        if not df.empty:

            try:

                # Converte o DataFrame para CSV

                buffer = BytesIO()

                df.to_csv(buffer, index=False)

                buffer.seek(0)



                # Faça o upload do conteúdo para o bucket na Oracle Cloud Object Storage

                object_storage.put_object(

                    namespace_name='grikosntyvsm',

                    bucket_name='mercado_de_acoes_analise',

                    object_name=f'coletar-dados-api/{empresa}_data_api.csv',

                    put_object_body=buffer.read()

                )



                print(f'Dados da empresa {empresa} armazenado na Oracle Cloud Object Storage.')

            except Exception as e:

                print(f"Erro ao salvar dados da empresa {empresa} na Oracle Cloud Object Storage: {e}")

        else:

            print(f'Empresa {empresa}: Nenhum dado para salvar.')



    # Se não houver dados, imprime uma mensagem

    if not dados_por_empresa:

        print("Nenhum dado disponível para salvar. Verifique se as tarefas anteriores foram concluídas corretamente.")



# Atualizando a tarefa

armazenar_oracle_cloud_task = PythonOperator(

    task_id='armazenar_oracle_cloud_infrastructure',

    python_callable=armazenar_na_oracle_cloud,

    dag=dag,

)



# Tarefa 4: Normalizar e Limpar Dados

def normalizar_limpar_dados(**kwargs):

    ti = kwargs['ti']



    # Lista para armazenar DataFrames de cada empresa

    dados_por_empresa = []



    for empresa, df in ti.xcom_pull(task_ids='coletar_dados'):

        # Adiciona lógica de normalização e limpeza de dados aqui

        # Exemplo de normalização e limpeza

        df = df.rename(columns={

            'v': 'Volume',

            'vw': 'Volume Weighted Average Price',

            'o': 'Open',

            'c': 'Close',

            'h': 'High',

            'l': 'Low',

            't': 'Timestamp',

            'n': 'Number of Trades'

        })



        # Conversão da coluna 'Timestamp' para datetime

        df['Timestamp'] = pd.to_datetime(df['Timestamp'])



        print(f'Dados normalizados e limpos para a empresa {empresa}:\n{df.head()}')



        # Adiciona à lista dados_por_empresa

        dados_por_empresa.append((empresa, df))



    # Você pode adicionar mais lógica de processamento aqui, se necessário



    return dados_por_empresa



normalizar_limpar_dados_task = PythonOperator(

    task_id='normalizar_limpar_dados',

    python_callable=normalizar_limpar_dados,

    dag=dag,

)


# Função para calcular o RSI (Índice de Força Relativa)

def calcular_rsi(prices, n=14):

    deltas = np.diff(prices)

    seed = deltas[:n + 1]

    up = seed[seed >= 0].sum() / n

    down = -seed[seed < 0].sum() / n

    rs = up / down

    rsi = np.zeros_like(prices)

    rsi[:n] = 100. - 100. / (1. + rs)



    for i in range(n, len(prices)):

        delta = deltas[i - 1]

        if delta > 0:

            upval = delta

            downval = 0.

        else:

            upval = 0.

            downval = -delta



        up = (up * (n - 1) + upval) / n

        down = (down * (n - 1) + downval) / n



        rs = up / down

        rsi[i] = 100. - 100. / (1. + rs)



    return rsi



def calcular_bollinger_bands(prices, window=20):

    rolling_mean = prices.rolling(window=window).mean()

    rolling_std = prices.rolling(window=window).std()

    upper_band = rolling_mean + (2 * rolling_std)

    lower_band = rolling_mean - (2 * rolling_std)

    return upper_band, lower_band



# Tarefa 5: Cálculos

def calcular_indicadores(**kwargs):

    ti = kwargs['ti']



    # Obtém a lista de DataFrames da Tarefa 3 normalizar e limpar dados

    dados_por_empresa = ti.xcom_pull(task_ids='normalizar_limpar_dados')



    if dados_por_empresa:

        # Lista para armazenar os indicadores calculados

        indicadores_por_empresa = []



        # Adicione aqui a lógica de cálculos de indicadores para cada empresa

        for empresa, df in dados_por_empresa:

            # Calcula a média móvel

            df['media_movel'] = df['Close'].rolling(window=20).mean()



            # Calcula o RSI

            df['rsi'] = calcular_rsi(df['Close'])



            # Calcula as bandas de Bollinger

            upper_band, lower_band = calcular_bollinger_bands(df['Close'], window=20)

            df['banda_superior'] = upper_band

            df['banda_inferior'] = lower_band



            # Armazena os indicadores no XCom

            indicadores_por_empresa.append((empresa, df))



            print(f'Indicadores calculados para a empresa {empresa}:\n{df.head()}')



        # Armazena os indicadores no XCom

        ti.xcom_push(key='indicadores_por_empresa', value=indicadores_por_empresa)



        # Você pode adicionar mais lógica de processamento aqui, se necessário

    else:

        print("Nenhum dado coletado. Verifique se as tarefas anteriores foram concluídas corretamente.")



calcular_indicadores_task = PythonOperator(

    task_id='calcular_indicadores',

    python_callable=calcular_indicadores,

    dag=dag,

)


# Tarefa 6: Execução de Análises

def executar_analises(**kwargs):

    ti = kwargs['ti']



    # Obtém os dados normalizados e limpos da tarefa anterior

    dados_por_empresa = ti.xcom_pull(task_ids='normalizar_limpar_dados', include_prior_dates=True)



    # Verifica se há dados

    if dados_por_empresa is None:

        raise ValueError("Nenhum dado normalizado e limpo encontrado.")



    resultados = []

    for empresa, dataframe in dados_por_empresa:

        # Estatísticas descritivas

        desc_stats = dataframe.describe()



        # Tendências temporais

        trend_analysis = dataframe.groupby(dataframe['Timestamp'].dt.year).mean()



        # Comparação de volume de negociação

        volume_comparison = dataframe.groupby(dataframe['Timestamp'].dt.year)['Volume'].sum()



        # Correlação entre variáveis

        correlation = dataframe.corr()



        # Análise de outliers

        outlier_days = dataframe[dataframe['Volume'] > dataframe['Volume'].mean() + 2 * dataframe['Volume'].std()]



        # Adiciona os resultados para cada empresa em uma lista

        resultados.append({

            'empresa': empresa,

            'desc_stats': json.loads(desc_stats.to_json()),  # Converte o DataFrame em um dicionário

            'trend_analysis': json.loads(trend_analysis.to_json()),  # Converte o DataFrame em um dicionário

            'volume_comparison': json.loads(volume_comparison.to_json()),  # Converte a Series em um dicionário

            'correlation': json.loads(correlation.to_json()),  # Converte o DataFrame em um dicionário

            'outlier_days': json.loads(outlier_days.to_json(orient='records'))  # Converte o DataFrame em uma lista de dicionários

        })



    # Converte os objetos Timestamp em strings

    resultados = converter_timestamps(resultados)



    # Salvando os resultados na Xcom

    ti.xcom_push(key='analises_resultados', value=resultados)



    print("Análises estatísticas concluídas.")



# Função auxiliar para converter os objetos Timestamp em strings

def converter_timestamps(resultados):

    # Cria uma cópia dos resultados para não alterar os originais

    resultados_convertidos = resultados.copy()

    # Itera sobre os resultados e verifica se há objetos Timestamp

    for resultado in resultados_convertidos:

        for key, value in resultado.items():

            if isinstance(value, pd.Timestamp):

                # Converte o objeto Timestamp em uma string no formato '%Y-%m-%d %H:%M:%S'

                print(f"Convertendo objeto Timestamp em string: {value}")

                resultado[key] = value.strftime('%Y-%m-%d %H:%M:%S')

                print(f"Resultado da conversão: {resultado[key]}")

            elif isinstance(value, dict):  # Se o valor for um dicionário, verifica os itens internos

                for sub_key, sub_value in value.items():

                    if isinstance(sub_value, pd.Timestamp):

                        # Converte o objeto Timestamp em uma string no formato '%Y-%m-%d %H:%M:%S'

                        print(f"Convertendo objeto Timestamp em string: {sub_value}")

                        resultado[key][sub_key] = sub_value.strftime('%Y-%m-%d %H:%M:%S')

                        print(f"Resultado da conversão: {resultado[key][sub_key]}")



    # Retorna os resultados convertidos

    return resultados_convertidos



executar_analises_task = PythonOperator(

    task_id='executar_analises',

    python_callable=executar_analises,

    dag=dag,

)



# Tarefa 7: Construção de Visualizações

def construir_visualizacoes(**kwargs):

    # Obtém os resultados das análises da tarefa 6

    resultados_analises = kwargs['ti'].xcom_pull(task_ids='executar_analises')



    # Verifica se há resultados

    if resultados_analises is None:

        raise ValueError("Nenhum resultado de análise encontrado.")



    # Construa as visualizações aqui

    for resultado in resultados_analises:

        empresa = resultado['empresa']

        desc_stats = resultado['desc_stats']



        # Exemplo de visualização: histograma dos preços de fechamento

        plt.figure(figsize=(10, 6))

        plt.hist(desc_stats['Close']['mean'])

        plt.title(f'Histograma dos preços de fechamento para {empresa}')

        plt.xlabel('Preço de fechamento')

        plt.ylabel('Frequência')

        plt.savefig(f'/path/to/visualizacoes/{empresa}_histograma.png')  # Salva a visualização como um arquivo PNG

        plt.close()



    # Adicione mais visualizações conforme necessário



construir_visualizacoes_task = PythonOperator(

    task_id='construir_visualizacoes',

    python_callable=construir_visualizacoes,

    dag=dag,

)



# Tarefa 8: Armazenamento das Análises e Visualizações na OCI

def armazenar_analises_visualizacoes(**kwargs):

    # Obtém a lista de DataFrames da Tarefa 4, 5 e 6
    
    dados_normalizados = kwargs['ti'].xcom_pull(task_ids='normalizar_limpar_dados')

    dados_calculos = kwargs['ti'].xcom_pull(task_ids='calcular_indicadores')

    dados_analises = kwargs['ti'].xcom_pull(task_ids='executar_analises')



    # Conecta-se ao serviço Object Storage da Oracle Cloud

    try:

        config = {

            'user': 'ocid1.user.oc1..aaaaaaaafonjbgfvlyhnbfen7ouzlfwodbq657z6bof44r47vta4sewhncwq',

            'key_file': '/usr/local/lib/python3.10/dist-packages/oci/config/eddie.silva_6@hotmail.com_2024-02-15T04 32 39.350Z.pem',

            'fingerprint': 'f9:87:f2:05:af:3a:2a:85:d4:b4:c7:06:51:24:3e:d8',

            'tenancy': 'ocid1.tenancy.oc1..aaaaaaaatxqv5l7ozoimucnw3kxbf25oycmqsyi6mmq5xvkvismirqggxa2a',

            'region': 'sa-saopaulo-1'

        }

        object_storage = oci.object_storage.ObjectStorageClient(config)



    except Exception as e:

        print(f"Erro ao conectar ao Oracle Cloud Object Storage: {e}")

        raise



    # Salvar dados normalizados

    for empresa, df in dados_normalizados:

        if not df.empty:

            try:

                # Converte o DataFrame para CSV

                buffer = BytesIO()

                df.to_csv(buffer, index=False)

                buffer.seek(0)



                # Faça o upload do conteúdo para o bucket na Oracle Cloud Object Storage

                object_storage.put_object(

                    namespace_name='grikosntyvsm',

                    bucket_name='mercado_de_acoes_analise',

                    object_name=f'dados-limpos/{empresa}_dados_limpos.csv',

                    put_object_body=buffer.read()

                )



                print(f'Dados normalizados da empresa {empresa} armazenados na Oracle Cloud Object Storage.')

            except Exception as e:

                print(f"Erro ao salvar dados normalizados da empresa {empresa}: {e}")

        else:

            print(f'Empresa {empresa}: Nenhum dado normalizado para salvar.')



    # Salvar dados de cálculos

    for empresa, df in dados_calculos:

        if not df.empty:

            try:

                # Converte o DataFrame para CSV

                buffer = BytesIO()

                df.to_csv(buffer, index=False)

                buffer.seek(0)



                # Faça o upload do conteúdo para o bucket na Oracle Cloud Object Storage

                object_storage.put_object(

                    namespace_name='grikosntyvsm',

                    bucket_name='mercado_de_acoes_analise',

                    object_name=f'calculos-indicadores/{empresa}_calculos_indicadores.csv',

                    put_object_body=buffer.read()

                )



                print(f'Dados de cálculos da empresa {empresa} armazenados na Oracle Cloud Object Storage.')

            except Exception as e:

                print(f"Erro ao salvar dados de cálculos da empresa {empresa}: {e}")

        else:

            print(f'Empresa {empresa}: Nenhum dado de cálculo para salvar.')



    # Salvar dados de análises

    for empresa, df in dados_analises:

        if not df.empty:

            try:

                # Converte o DataFrame para CSV

                buffer = BytesIO()

                df.to_csv(buffer, index=False)

                buffer.seek(0)



                # Faça o upload do conteúdo para o bucket na Oracle Cloud Object Storage

                object_storage.put_object(

                    namespace_name='grikosntyvsm',

                    bucket_name='mercado_de_acoes_analise',

                    object_name=f'analises/{empresa}_analises.csv',

                    put_object_body=buffer.read()

                )



                print(f'Dados de análises da empresa {empresa} armazenados na Oracle Cloud Object Storage.')

            except Exception as e:

                print(f"Erro ao salvar dados de análises da empresa {empresa}: {e}")

        else:

            print(f'Empresa {empresa}: Nenhum dado de análise para salvar.')



    # Criar e salvar visualizações

    for empresa, df in dados_analises:

        if not df.empty:

            try:

                # Exemplo de visualização: histograma dos preços de fechamento

                plt.figure(figsize=(10, 6))

                plt.hist(df['Close'])

                plt.title(f'Histograma dos preços de fechamento para {empresa}')

                plt.xlabel('Preço de fechamento')

                plt.ylabel('Frequência')

                plt.savefig(f'/path/to/visualizacoes/{empresa}_histograma.png')  # Salva a visualização como um arquivo PNG

                plt.close()



                print(f'Visualização para empresa {empresa} criada e armazenada na Oracle Cloud Object Storage.')

            except Exception as e:

                print(f"Erro ao criar e salvar visualização para empresa {empresa}: {e}")

        else:

            print(f'Empresa {empresa}: Nenhum dado disponível para criar visualização.')



    # Se não houver dados, imprime uma mensagem

    if not dados_normalizados and not dados_calculos and not dados_analises:

        print("Nenhum dado disponível para salvar. Verifique se as tarefas anteriores foram concluídas corretamente.")



# Atualizando a tarefa

armazenar_analises_visualizacoes_task = PythonOperator(

    task_id='resultados_analises_visualizacoes_oci',

    python_callable=armazenar_analises_visualizacoes,

    dag=dag,

)



# Tarefa 9: Relatórios Automatizados com Airflow

def gerar_relatorios_automatizados(**kwargs):

    # Adicione aqui a lógica para gerar relatórios automatizados com o Airflow

    print("Gerando relatórios automatizados com o Airflow")



gerar_relatorios_automatizados_task = PythonOperator(

    task_id='gerar_relatorios_automatizados',

    python_callable=gerar_relatorios_automatizados,

    dag=dag,

)



# Tarefa 10: Monitoramento de Execução das DAGs

def monitorar_execucao(**kwargs):

    # Adicione aqui a lógica para monitorar a execução das DAGs

    print("Monitorando execução das DAGs")



monitorar_execucao_task = PythonOperator(

    task_id='monitorar_execucao',

    python_callable=monitorar_execucao,

    dag=dag,

)



# Tarefa 11: Análise de Logs

def analisar_logs(**kwargs):

    # Adicione aqui a lógica para análise de logs

    print("Analisando logs")



analisar_logs_task = PythonOperator(

    task_id='analisar_logs',

    python_callable=analisar_logs,

    dag=dag,

)





# Definindo dependências entre as tarefas



(tarefas_conectar_api >> coletar_dados_task >> armazenar_oracle_cloud_task >> normalizar_limpar_dados_task

 >>calcular_indicadores_task >> executar_analises_task >> construir_visualizacoes_task >> armazenar_analises_visualizacoes_task)

gerar_relatorios_automatizados_task

gerar_relatorios_automatizados_task >> monitorar_execucao_task

monitorar_execucao_task >> analisar_logs_task

