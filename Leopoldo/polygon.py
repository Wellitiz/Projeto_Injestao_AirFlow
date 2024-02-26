from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd


api_key = 'xCbjAlfJpDVH7NZwuecazcHShX1iDvKO'

# argumentos padrão para criar dags
default_args = {
    'owner' : 'Leopoldo',  #proprietario responsve pela dag
    'dependent_on_past' : False,   # dependencia de processos anteriores
    'start_date': datetime(2024,1,1), #data de inicio do processo 
    'email_on_failure': False,  # retornar email em caso de falha
    'email_on_retry': False, # retornar email em caso de tentativa
    'retries': 1,  # numero de tentativas
    'retry_delay': timedelta(minutes=5), # intervalo de tempo para nova tentativa       
}

dag = DAG (
    'polygon_api', #nome da dag 
    default_args=default_args,  
    description='Dag para conexão através da API polygon.io',
    schedule_interval=timedelta(days=1) #frequência de conexão
)

def polygon_dag(): 

# coleta dados APPLE-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    #função conexao e coleta Apple
   
    def chamada_apple(**kwargs):
        
        # Coleta dados Apple
        symbol = "AAPL"

        # Defina o intervalo de datas para 01/01/2022 a 31/12/2023
        start_date = "2022-01-01"
        end_date = "2023-12-31"

        # Construa a URL com os parâmetros
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

        # Faça a solicitação GET para obter os dados
        response = requests.get(url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Converta os dados em um DataFrame do Pandas
            dados = response.json()
            df_apple = pd.DataFrame(dados['results'])
            
            # Imprima o DataFrame
            print(df_apple)
        else:
            print(f"Erro na solicitação: {response.status_code}")



    #coleta de dados Microsoft -=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def chamada_Microsoft(**kwargs):
        
        
            # Defina o símbolo da ação (Microsoft - MSFT)
        symbol = "MSFT"

        # Defina o intervalo de datas para 01/01/2022 a 31/12/2023
        start_date = "2022-01-01"
        end_date = "2023-12-31"

        # Construa a URL com os parâmetros
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

        # Faça a solicitação GET para obter os dados
        response = requests.get(url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Converta os dados em um DataFrame do Pandas
            dados = response.json()
            df_microsoft = pd.DataFrame(dados['results'])
            
            # Imprima o DataFrame
            print(df_microsoft)
        else:
            print(f"Erro na solicitação: {response.status_code}")

        
    #COleta de dados AMazon =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def chamada_amazon(**Kwargs):

        # Defina o símbolo da ação (Amazon - AMZN)
        symbol = "AMZN"

        # Defina o intervalo de datas para 01/01/2022 a 31/12/2023
        start_date = "2022-01-01"
        end_date = "2023-12-31"

        # Construa a URL com os parâmetros
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

        # Faça a solicitação GET para obter os dados
        response = requests.get(url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Converta os dados em um DataFrame do Pandas
            dados = response.json()
            df_amazon = pd.DataFrame(dados['results'])
            
            # Imprima o DataFrame
            print(df_amazon)
        else:
            print(f"Erro na solicitação: {response.status_code}")

    # Coleta de dados Alphabet Google -=-=-=--=-=---=-=-==-=-=-=-=-

    def chamada_alphabet(**kwargs):
        
        # Defina a chave de API e o símbolo da ação (Alphabet - GOOGL ou GOOG)
        symbol = "GOOGL"  # ou "GOOG"

        # Defina o intervalo de datas para 01/01/2022 a 31/12/2023
        start_date = "2022-01-01"
        end_date = "2023-12-31"

        # Construa a URL com os parâmetros
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

        # Faça a solicitação GET para obter os dados
        response = requests.get(url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Converta os dados em um DataFrame do Pandas
            dados = response.json()
            df_alphabet = pd.DataFrame(dados['results'])
            
            # Imprima o DataFrame
            print(df_alphabet)
        else:
            print(f"Erro na solicitação: {response.status_code}")

    #Coleta de dados CISCO -=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=

    def chamada_cisco(**kwargs):
        symbol = "CSCO"

        # Defina o intervalo de datas para 01/01/2022 a 31/12/2023
        start_date = "2022-01-01"
        end_date = "2023-12-31"

        # Construa a URL com os parâmetros
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        url = f"{base_url}/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

        # Faça a solicitação GET para obter os dados
        response = requests.get(url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Converta os dados em um DataFrame do Pandas
            dados = response.json()
            df_cisco = pd.DataFrame(dados['results'])
            
            # Imprima o DataFrame
            print(df_cisco)
        else:
            print(f"Erro na solicitação: {response.status_code}")


polygon_api_task = PythonOperator(
    task_id = 'chamada_polygon_api',
    python_callable = polygon_dag, 
    provide_context = True, 
    dag=dag
)

