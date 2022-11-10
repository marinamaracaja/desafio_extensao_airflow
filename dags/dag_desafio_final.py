from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
import os
os.system('pip install fastparquet')


#Função para extrair dados:
def extrair_dados():
    url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos-painel-de-produtores-de-derivados-vendas-de-derivados-de-petroleo-e-biocombustiveis/biodiesel_dadosabertos_csv_vendas.csv'
    x = requests.get(url)
    open('bases_brutas/biodiesel_dadosabertos_csv_vendas.csv', 'wb').write(x.content)

#Função para transformar dados:
def transformar_dados():
    df = pd.read_csv('bases_brutas/biodiesel_dadosabertos_csv_vendas.csv', sep=',')

    #Verifica se existem valores nulos na tabela
    df.isna().sum()

    #Verifica se todas as regiões são únicas
    df['Região Destino'].unique()

    #Dropa as linhas que possuem como "região destino" o valor "não informada"
    df.drop(df.loc[df['Região Destino']=='NÃO INFORMADA'].index, inplace=True)

    #Confirma se agora todas as regiões estão únicas
    df['Região Destino'].unique()

    #Salvar tratamento inicial df
    df_tratado_inicial = df
    df_tratado_inicial.to_csv('bases_tratadas/biodiesel_tratamento_inicial.csv')

    #Verifica os tipos das colunas
    df.dtypes

    #Exclui a coluna "Região Origem"
    df.drop(['Região Origem'],axis=1,inplace=True)

    #Substitui os valores de "mês e ano" por apenas "ano"
    df['Mês/Ano'].replace(['01/2017', '02/2017', '03/2017', '04/2017', '05/2017', '06/2017', '07/2017', '08/2017', '09/2017', '10/2017', '11/2017', '12/2017'],
                            ['2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017'], inplace=True)
    df['Mês/Ano'].replace(['01/2018', '02/2018', '03/2018', '04/2018', '05/2018', '06/2018', '07/2018', '08/2018', '09/2018', '10/2018', '11/2018', '12/2018'],
                            ['2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018'], inplace=True)
    df['Mês/Ano'].replace(['01/2019', '02/2019', '03/2019', '04/2019', '05/2019', '06/2019', '07/2019', '08/2019', '09/2019', '10/2019', '11/2019', '12/2019'],
                            ['2019', '2019', '2019', '2019', '2019', '2019', '2019', '2019', '2019', '2019', '2019', '2019'], inplace=True)
    df['Mês/Ano'].replace(['01/2020', '02/2020', '03/2020', '04/2020', '05/2020', '06/2020', '07/2020', '08/2020', '09/2020', '10/2020', '11/2020', '12/2020'],
                            ['2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020'], inplace=True)
    df['Mês/Ano'].replace(['01/2021', '02/2021', '03/2021', '04/2021', '05/2021', '06/2021', '07/2021', '08/2021', '09/2021', '10/2021', '11/2021', '12/2021'],
                            ['2021', '2021', '2021', '2021', '2021', '2021', '2021', '2021', '2021', '2021', '2021', '2021'], inplace=True)
    df['Mês/Ano'].replace(['01/2022', '02/2022', '03/2022'],
                            ['2022', '2022', '2022'], inplace=True)

    #Renomeia as colunas para "ano", "regiao", "vendas_biodiesel"
    df.rename(columns={"Mês/Ano": "ano", "Região Destino": "regiao", "Vendas de Biodiesel": "vendas_biodiesel" }, inplace=True)

    #Agrupa o dataframe por "ano" e "regiao" e soma o valor referente as respectivas "vendas_biodisel"
    df_tratado_final = df.groupby(by=['ano', 'regiao']).sum().sort_values(by='ano')
    df_tratado_final

    #Exporta o dataframe tratado para um CSV
    df_tratado_final.to_csv('bases_tratadas/biodiesel_tratamento_final.csv')

#Função para converter dados:
def converter_dados():
    df_tratado_final = pd.read_csv('bases_tratadas/biodiesel_tratamento_final.csv', sep=',')
    df_tratado_final.to_parquet('bases_tratadas/biodiesel_tratamento_final.parquet')


#Função para validar os dados tratados inicialmente com os dados tratados finais:
def validar_dados():
    df_tratado_inicial = pd.read_csv('bases_tratadas/biodiesel_tratamento_inicial.csv')
    soma_vendas_biodisel_inicial = df_tratado_inicial['Vendas de Biodiesel'].sum()
    

    df_tratado_final = pd.read_csv('bases_tratadas/biodiesel_tratamento_final.csv', sep=',')
    soma_vendas_biodisel_tratado = df_tratado_final['vendas_biodiesel'].sum()


    if soma_vendas_biodisel_inicial == soma_vendas_biodisel_tratado:
        return 'validos'
    else:
        return 'invalidos'

#Função para inserir dados na tabela sql a partir do dataframe tratado
def inserir_dados_sql():
    df = pd.read_parquet('bases_tratadas/biodiesel_tratamento_final.parquet', engine='fastparquet')
    valores = []
    for i in range(len(df)):
        ano = df.iloc[i,0]
        regiao = df.iloc[i,1]
        quantidade_venda = df.iloc[i,2]
        valores.append("('%s', '%s', %s)" % (ano, regiao, quantidade_venda))
       
    values = str(valores).strip('[]')
    values = values.replace('"', '')
    query = "INSERT INTO TB_VENDAS_BIODIESEL(ano, regiao, quantidade_venda) VALUES %s;" % (values)
    return query
        

# Definindo alguns argumentos básicos
default_args = {
    'owner':'Marina Maracaja',
    'start_date': datetime(2022,11,8),
}

#Criando a DAG:
with DAG(
    'dag_desafio_final',
    max_active_runs = 1,
    schedule_interval = '@hourly',
    catchup=False,
    template_searchpath = '/opt/airflow/sql',
    default_args = default_args
) as dag:

  extrair = PythonOperator(
      task_id = 'extrair',
      python_callable = extrair_dados
  )
  transformar = PythonOperator(
      task_id = 'transformar',
      python_callable = transformar_dados
  )
  converter = PythonOperator(
      task_id = 'converter',
      python_callable = converter_dados
  )
  validar = BranchPythonOperator(
      task_id = 'validar_dados',
      python_callable = validar_dados
  )    
  validos = BashOperator(
      task_id = 'validos',
      bash_command ="echo 'Arquivos validos' " 
  )
  invalidos = BashOperator(
      task_id = 'invalidos',
      bash_command ="echo 'Arquivos inválidos' " 
  )
  criar_tabela_db = PostgresOperator(
        task_id='criar_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'criar_tabela_db.sql'
    )
  inserir_dados_tabela_db = PostgresOperator(
        task_id='inserir_dados_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = inserir_dados_sql()
    )
  
  
extrair >> transformar >> converter >> validar >> [validos, invalidos]
validos >> criar_tabela_db >> inserir_dados_tabela_db