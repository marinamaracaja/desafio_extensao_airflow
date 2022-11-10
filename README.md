# Desafio Extensão Airflow
O desafio consiste em desenvolver uma pipeline de orquestração de dados e um ETL que extraia dados de uma fonte da internet, transforme os dados fazendo uma limpeza e estruturação dos campos, exporte os arquivos em um formato de big data e valide a integridade dos dados brutos com os dados tratados.

A base de dados escolhida é extraída do site da ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis) e a tabela escolhida é:
- Vendas de biodiesel;

"Do resumo do conjunto de dados:
São apresentados os dados referentes à quantidade de biodiesel vendido entre a UF de origem e a UF de destino informados pelos produtores de biodiesel."

Os dados brutos se apresentam da seguinte forma:

| Coluna               | Tipo        |
| -------------------- | ----------- |
| `Mês/Ano`            | `date`      |
| `Região Origem`      | `string`    |
| `Região Destino`     | `string`    |
| `Vendas de Biodiesel`| `int`       |


A base está disponível no site da ANP: https://dados.gov.br/dataset/painel-de-produtores-de-derivados-vendas-de-derivados-de-petroleo-e-biocombustiveis/resource/f06c410e-9e71-4a5e-8551-611517de96e9?inner_span=True


## Ferramentas utilizadas:
- VS Code: IDE utilizada para utilizado para executar de forma rápida os códigos e funções em python. Toda limpeza e tratamento inicial dos dados foi realizado nessa ferramenta que permitiu uma rápida visualização dos dataframes criados. Além disto foi utilizada para integrar o código criado em python com a ferramenta airflow através do uso de DAGs (Directed Acyclic Graph).
- Apache Airflow: Orquestrador de pipelines que possibilitou o gerenciamento e a automatização do fluxo de tarefas criado.
- Docker: Ferramenta gerenciadora de containers que permitiu a criação de um ambiente escalável onde o airflow foi executado.
- PgAdmin: Administrador Open Source e plataforma de desenvolvimento para o PostgreSQL.

## Desenvolvimento:
O primeiro passo para execução da atividade foi pensar em um possível tratamento que resultasse em um insight interessante obtido a partir da base de dados escolhida. Dessa forma, a ideia que buscou ser alcançada com o processo de ETL foi a de realizar uma limpeza dos dados não relevantes (excluindo colunas e outras informações desnecessárias para o objetivo) e também simplificar a base de dados escolhida, trazendo apenas as informações relevantes para o insight, facilitando a visualização dos dados.

Assim, a estrutura final do dataframe após o processo de tratamento dos dados foi:

| Coluna             | Tipo        |
| ------------------ | ----------- |
| `ano`              | `date`      |
| `regiao`           | `string`    |
| `vendas_biodiesel` | `int`       |

Processo de tratamento dos dados utilizando o VSCode:
![image](https://user-images.githubusercontent.com/86935693/201013629-117f8e34-b540-4424-802c-b3ae2b438fce.png)

O resultado foi um dataframe que agrupou as informações referentes ao ano da venda, região responsável pela compra do biodiesel e suas respectivas ligações com soma da quantidade de vendas do biodiesel.


```
df_tratado = df.groupby(by=['ano', 'regiao']).sum().sort_values(by='ano')
df_tratado
```
![image](https://user-images.githubusercontent.com/86935693/201012938-44217863-9e5f-44b1-aa8a-ddd230cede21.png)



## Pipeline:
Uma vez que foram definidos quais tratamentos serão aplicados no dataframe, utilizando o Vs Code foi criado um arquivo *.py* contendo as funções de ETL e a DAG que irá definir as tasks e o fluxo de atividades no Airflow:
```
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
```

Desenho do fluxo da DAG no airflow:

![image](https://user-images.githubusercontent.com/86935693/201014300-b4976251-b33c-4df4-972a-cb55333d2f0e.png)


## Fluxo de trabalho:
O fluxo de trabalho desenvolvido extrai os dados brutos do site da ANP, carrega localmente uma cópia do dataframe, faz as transformações solicitadas nos dados, converte o arquivo final *.csv* para o formato big data escolhido: *.parquet*, em seguida, ocorre uma validação da integridade dos dados brutos em comparação com os dados finais tratados, para tanto é verificado se a soma da quantidade de vendas do dataframe tratado inicial está igual a soma do dataframe tratado final.
Por fim é realizada uma inserção do dataframe final em uma tabela SQL no Postgres.

## Execução do projeto:
Para executar o projeto via Docker, deve-se:

- Clonar o atual repositório:
git clone https://github.com/marinamaracaja/desafio_extensao_airflow.git

- Abrir o Docker desktop

- Upar o arquivo do airflow bem como todas as pastas do projeto e na pasta raiz "airflow-desafio-final" rodar via terminal os seguintes comandos:
```
docker-compose up airflow-init
```
```
docker-compose up -d --no-deps --build postgres
```
```
docker-compose up
```
- Acessar no navegador web:
http://localhost:8080/

- Criar na aba "Admin" -> "Connections" um conector Postgree com as seguintes informações:
  - Connection Id: postgres-airflow
  - Connection Type: Postgres
  - Host: host.docker.internal
  - Schema: desafio-airflow-db
  - Login: airflow
  - Port: 5432

- Abrir um administrador de banco de dados Postgres (preferencialmente o PgAdmin4)
  - Criar um servidor "desafio-airflow"
  - Criar uma conexão com login e senha "airflow"

- Retornar ao localhost e buscar a DAG: dag_desafio_final

- Executar o fluxo de tarefas.

## Possíveis melhorias:
- Realizar o processo em nuvem;
- Otimizar as conexões entre funções.

## Conclusão:
Ao final das aulas de extensão de Airflow aprendi conceitos, executei práticas e construí uma comunidade de estudos e dúvidas com as colegas e professor que fizeram parte desta incrível formação. Agradeço em especial ao Professor Adriano pelos ensinamentos e à SoulCode e à Raízen pela oportunidade.
##

### Autora:
[Marina Maracajá](https://www.linkedin.com/in/marinamaracaja/)

##
<img align="right" alt="grogu" height="50" style="border-radius:50px;" src="https://soulcodeacademy.org/martech-academy/assets/images/Picture1.png">


