import pandas as pd

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

    #Soma os valores de todas as vendas de biodesel 
    soma_vendas_biodisel = df['Vendas de Biodiesel'].sum()
    soma_vendas_biodisel

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
    df_tratado = df.groupby(by=['ano', 'regiao']).sum().sort_values(by='ano')
    df_tratado

    #Soma os valores de todas as vendas de biodesel do dataframe tratado
    soma_vendas_biodisel_tratado = df_tratado['vendas_biodiesel'].sum()
    soma_vendas_biodisel_tratado

    #Exporta o dataframe tratado para um CSV
    df_tratado.to_csv('teste.csv')