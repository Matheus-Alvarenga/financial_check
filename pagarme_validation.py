import json
import os
import pandas as pd

from dotenv import load_dotenv
from mysql.connector import connect, Error
from sqlalchemy import create_engine, text

load_dotenv()


def db_load(query):
    try:
        # create sqlalchemy engine
        engine = create_engine("mysql+pymysql://{user}:{pw}@{ep}:{port}/{db}"
                               .format(user=os.environ.get("DATABASE_USER"),
                                       pw=os.environ.get("DATABASE_PASS"),
                                       ep=os.environ.get("DATABASE_HOST"),
                                       port=os.environ.get("DATABASE_PORT"),
                                       db=os.environ.get("DATABASE_NAME")))
        df_retrieved = pd.read_sql(query, engine)
        return df_retrieved
    except Error as err:
        return -1, err


def db_sales_load():
    if os.environ.get("DATABASE_NAME") == 'dnc_sales':
        sales = db_load("SELECT * from pagarme_sales_corrigido WHERE gateway_name = 'Pagarme'")
    else:
        sales = db_load("SELECT * from sales WHERE gateway_name = 'pagarme'")
    return sales


def local_df_save(dict2save):
    for file_name, df_name in dict2save.items():
        df_name.to_feather(os.environ.get("DATABASE_NAME") + "_" + file_name + ".feather")


def local_df_load(files2load):
    df_loaded = []
    for file_name in files2load:
        df = pd.read_feather(os.environ.get("DATABASE_NAME") + "_" + file_name + ".feather")
        df_loaded.append(df)
    return df_loaded


def local_df_load_extrato_diario():
    import os
    import pandas as pd

    # Directory containing the Excel files
    folder_path = './extrato_diario'
    # List to store DataFrames
    dfs = []
    # Iterate through each subfolder
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                df = pd.read_csv(file_path)
                dfs.append(df)
    # Concatenate DataFrames vertically
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def adjust_sales(df_sales):
    # select only data between desired time
    df_sales['data_caixa'] = pd.to_datetime(df_sales['recebimento_financiamento']).dt.normalize()
    df_sales = df_sales[(df_sales['data_caixa'] >= pd.to_datetime('2023-01-01')) &
                        (df_sales['data_caixa'] < pd.to_datetime('2024-01-01'))]
    # create id and installment from gateway_id
    df_sales['parcela'] = df_sales['gateway_id'].str.split("-").str[0]
    df_sales['parcela'].replace(r'\b0\b', '1', regex=True, inplace=True)
    df_sales['transaction_id'] = df_sales['gateway_id'].str.split("-").str[1]
    # define types
    df_sales[['parcela', 'transaction_id']] = df_sales[['parcela', 'transaction_id']].astype(str)
    df_sales.fillna('0', inplace=True)
    df_sales[['valor_total_venda', 'valor_taxa_total', 'valor_cancelamento', 'reembolso_taxa']] = \
        df_sales[['valor_total_venda', 'valor_taxa_total', 'valor_cancelamento', 'reembolso_taxa']].astype(float)
    # restrict columns to analyze
    colums_to_keep = ['gateway_id', 'data_venda', 'valor_total_venda', 'valor_taxa_total', 'valor_cancelamento',
                      'reembolso_taxa', 'transaction_id', 'parcela', 'data_caixa']
    return df_sales[colums_to_keep]


def adjust_extrato():
    df_extrato = local_df_load_extrato_diario()
    df_extrato['data_caixa'] = pd.to_datetime(df_extrato['Data de pagamento'], format='%d/%m/%Y %H:%M').dt.normalize()
    df_extrato.rename(columns={'ID da Transação': 'nsu', 'Parcela': 'parcela'}, inplace=True)
    # adjust types
    df_extrato['parcela'].replace({'-': '1'}, inplace=True)
    df_extrato[['nsu', 'parcela']] = df_extrato[['nsu', 'parcela']].astype(str)
    return df_extrato


def adjust_transactions():
    df_transactions = pd.read_feather("faturamento_pagarme_transactions.feather")
    df_transactions = df_transactions[['transaction_id', 'nsu']]
    df_transactions['transaction_id'] = df_transactions['transaction_id'].astype(str)
    df_transactions['nsu'] = df_transactions['nsu'].astype(str)
    df_transactions['nsu'] = df_transactions['nsu'].str.split(".").str[0]
    return df_transactions


def extrato_check(df_sales):

    df_sales = adjust_sales(df_sales)
    df_extrato = adjust_extrato()
    df_transactions = adjust_transactions()

    df_extrato = df_extrato.merge(df_transactions, on=['nsu'], how='left')

    df_compare = df_sales.merge(df_extrato, on=['transaction_id', 'parcela'], how='left')

    df_group_sum = df_sales.groupby(pd.Grouper(key='data_caixa', freq='ME')).agg(
        {'valor_total_venda': 'sum', 'valor_taxa_total': 'sum', 'valor_cancelamento': 'sum', 'reembolso_taxa': 'sum',
         'juros_atraso': 'sum'})
    df_group_sum_interest = df_group_sum[df_group_sum.index > pd.to_datetime('2023-01-01')]
    df_group_sum_interest['total'] = df_group_sum_interest.sum(axis=1)
    pass


if __name__ == '__main__':

    load_from_DB = True

    if load_from_DB:
        df_sales = db_sales_load()
        # Save new data locally
        local_df_save({'pagarme_sales': df_sales})
    else:
        # Dataframe local data load
        df_sales = local_df_load(['pagarme_sales'])

    # Check with extrato
    extrato_check(df_sales)
