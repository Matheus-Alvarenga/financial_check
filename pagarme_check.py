import json
import os
import pandas as pd

from dotenv import load_dotenv
from mysql.connector import connect, Error
from sqlalchemy import create_engine, text

load_dotenv()

load_from_DB = True


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


def db_bulk_load():
    payables = db_load("SELECT * FROM pagarme_payables")
    transactions = db_load("SELECT * FROM pagarme_transactions")
    if os.environ.get("DATABASE_NAME") == 'dnc_sales':
        sales = db_load("SELECT * from pagarme_sales_corrigido WHERE gateway_name = 'Pagarme'")
    else:
        sales = db_load("SELECT * from sales WHERE gateway_name = 'pagarme'")
    return payables, transactions, sales


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
    # Iterate over files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            # Read Excel file into DataFrame
            df = pd.read_csv(file_path)
            # Append DataFrame to list
            dfs.append(df)
    # Concatenate DataFrames vertically
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def payables_adjust(df):
    # Adjust Date Format
    string_format = "%Y-%m-%d"
    df['data_de_competencia'] = df['data_de_competencia'].apply(lambda x: x.strftime(string_format))
    # Select specific interest rows from table
    df = df[['data_de_competencia', 'transaction_id', 'installment', 'amount', 'type']]
    # Rename rows to match sales
    df.rename(columns={'data_de_competencia': 'data_venda', 'amount': 'valor', 'type': 'status'}, inplace=True)
    df.sort_values(by=['data_venda', 'transaction_id', 'installment'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['data_venda'] = df['data_venda'].astype('string')
    df['transaction_id'] = df['transaction_id'].astype('string')
    return df


def sales_adjust(df):
    # Create columns with corresponding installment and transaction id
    df['installment'] = df['gateway_id'].str.split('-').str[0]
    df['transaction_id'] = df['gateway_id'].str.split('-').str[1]
    # Select specific columns to work with
    df = df[['data_venda', 'transaction_id', 'installment', 'cpf_responsavel_compra', 'valor_parcela_total', 'status',
             'valor_cancelamento']]
    # Rename Columns to match other DB
    df.rename(columns={'valor_parcela_total': 'valor', 'valor_cancelamento': 'refund'}, inplace=True)
    df['refund'].fillna(0, inplace=True)
    # Dataframe specific column types
    df['installment'] = df['installment'].astype('int64')
    df['data_venda'] = df['data_venda'].astype('string')
    df['transaction_id'] = df['transaction_id'].astype('string')
    # Dataframe sort by column values
    df.sort_values(by=['data_venda', 'transaction_id', 'installment'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def check_single_occurancy(df_p, df_t, df_s):
    # Get sub dataframe from payables where there is only one status per transaction id
    df_p_unique_status = df_p.groupby('transaction_id').filter(lambda x: x['status'].nunique() == 1)
    # Invalid payables: status != 'credit' with unique id  =>  These values are not present on sales DB
    invalid_unique_st_status = df_p_unique_status[df_p_unique_status['status'] != 'credit']['transaction_id'].unique().tolist()

    # Check within unique ids (status == credit) the number of installments
    df_t['installments'].fillna(1, inplace=True)
    # Filter df_p_unique_status for status == 'credit'
    df_p_credit = df_p_unique_status[df_p_unique_status['status'] == 'credit']
    # Group df_p_credit by 'transaction_id' and count the number of rows for each group
    installments_count = df_p_credit.groupby('transaction_id').size().rename('installments')
    # Merge installments_count with df_t on 'transaction_id'
    merged_df = pd.merge(df_t[df_t['transaction_id'].isin(df_p_credit['transaction_id'])], installments_count,
                         left_on='transaction_id', right_index=True, how='left')
    # Filter rows where installments from df_t don't match installments from groupby
    invalid_unique_st_installments = merged_df[merged_df['installments_x'] != merged_df['installments_y']]['transaction_id'].unique().tolist()

    # Make comparison with sales DB
    df_p_vs_s = df_p_credit.merge(df_s, on=['transaction_id', 'installment'], how='left', suffixes=('_p', '_s'))
    df_p_vs_s['refund'].fillna(0, inplace=True)
    # Check if there is any refund value within sales => not expected
    invalid_unique_st_s_refund = df_p_vs_s[df_p_vs_s['refund'] != 0]['transaction_id'].unique().tolist()
    # Check missing values within sales that are present within payables
    invalid_unique_st_missing_s = df_p_vs_s[df_p_vs_s['valor_s'].isna()]['transaction_id'].unique().tolist()
    # Check discrepancies between sales and payables values
    df_p_vs_s.dropna(subset=['valor_s'], inplace=True)
    df_p_vs_s['delta_v'] = df_p_vs_s['valor_s'] - df_p_vs_s['valor_p']
    invalid_unique_st_valuesDiff = df_p_vs_s[df_p_vs_s['delta_v'].abs() > .01]['transaction_id'].unique().tolist()

    return invalid_unique_st_status, invalid_unique_st_installments, invalid_unique_st_s_refund, \
           invalid_unique_st_missing_s, invalid_unique_st_valuesDiff


def check_refund(df_p, df_s):
    # Get subframe from payables containing only status credit and refund
    df_p_refund_only = df_p.groupby('transaction_id').filter(lambda x: set(x['status']) == {'credit', 'refund'})
    p_refund_only_ids = df_p_refund_only['transaction_id'].unique().tolist()  # Get their transaction ids
    # Get subframe from sales with such transaction ids
    df_s_refund_only = df_s[df_s['transaction_id'].isin(p_refund_only_ids)]
    s_refund_only_ids = df_s_refund_only['transaction_id'].unique().tolist()  # Get their transaction ids
    # Check refunds id not present on sales and payables
    invalid_refund_only_no_sales = list(set(p_refund_only_ids) - set(s_refund_only_ids))
    # Check sum discrepancy
    df_p_refund_only_valid = df_p_refund_only[~df_p_refund_only['transaction_id'].isin(invalid_refund_only_no_sales)]
    df_s_refund_only['refund'] = df_s_refund_only['refund'].astype(float)
    s_refund_only_sum = df_s_refund_only.groupby('transaction_id')['valor'].sum() + \
                        df_s_refund_only.groupby('transaction_id')['refund'].sum()
    p_refund_only_valid_sum = df_p_refund_only_valid.groupby('transaction_id')['valor'].sum()
    refund_p_vs_s_delta_sum = s_refund_only_sum - p_refund_only_valid_sum
    invalid_refund_only_sum_error = refund_p_vs_s_delta_sum.index[refund_p_vs_s_delta_sum.abs() > .1].tolist()
    return invalid_refund_only_no_sales, invalid_refund_only_sum_error


def check_chargeback(df_p, df_s):
    # chargeback list ids
    list_chargeback = df_p[df_p['status'] == 'chargeback']['transaction_id'].unique()
    # chargeback_refund list ids
    list_chargeback_refund = df_p[df_p['status'] == 'chargeback_refund']['transaction_id'].unique()
    # First problem: id within chargeback refund but not within chargeback
    invalid_chargeback_refund_no_chargeback = [v for v in list_chargeback_refund if v not in list_chargeback]
    # chargeback ids that have counterpart within credit
    list_chargeback_credit = [v for v in df_p[df_p['status'] == 'credit']['transaction_id'].unique()
                              if v in list_chargeback and v not in list_chargeback_refund]
    # Second problem: id within chargeback but no counterpart within chargeback_refund nor credit
    invalid_chargeback_no_counterpart = [v for v in list_chargeback if v not in list_chargeback_refund.tolist() +
                                         list_chargeback_credit]
    # Check count identity :: chargeback - chargeback_refund - chargeback_credit (taking problems apart)
    invalid_chargeback_amount_check = (len(list_chargeback) - len(invalid_chargeback_no_counterpart)) - \
                                      (len(list_chargeback_refund) - len(invalid_chargeback_refund_no_chargeback)) - \
                                      len(list_chargeback_credit)
    # 3rd Problem: sum from sales comparing to payables not matching
    list_chargeback_ok = list(set(list_chargeback) - set(invalid_chargeback_refund_no_chargeback) -
                              set(invalid_chargeback_no_counterpart))
    df_s['valor'] = df_s['valor'].astype(float)
    df_s['refund'] = df_s['refund'].astype(float)
    df_s_group = df_s[df_s['transaction_id'].isin(list_chargeback_ok)].groupby('transaction_id')
    df_p_group = df_p[df_p['transaction_id'].isin(list_chargeback_ok)].groupby('transaction_id')
    invalid_chargeback_sum_error = df_s_group.sum().index[(df_s_group['valor'].sum() + df_s_group['refund'].sum() -
                                                           df_p_group['valor'].sum()).abs() > .1].tolist()
    return invalid_chargeback_refund_no_chargeback, invalid_chargeback_no_counterpart, invalid_chargeback_amount_check, \
           invalid_chargeback_sum_error


def check_payables_refund_reversal(df_p, df_s):
    list_refund_reversal = df_p[df_p['status'] == 'refund_reversal']['transaction_id'].unique()
    invalid_refund_reversal_sum = []
    for refund_id in list_refund_reversal:
        p_sum = df_p[df_p['transaction_id'] == refund_id]['valor'].sum()
        s_sum = df_s[df_s['transaction_id'] == refund_id]['valor'].sum() + df_s[df_s['transaction_id'] == refund_id]['refund'].sum()
        if p_sum != s_sum:
            invalid_refund_reversal_sum.append(refund_id)
    return invalid_refund_reversal_sum


def check_sum_by_month(df):
    df['data_caixa'] = pd.to_datetime(df['recebimento_financiamento'])
    df['Parcela'] = df['gateway_id'].str.split("-").str[0]
    df['Id da transação'] = df['gateway_id'].str.split("-").str[1]
    df['Parcela'].replace(r'\b0\b', '1', regex=True, inplace=True)
    df = df[df['data_caixa'] > pd.to_datetime('2023-01-01')]
    df['Parcela'] = df['Parcela'].astype(str)
    df['Id da transação'] = df['Id da transação'].astype(str)

    extrato_diario = True
    if extrato_diario:
        df_extrato = local_df_load_extrato_diario()
        df_extrato['data_caixa'] = pd.to_datetime(df_extrato['Data de pagamento'], format='%d/%m/%Y %H:%M')
        df_extrato['Id da transação'] = df_extrato['ID da Transação'].astype(str)
        df_extrato['Parcela'].replace({'-': '1'}, inplace=True)
        df_extrato['Parcela'] = df_extrato['Parcela'].astype(str)
    else:
        df_extrato = pd.read_excel('pagarme_extrato.xlsx', engine='openpyxl')
        df_extrato['data_caixa'] = pd.to_datetime(df_extrato['Data da operação'], format='%d/%m/%Y %H:%M')
        df_extrato['Id da transação'] = df_extrato['Id da transação'].astype(str)
        df_extrato['Parcela'].replace({'-': '1'}, inplace=True)
        df_extrato['Parcela'] = df_extrato['Parcela'].astype(str)
        df_extrato['Tipo da operação'] = df_extrato['Tipo da operação'].astype(str)

    df_transactions = pd.read_feather("faturamento_pagarme_transactions.feather")
    df_transactions = df_transactions[['transaction_id', 'nsu']]
    df_transactions['transaction_id'] = df_transactions['transaction_id'].astype(str)
    df_transactions['nsu'] = df_transactions['nsu'].astype(str)
    df_transactions['nsu'] = df_transactions['nsu'].str.split(".").str[0]

    df_extrato = df_extrato.merge(df_transactions, left_on=['Id da transação'], right_on=['nsu'], how='left')

    df_compare = df.merge(df_extrato, left_on=['Id da transação', 'Parcela'], right_on=['transaction_id', 'Parcela'], how='left')

    df_group_sum = df.groupby(pd.Grouper(key='data_caixa', freq='ME')).agg(
        {'valor_total_venda': 'sum', 'valor_taxa_total': 'sum', 'valor_cancelamento': 'sum', 'reembolso_taxa': 'sum',
         'juros_atraso': 'sum'})
    df_group_sum_interest = df_group_sum[df_group_sum.index > pd.to_datetime('2023-01-01')]
    df_group_sum_interest['total'] = df_group_sum_interest.sum(axis=1)


def check_according_spreadsheet(df):
    pass


if __name__ == '__main__':

    if load_from_DB:
        df_pagarme_payables, df_pagarme_transactions, df_pagarme_sales = db_bulk_load()
        # Eliminate problem columns
        df_pagarme_transactions.pop('pagarme_transactions_created_at')
        df_pagarme_transactions.pop('pagarme_transactions_updated_at')
        # Save new data locally
        local_df_save({'pagarme_payables': df_pagarme_payables, 'pagarme_transactions': df_pagarme_transactions,
                       'pagarme_sales': df_pagarme_sales})
    else:
        # Dataframe local data load
        [df_pagarme_payables, df_pagarme_transactions, df_pagarme_sales] = \
            local_df_load(["pagarme_payables", 'pagarme_transactions', 'pagarme_sales'])

    # Check with extrato
    check_sum_by_month(df_pagarme_sales)

    # Adjust Payables DB
    df_pagarme_payables = payables_adjust(df_pagarme_payables)
    # Adjust Sales DB
    df_pagarme_sales = sales_adjust(df_pagarme_sales)

    # Get Status Types and Ocurrancies within transactions ad payables DB
    status_transactions = df_pagarme_transactions['status'].value_counts()
    status_payables = df_pagarme_payables['status'].value_counts()

    # Check single occurrences within payables
    invalid_unique_st_status, invalid_unique_st_installments, invalid_unique_st_s_refund,  \
    invalid_unique_st_missing_s, invalid_unique_st_valuesDiff = \
        check_single_occurancy(df_pagarme_payables, df_pagarme_transactions, df_pagarme_sales)

    # Check pair credit | refund discrepancies
    invalid_refund_only_no_sales, invalid_refund_only_sum_error = check_refund(df_pagarme_payables, df_pagarme_sales)

    # Check chargeback payables discrepancies
    invalid_chargeback_refund_no_chargeback, invalid_chargeback_no_counterpart, invalid_chargeback_amount_check, \
    invalid_chargeback_sum_error = check_chargeback(df_pagarme_payables, df_pagarme_sales)

    # Check refund reversal discrepancies between sales and payables
    invalid_refund_reversal_sum = check_payables_refund_reversal(df_pagarme_payables, df_pagarme_sales)

    # Get complete transactions
    invalid_unique_st_missing_s_transactions = df_pagarme_transactions[df_pagarme_transactions['transaction_id'].
    isin(invalid_unique_st_missing_s)]

    pass
