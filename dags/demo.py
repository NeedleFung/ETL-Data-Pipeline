from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now().date().strftime('%Y-%m-%d'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Demo', 
    default_args=default_args,
    description='ETL pipeline for Demo',
    schedule=timedelta(days=1),
)

def extract_data(**kwargs): 
    db = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=172.23.32.1; DATABASE=Bank_Transactions; UID=sa; PWD=Pdc3001@;')

    df1 = pd.read_sql('SELECT Customer_ID, Income_Level FROM Customers', db)
    df2 = pd.read_sql('SELECT Customer_ID, Transaction_Date, Transaction_Time, TransactionAmount_INR FROM Transactions', db)   
     
    ti = kwargs['ti']
    ti.xcom_push(key='df1', value=df1)
    ti.xcom_push(key='df2', value=df2)

    db.close()

def transform_data(**kwargs):
    ti = kwargs['ti']
    df1 = ti.xcom_pull(task_ids='extract_data', key='df1')
    df2 = ti.xcom_pull(task_ids='extract_data', key='df2')

    df1['Customer_ID'] = df1['Customer_ID'].str.strip()
    
    df2['Customer_ID'] = df2['Customer_ID'].str.strip()
    df2 = df2.groupby(['Customer_ID', 'Transaction_Date']).agg(
        Transaction_Times_In_Day=('Transaction_Time', 'count'),
        Transaction_Amount=('TransactionAmount_INR', 'sum'),
        Total_Transaction_Times=('Transaction_Time', 'max')    
    ).reset_index()
    df2 = df2.sort_values(by=['Customer_ID', 'Transaction_Date'])

    merged_df = pd.merge(df1, df2, on='Customer_ID', how='left')
    merged_df.loc[(merged_df['Transaction_Date'] == '2016-09-14') & (merged_df['Total_Transaction_Times'] > 100000), 'Result'] = 1
    merged_df['Result'] = merged_df['Result'].fillna(0).astype(int)
    merged_df = merged_df[
                            ['Customer_ID',
                            'Transaction_Date',
                            'Transaction_Times_In_Day',
                            'Total_Transaction_Times',
                            'Transaction_Amount',
                            'Income_Level',
                            'Result']
                        ].dropna()

    ti.xcom_push(key='merged_df', value=merged_df)

def load_data(**kwargs):
    ti = kwargs['ti']
    merged_df = ti.xcom_pull(task_ids='transform_data', key='merged_df')
    db = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=172.23.32.1; DATABASE=Bank_Transactions; UID=sa; PWD=Pdc3001@;')
    cursor = db.cursor()
    cursor.execute('TRUNCATE TABLE Result')
    for row in merged_df.iterrows():
        cursor.execute(
                    """
                        INSERT INTO Result (
                            Customer_ID,
                            Transaction_Date,
                            Transaction_Times_In_Day,
                            Total_Transaction_Times,
                            Transaction_Amount,
                            Income_Level,
                            Result
                        ) VALUES (?,?,?,?,?,?,?)
                    """, tuple(row[1]))
    db.commit()
    db.close()

extract = PythonOperator(
    task_id='extract_data',     
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract >> transform >> load
