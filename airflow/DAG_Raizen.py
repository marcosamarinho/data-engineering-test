from   datetime import timedelta, datetime
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
	'owner': 'Marcos',
	'start_date': datetime(2022, 3, 4),
	'retries': 3,
	'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
dag = DAG('DAG_Raizen',
	default_args=default_args,
	description= 'DAG to open a spreadsheet and save sheets as partioned parquet as output',
	schedule_interval='*/30 * * * *', 
    max_active_runs=1,
	catchup=False,
	tags=['raizen , marcos']
)

def read_xls_and_save_parquet(path,xlsfile,sheet_name):
    """
    main procedure that reads excell 
    this uses pandas dataframe as size of 
    save as partitioned by date parquet 
    """
    file='/'.join((path,xlsfile))
    
    df=pd.read_excel(file,
        engine= "xlrd",
        sheet_name=sheet_name,
        # force read ANO as string to avoid error 
        dtype={'ANO': np.str_},
        # Not read "REGIÃO",and wrong "TOTAL" [2 ,16]
        usecols=[0,1,3,4,5,6,7,8,9,10,11,12,13,14,15]
    )
    # df=df.head(2) # Reduce data in test time 
    # print totals to check during tests
    # print(df.groupby('ANO')['Jan','Fev','Mar','Abr','Mai','Jun'
    #    ,'Jul','Ago','Set','Out','Nov','Dez'].sum())
    #Sum All Columns Jan,Fev...,Dez
    total=0
    for i in df.iloc[:,3:15].sum(axis=1):
        total+=i
 
    # Transform cols to rows using melt 
    df2=df.melt(id_vars=[ "COMBUSTÍVEL","ESTADO","ANO"], 
        var_name="MES", value_name="volume") 
    
     # Take unit (m3)and take unit COMBUSTÍVEL out using regex
    df2['unit'] = df2['COMBUSTÍVEL'].str.extract('.*\((.*)\).*', expand=True)
    df2['COMBUSTÍVEL'] = df2['COMBUSTÍVEL'].str.extract('(.*)\(.*\).*', expand=True)        
    # Replace month to allow create DATE latter
    df2['MES'] = df2['MES'].replace(
        ['Jan','Fev','Mar','Abr','Mai','Jun'
        ,'Jul','Ago','Set','Out','Nov','Dez']
        # to 
        ,['01', '02','03', '04','05', '06',
        '07', '08','09', '10','11', '12'])
    # Create a new Column year_month merging ANO and MES using Ansii date 
    df2 = df2.assign(year_month=lambda x: (x['ANO'] + '-' + x['MES'] + '-01' ))
    df2["year_month"]=pd.to_datetime(df2['year_month']).dt.date
    
    # Drop unused columns 
    df2=df2.drop("ANO",axis=1).drop("MES",axis=1)

    # New column Created_at   
    df2['created_at'] = pd.to_datetime('today')
    # rename columns to save file(s) with names defined 
    df2=df2.rename(columns={
        "ESTADO": "uf",
        "COMBUSTÍVEL": "product"})
    # just check totals before create files 
    if round(df2["volume"].sum(),3) == round(total,3):
        print("total {} OK=".format(sheet_name),total) 
        # Convert DataFrame to Apache Arrow Table
        table = pa.Table.from_pandas(df2)
        # pq.write_table(table, '/'.join((path,parquet_file)))
        root_path='/'.join((path,sheet_name))
        # rm dir to not duplicate data 
        shutil.rmtree(root_path, ignore_errors=True)
        pq.write_to_dataset(
            table,
            root_path = root_path,
            partition_cols = ['year_month'],
            # not works at arrow libs at docker airflow 4.2  
            # existing_data_behavior="delete_matching"
        )
        # create csv to just to check output during tests
        # df2.to_csv('/'.join((path,sheet+'.csv')),index=False,)              
    else:
        raise ValueError("ERROR total not matched , not creating output Files")
    
def sheet_proc(**kwargs):
    """
    AIRFLOW Callable Function receive kwargs { 'sheet': sheet_name }
    calls read_xls_and_save_parquet
    NOTE:/opt/airflow/dags/data is internal docker path 
    used on test as "shared disk" 
    """
    read_xls_and_save_parquet(
		path='/opt/airflow/dags/data',
		xlsfile='vendas-combustiveis-m3.xls',
		sheet_name=kwargs['sheet_name']
	)

# Creating dummy  tasks
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)
# Creating list of sheets 
sheets=["DPCache_m3","DPCache_m3_2","DPCache_m3_3"]
# Create list to execute tasks in paralell fashion  
# assuming small spreadsheet file size  
listTasks=[]
for sheet_name in sheets:
    listTasks.append( PythonOperator(
        task_id=sheet_name,
        python_callable=sheet_proc,
        op_kwargs={'sheet_name': sheet_name},
        dag=dag
    ))

# Task order execution. 
start_task >> listTasks >> end_task
