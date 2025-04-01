from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from datetime import datetime, timedelta, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
import psycopg2
import pandas as pd
import requests
import json
from clickhouse_driver import Client

time_lag = timedelta(hours=7)
current_date = (datetime.now() - time_lag).date()

HDFS_PATH = 'hdfs://namenode:8020/vacancy'

default_args = {
    'start_date' : datetime(current_date.year, current_date.month, current_date.day)
    'owner' : 'airflow',
}

dag = DAG(
    'etl_hadoop',
    default_args=default_args,
    description='etl_hadoop',
    schedule_interval='0 23 * * *'
)

def save_file_to_hadoop():

    spark = SparkSession.builder \
                        .appName('CreateDataFrame') \
                        .getOrCreate()

    schema = StructType([
        StructField('page', IntegerType(), True),
        StructField('id', IntegerType(), True),
        StructField('vacancy', StringType(), True),
        StructField('experience', StringType(), True),
        StructField('company', StringType(), True),
        StructField('specialize', IntegerType(), True)
    ])
    
    main_df = spark.createDataFrame(data=[], schema=schema)
 
    for page in range(0, 20):
        params = {
            'text': 'NAME:Аналитик',
            'area': 1,  # Поиск по Москве
            'page': page,
            'per_page': 100  # Кол-во вакансий на 1 стр.
        }

        req = requests.get('https://api.hh.ru/vacancies', params)
        result = req.json()
        req.close()

        # Разворачиваем json
        ids = list(int(x['id']) for x in result['items'])
        pages = [int(result['page'])] * len(ids)
        vacancies = list(x['name'] for x in result['items'])
        experience = list(x['experience']['name'] for x in result['items'])
        company = list(x['employer']['name'] for x in result['items'])
        specialize = list(int(_[0]['id']) for _ in [x['professional_roles'] for x in result['items']])
        
        data = list(zip(pages, ids, vacancies, experience, company, specialize))
        
        df = spark.createDataFrame(data=data, schema=schema)
    
        main_df = main_df.union(df)
 
    try:
        main_df.write.parquet(f'{HDFS_PATH}{current_date}.parquet')
        print(f'Файл {HDFS_PATH}{current_date}.parquet ЗАПИСАН!')
    except:
        print(f'Файл {HDFS_PATH}{current_date}.parquet УЖЕ СУЩЕСТВУЕТ В HDFS')
            
    spark.stop()
        
    pass



def transform_load_CH_company():
        
    HDFS_PATH = 'hdfs://namenode:8020/vacancy'  
        
    spark = SparkSession.builder.getOrCreate()
    df_sp = spark.read.parquet(f'{HDFS_PATH}{current_date}.parquet', header=True)
        
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \(").getItem(0))
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \/").getItem(0))
    
    df_sp_company = df_sp\
        .groupBy('company')\
        .agg(F.count('company')\
        .alias('count_company'))
        
    df_sp_company = df_sp_company\
        .filter(df_sp_company.count_company >= 3)\
        .orderBy(['count_company'], ascending=[False])
    df_sp_company = df_sp_company.withColumn('date', F.lit(current_date))
    
    client = Client(host='host.docker.internal', port=9000, database='default')  
        
    create_table_query = """
        CREATE TABLE IF NOT EXISTS df_sp_company(
            company Varchar,
            count_company UInt16,
            date Date
            )
        ENGINE = MergeTree()
        PRIMARY KEY (company)
        ORDER BY (company)  
        SETTINGS index_granularity = 8192;
        """
        
    client.execute(create_table_query)#,  with_column_types=False)
        
    data = df_sp_company.rdd.map(lambda row: tuple(row)).collect()
    client.execute('INSERT INTO df_sp_company (company, count_company, date) VALUES', data)

    df_sp_vacancy = df_sp\  
        .groupBy('vacancy')\
        .agg(F.count('vacancy')\
        .alias('vacancy_count'))\
        .orderBy(['vacancy_count'], ascending=[False])
            
    df_sp_vacancy = df_sp_vacancy.filter(df_sp_vacancy.vacancy_count>=3)
    df_sp_vacancy = df_sp_vacancy.withColumn('date', F.lit(current_date))
        
    create_table_query = """ 
        CREATE TABLE IF NOT EXISTS df_sp_vacancy(
            vacancy Varchar,
            count_vacancy UInt16,
            date Date
            )
        ENGINE = MergeTree()
        PRIMARY KEY (vacancy)
        ORDER BY (vacancy)
        SETTINGS index_granularity = 8192;
        """
        
    client.execute(create_table_query)
    
    data = df_sp_vacancy.rdd.map(lambda row: tuple(row)).collect()
    client.execute('INSERT INTO df_sp_vacancy (vacancy, count_vacancy, date) VALUES', data)
            
    
    df_sp_experience = df_sp\
        .groupBy('experience')\
        .agg(F.count('experience')\
        .alias('count_experience'))\
        .orderBy(['count_experience'], ascending=[False])
            
    df_sp_experience = df_sp_experience.withColumn('date', F.lit(current_date))
             
    create_table_query = """
        CREATE TABLE IF NOT EXISTS df_sp_experience(
            experience Varchar,
            count_experience UInt16,
            date Date
            )
        ENGINE = MergeTree()
        PRIMARY KEY (experience)
        ORDER BY (experience) 
        SETTINGS index_granularity = 8192;
        """
    
    client.execute(create_table_query)

    data = df_sp_experience.rdd.map(lambda row: tuple(row)).collect()
    
    client.execute('INSERT INTO df_sp_experience (experience, count_experience, date) VALUES', data)
        
    client.disconnect()
    spark.stop()
    
    pass


task_save_hadoop = PythonOperator(task_id='save_file_to_hadoop',
                                  python_callable=save_file_to_hadoop,
                                  dag=dag,
                                  provide_context=True)
        
task_transform_save_CH = PythonOperator(task_id='transform_save_to_CH',
                                        python_callable=transform_load_CH_company,
                                        dag=dag,
                                        provide_context=True)
    
task_save_hadoop >> task_transform_save_CH
